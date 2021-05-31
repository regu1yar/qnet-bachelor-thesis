from functools import partial
import typing as tp
from google.protobuf.message import DecodeError

from . import network_pb2
from qnet2.utils.timer import RandomShiftedRepeaterSyncCallback, RepeaterSyncCallback
from .metrics import MetricService
from .scatter import Scatterer, MessageHandlerStrategy
from qnet2.config.net_config import Config


class Router:
    __SCATTER_RT_TIMEOUT = 30
    __MAX_TIMER_SHIFT = 5
    __KEEPALIVE_TIMEOUT = __SCATTER_RT_TIMEOUT

    ROUTE_UPDATES_TOPIC = 'router'

    def __init__(self, config: Config, metric_service: MetricService, scatterer: Scatterer):
        self.__id = config.get_local_node_id()
        self.__group = config.get_local_group_id()

        self.__groups = config.get_groups_to_nodes_dict()
        self.__metric_service = metric_service
        self.__scatterer = scatterer

        self.__route_table = network_pb2.RouteTable()
        self.__route_table.routes[self.__group] = network_pb2.Route(next_hop=self.__id, metric=0, length=0)
        for group_id in self.__groups:
            self.__set_naive_route(group_id)

        self.__scatterer.set_router(self)
        self.__scatterer.set_handler_strategy(self.ROUTE_UPDATES_TOPIC, RouterHandler(self))

        self.__scatter_rt_timer = RandomShiftedRepeaterSyncCallback(
            self.__SCATTER_RT_TIMEOUT,
            -self.__MAX_TIMER_SHIFT,
            self.__MAX_TIMER_SHIFT,
            partial(self.__scatter_updates, self.__route_table.routes)
        )
        self.__availability_checker = RepeaterSyncCallback(
            self.__KEEPALIVE_TIMEOUT,
            self.__availability_checker_callback
        )

    def __set_naive_route(self, target_group_id: int) -> bool:
        if target_group_id == self.__group:
            return False
        best_direct_metric: tp.Optional[float] = None
        for node in self.__groups[target_group_id]:
            if self.__metric_service.is_node_available(node):
                direct_metric = self.__metric_service.get_direct_metric(node)
                if best_direct_metric is None or best_direct_metric > direct_metric:
                    best_direct_metric = direct_metric
                    self.__route_table.routes[target_group_id] = network_pb2.Route(
                        next_hop=node, metric=direct_metric, length=1
                    )
        return best_direct_metric is not None

    def __scatter_updates(self, updated_routes: tp.Dict[int, network_pb2.Route]) -> None:
        update_message = network_pb2.RouteTable(
            routes=updated_routes,
        )
        self.__scatterer.scatter_global(update_message.SerializeToString(), self.ROUTE_UPDATES_TOPIC)

    def __availability_checker_callback(self) -> None:
        reset_routes: tp.Dict[int, network_pb2.Route] = {}
        for group in self.__groups:
            if group not in self.__route_table.routes or \
                    not self.__metric_service.is_node_available(self.__route_table.routes[group].next_hop):
                if self.__set_naive_route(group):
                    reset_routes[group] = self.__route_table.routes[group]

        self.__scatter_updates(reset_routes)

    def get_route(self, target_group: int) -> tp.Optional[network_pb2.Route]:
        if target_group in self.__route_table.routes:
            return self.__route_table.routes[target_group]
        else:
            return None

    def get_next_hop(self, target_group: int) -> tp.Optional[int]:
        if self.__group == target_group or target_group not in self.__route_table.routes:
            return None
        else:
            return self.__route_table.routes[target_group].next_hop

    def handle_update(self, updated_routes: network_pb2.RouteTable, source_node: int) -> None:
        emergency_updates: tp.Dict[int, network_pb2.Route] = {}
        for group, route in updated_routes.routes.items():
            self.__update_route(group, source_node, route, emergency_updates)

        self.__scatter_updates(emergency_updates)

    def __update_route(self, target_group: int, proposed_next_hop: int,
                       proposed_route: network_pb2.Route,
                       emergency_updates: tp.Dict[int, network_pb2.Route]) -> None:
        proposed_metric = proposed_route.metric + self.__metric_service.get_direct_metric(proposed_next_hop)
        proposed_length = proposed_route.length + 1
        if target_group not in self.__route_table.routes:
            self.__route_table.routes[target_group] = network_pb2.Route(
                next_hop=proposed_next_hop,
                metric=proposed_metric,
                length=proposed_length,
            )
            emergency_updates[target_group] = self.__route_table.routes[target_group]
            return

        current_route = self.__route_table.routes[target_group]
        old_metric = current_route.metric
        if proposed_metric < current_route.metric or \
                (proposed_metric == current_route.metric and proposed_length < current_route.length):
            current_route.next_hop = proposed_next_hop
            current_route.metric = proposed_metric
            current_route.length = proposed_length
        elif current_route.next_hop == proposed_next_hop:
            current_route.metric = proposed_metric
            current_route.length = proposed_length

        if abs(old_metric - current_route.metric) >= self.__get_emergency_metric_delta():
            emergency_updates[target_group] = current_route

    def __get_emergency_metric_delta(self) -> float:
        pass


class RouterHandler(MessageHandlerStrategy):
    def __init__(self, router: Router):
        super().__init__()
        self.__router = router

    def handle(self, data: bytes, source_node: int) -> None:
        update_message = network_pb2.RouteTable()
        try:
            update_message.ParseFromString(data)
            self.__router.handle_update(update_message, source_node)
        except DecodeError:
            print('Can\'t parse UpdateMessage from data:', data)
