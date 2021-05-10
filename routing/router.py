import asyncio
from functools import partial
import typing as tp
from google.protobuf.message import DecodeError

from . import routing_pb2
from .repeater import RandomShiftedRepeater, Repeater
from .metrics import MetricService
from .scatter import Scatterer, MessageHandlerStrategy
from .net_config import Config


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

        self.__route_table = routing_pb2.RouteTable()
        self.__route_table.routes[self.__group] = routing_pb2.Route(next_hop=self.__id, metric=0, length=0)
        for group_id in self.__groups:
            self.__set_naive_route(group_id)

        self.__scatterer.set_router(self)
        self.__scatterer.set_handler_strategy(self.ROUTE_UPDATES_TOPIC, RouterHandler(self))

        self.__scatter_rt_timer = RandomShiftedRepeater(
            self.__SCATTER_RT_TIMEOUT,
            -self.__MAX_TIMER_SHIFT,
            self.__MAX_TIMER_SHIFT,
            partial(self.__scatter_updates, self.__route_table.routes)
        )
        self.__availability_checker = Repeater(self.__KEEPALIVE_TIMEOUT, self.__availability_checker_callback)

    def __set_naive_route(self, target_group_id: int) -> None:
        if target_group_id == self.__group:
            return

        best_direct_metric: tp.Optional[float] = None
        for node in self.__groups[target_group_id]:
            direct_metric = self.__metric_service.get_direct_metric(node)
            if best_direct_metric is None or best_direct_metric > direct_metric:
                best_direct_metric = direct_metric
                self.__route_table.routes[target_group_id] = routing_pb2.Route(
                    next_hop=node, metric=direct_metric, length=1
                )

    async def __scatter_updates(self, updated_routes: tp.Dict[int, routing_pb2.Route]) -> None:
        update_message = routing_pb2.RouteUpdateMessage(
            source_node=self.__id,
            updated_routes=updated_routes,
        )
        await self.__scatterer.scatter_global(update_message.SerializeToString(), self.ROUTE_UPDATES_TOPIC)

    async def __availability_checker_callback(self) -> None:
        reset_routes: tp.Dict[int, routing_pb2.Route] = {}
        for group, route in self.__route_table.routes.items():
            if not self.__metric_service.is_node_available(route.next_hop):
                self.__set_naive_route(group)
                reset_routes[group] = self.__route_table.routes[group]

        await self.__scatter_updates(reset_routes)

    def get_route(self, target_group: int) -> routing_pb2.Route:
        return self.__route_table.routes[target_group]

    def get_next_hop(self, target_group: int) -> tp.Optional[int]:
        if self.__group == target_group:
            return None
        else:
            return self.__route_table.routes[target_group].next_hop

    async def handle_update(self, update_message: routing_pb2.RouteUpdateMessage) -> None:
        emergency_updates: tp.Dict[int, routing_pb2.Route] = {}
        for group, route in update_message.updated_routes.items():
            self.__update_route(group, update_message.source_node, route, emergency_updates)

        await self.__scatter_updates(emergency_updates)

    def __update_route(self, target_group: int, proposed_next_hop: int,
                       proposed_route: routing_pb2.Route,
                       emergency_updates: tp.Dict[int, routing_pb2.Route]) -> None:
        current_route = self.__route_table.routes[target_group]
        proposed_metric = proposed_route.metric + self.__metric_service.get_direct_metric(proposed_next_hop)
        proposed_length = proposed_route.length + 1
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

    async def handle(self, data: bytes) -> None:
        update_message = routing_pb2.RouteUpdateMessage()
        try:
            update_message.ParseFromString(data)
            await self.__router.handle_update(update_message)
        except DecodeError:
            print('Can\'t parse UpdateMessage from data:', data)
