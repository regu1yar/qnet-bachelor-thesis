import asyncio
from functools import partial
import typing as tp
from google.protobuf.message import DecodeError

from . import routing_pb2
from .repeater import RandomShiftedRepeater, Repeater
from .metrics import MetricService
from .scatter import Scatterer
from .network import DataHandler, UDPServer
from .net_config import Config


class Router:
    __SCATTER_RT_TIMEOUT = 30
    __MAX_TIMER_SHIFT = 5
    __KEEPALIVE_TIMEOUT = __SCATTER_RT_TIMEOUT

    ROUTER_SERVER_PORT = 4200

    def __init__(self, config: Config, metric_service: MetricService, scatterer: Scatterer):
        self.__id = config.get_local_node_id()
        self.__group = config.get_local_group_id()

        self.__groups = config.get_groups_to_nodes_dict()
        self.__metric_service = metric_service
        self.__scatterer = scatterer

        self.__rt_version = 0
        self.__last_seen_rt_versions: tp.Dict[int, tp.Dict[int, int]] = {}

        self.__route_table = routing_pb2.RouteTable()
        self.__route_table.routes[self.__group] = routing_pb2.Route(next_hop=-1, metric=0)
        for group_id in self.__groups:
            self.__set_naive_route(group_id)

        self.__transport: tp.Optional[asyncio.BaseTransport] = None
        self.__run_server_task = asyncio.create_task(self.__run_server())

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
                self.__route_table.routes[target_group_id] = routing_pb2.Route(next_hop=node, metric=direct_metric)

    async def __run_server(self) -> None:
        loop = asyncio.get_event_loop()
        self.__transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPServer(RouterHandler(self)),
            local_addr=('0.0.0.0', self.ROUTER_SERVER_PORT))

    async def __scatter_updates(self, updated_routes: tp.Dict[int, routing_pb2.Route]) -> None:
        emergency_update = routing_pb2.UpdateMessage(
            rt_version=self.__rt_version,
            source_node=self.__id,
            updated_routes=updated_routes,
        )
        self.__rt_version += 1
        await self.__scatterer.scatter(emergency_update.SerializeToString())

    async def __availability_checker_callback(self) -> None:
        reset_routes: tp.Dict[int, routing_pb2.Route] = {}
        for group, route in self.__route_table.routes.items():
            if not self.__metric_service.is_node_available(route.next_hop):
                self.__set_naive_route(group)
                reset_routes[group] = self.__route_table.routes[group]

        await self.__scatter_updates(reset_routes)

    @staticmethod
    def get_server_port() -> int:
        return Router.ROUTER_SERVER_PORT

    def stop(self) -> None:
        self.__scatter_rt_timer.cancel()
        if self.__transport is not None:
            self.__transport.close()
        else:
            self.__run_server_task.cancel()

    def get_next_hop(self, target_group: int) -> tp.Optional[int]:
        if self.__group == target_group:
            return None
        else:
            return self.__route_table.routes[target_group].next_hop

    async def handle_update(self, update_message: routing_pb2.UpdateMessage) -> None:
        emergency_updates: tp.Dict[int, routing_pb2.Route] = {}
        for group, route in update_message.updated_routes.items():
            self.__update_route(group, update_message.source_node, route, update_message.rt_version, emergency_updates)

        await self.__scatter_updates(emergency_updates)

    def __update_route(self, target_group: int, proposed_next_hop: int,
                       proposed_route: routing_pb2.Route, rt_version: int,
                       emergency_updates: tp.Dict[int, routing_pb2.Route]) -> None:
        if proposed_next_hop in self.__last_seen_rt_versions[target_group] and \
                self.__last_seen_rt_versions[target_group][proposed_next_hop] > rt_version:
            return
        else:
            self.__last_seen_rt_versions[target_group][proposed_next_hop] = rt_version

        current_route = self.__route_table.routes[target_group]
        proposed_metric = proposed_route.metric + self.__metric_service.get_direct_metric(proposed_next_hop)
        old_metric = current_route.metric
        if proposed_metric < current_route.metric:
            current_route.next_hop = proposed_next_hop
            current_route.metric = proposed_metric
        elif current_route.next_hop == proposed_next_hop:
            current_route.metric = proposed_metric

        if abs(old_metric - current_route.metric) >= self.__get_emergency_metric_delta():
            emergency_updates[target_group] = current_route

    def __get_emergency_metric_delta(self) -> float:
        pass


class RouterHandler(DataHandler):
    def __init__(self, router: Router):
        super().__init__()
        self.__router = router

    async def handle(self, data: bytes) -> None:
        update_message = routing_pb2.UpdateMessage()
        try:
            update_message.ParseFromString(data)
            await self.__router.handle_update(update_message)
        except DecodeError:
            print('Can\'t parse UpdateMessage from data:', data)
