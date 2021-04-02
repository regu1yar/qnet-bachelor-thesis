from functools import partial
import typing as tp

from . import routing_pb2
from .repeater import RandomShiftedRepeater, Repeater
from .metrics import MetricService
from .scatter import Scatterer


class Router:
    SCATTER_RT_TIMEOUT = 30
    MAX_TIMER_SHIFT = 5
    KEEPALIVE_TIMEOUT = SCATTER_RT_TIMEOUT * 3

    def __init__(self, node_id: int, groups: tp.Dict[int, tp.Set[int]],
                 metric_service: MetricService, scatterer: Scatterer):
        self.__id = node_id
        self.__group: tp.Optional[int] = None

        self.__groups = groups
        self.__metric_service = metric_service
        self.__scatterer = scatterer

        self.__available_nodes: tp.Set[int] = set()
        self.__available_nodes.add(self.__id)

        self.__rt_version = 0
        self.__last_seen_rt_versions: tp.Dict[int, tp.Dict[int, int]] = {}

        self.__route_table = routing_pb2.RouteTable()
        for group_id in self.__groups:
            self.__set_naive_route(group_id)

        self.__scatter_rt_timer = RandomShiftedRepeater(
            self.SCATTER_RT_TIMEOUT,
            -self.MAX_TIMER_SHIFT,
            self.MAX_TIMER_SHIFT,
            partial(self.__scatter_rt_callback, self.__route_table.routes)
        )
        self.__availability_checker = Repeater(self.KEEPALIVE_TIMEOUT, self.__availability_checker_callback)

    def __set_naive_route(self, target_group_id: int) -> None:
        if target_group_id == self.__group:
            return

        best_direct_metric: tp.Optional[float] = None
        for node in self.__groups[target_group_id]:
            if node == self.__id and self.__group is None:
                self.__route_table.routes[target_group_id] = routing_pb2.Route(next_hop=-1, metric=0)
                self.__group = target_group_id
                return

            direct_metric = self.__metric_service.get_direct_metric(node)
            if best_direct_metric is None or best_direct_metric > direct_metric:
                best_direct_metric = direct_metric
                self.__route_table.routes[target_group_id] = routing_pb2.Route(next_hop=node, metric=direct_metric)

    async def __scatter_rt_callback(self) -> None:
        update = routing_pb2.UpdateMessage(
            rt_version=self.__rt_version,
            source_node=self.__id,
            updated_routes=self.__route_table.routes,
        )
        self.__rt_version += 1
        await self.__scatterer.scatter(update.SerializeToString())

    async def __availability_checker_callback(self) -> None:
        reset_routes: tp.Dict[int, routing_pb2.Route] = {}
        for group, route in self.__route_table.routes.items():
            if route.next_hop not in self.__available_nodes:
                self.__set_naive_route(group)
                reset_routes[group] = self.__route_table.routes[group]

        await self.__scatter_updates(reset_routes)
        self.__available_nodes.clear()
        self.__available_nodes.add(self.__id)

    def stop_scattering(self) -> None:
        self.__scatter_rt_timer.cancel()

    def restart_scattering(self) -> None:
        self.__scatter_rt_timer = RandomShiftedRepeater(
            self.SCATTER_RT_TIMEOUT,
            -self.MAX_TIMER_SHIFT,
            self.MAX_TIMER_SHIFT,
            self.__scatter_rt_callback
        )

    def get_next_hop(self, target_group: int) -> tp.Optional[int]:
        if self.__group == target_group:
            return None
        else:
            return self.__route_table.routes[target_group].next_hop

    async def handle_update(self, update_message: routing_pb2.UpdateMessage) -> None:
        self.__available_nodes.add(update_message.source_node)
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
        if proposed_metric < current_route.metric:
            current_route.next_hop = proposed_next_hop
            current_route.metric = proposed_metric
        elif current_route.next_hop == proposed_next_hop:
            old_metric = current_route.metric
            current_route.metric = proposed_metric
            if old_metric - current_route.metric >= self.__metric_service.get_emergency_metric_delta():
                emergency_updates[target_group] = current_route

    async def __scatter_updates(self, updated_routes: tp.Dict[int, routing_pb2.Route]) -> None:
        emergency_update = routing_pb2.UpdateMessage(
            rt_version=self.__rt_version,
            source_node=self.__id,
            updated_routes=updated_routes,
        )
        self.__rt_version += 1
        await self.__scatterer.scatter(emergency_update.SerializeToString())
