from . import routing_pb2
from repeater import RandomShiftedRepeater, Repeater


class Router:
    SCATTER_RT_TIMEOUT = 25
    MAX_TIMER_SHIFT = 10
    KEEPALIVE_TIMEOUT = (SCATTER_RT_TIMEOUT + (MAX_TIMER_SHIFT + 1) // 2) * 3

    def __init__(self, node_id, groups, metric_service, scatterer):
        self.__id = node_id
        self.__group = None

        self.__groups = groups
        self.__metric_service = metric_service
        self.__scatterer = scatterer

        self.__alive_nodes = set()
        self.__alive_nodes.add(self.__id)

        self.__update_number = 0
        self.__last_seen_updates = {}

        self.__route_table = routing_pb2.RouteTable()
        for group_id in self.__groups:
            self.__set_naive_route(group_id)

        self.__scatter_rt_timer = RandomShiftedRepeater(
            self.SCATTER_RT_TIMEOUT,
            self.MAX_TIMER_SHIFT,
            self.__scatter_rt_callback
        )
        self.__check_aliveness_timer = Repeater(self.KEEPALIVE_TIMEOUT, self.__check_aliveness_callback)

    def __set_naive_route(self, target_group_id):
        if target_group_id == self.__group:
            return

        best_direct_metric = None
        for node in self.__groups[target_group_id]:
            if node == self.__id and self.__group is None:
                self.__route_table.routes[target_group_id] = routing_pb2.Route(next_hop=-1, metric=0)
                self.__group = target_group_id
                return

            direct_metric = self.__metric_service.get_direct_metric(node)
            if best_direct_metric is None or best_direct_metric > direct_metric:
                best_direct_metric = direct_metric
                self.__route_table.routes[target_group_id] = routing_pb2.Route(next_hop=node, metric=direct_metric)

    async def __scatter_rt_callback(self):
        update = routing_pb2.UpdateMessage(
            update_number=self.__update_number,
            source_node=self.__id,
            update=self.__route_table,
        )
        self.__update_number += 1
        await self.__scatterer.scatter(update.SerializeToString())

    async def __check_aliveness_callback(self):
        for group, route in self.__route_table.routes.items():
            if route.next_hop not in self.__alive_nodes:
                self.__set_naive_route(group)

        self.__alive_nodes.clear()
        self.__alive_nodes.add(self.__id)

    def stop_scattering(self):
        self.__scatter_rt_timer.cancel()

    def restart_scattering(self):
        self.__scatter_rt_timer = RandomShiftedRepeater(
            self.SCATTER_RT_TIMEOUT,
            self.MAX_TIMER_SHIFT,
            self.__scatter_rt_callback
        )

    def get_next_hop(self, target_group):
        if self.__group == target_group:
            return None
        else:
            return self.__route_table.routes[target_group].next_hop

    async def handle_update(self, update_message):
        self.__alive_nodes.add(update_message.source_node)
        if update_message.HasField('route_update'):
            await self.__update_route(
                update_message.route_update.target_group,
                update_message.source_node,
                update_message.route_update.route,
                update_message.update_number
            )
        elif update_message.HasField('route_table_update'):
            for group, route in update_message.route_table_update.routes.items():
                await self.__update_route(group, update_message.source_node, route, update_message.update_number)

    async def __update_route(self, target_group, proposed_next_hop, proposed_route, update_number):
        if self.__last_seen_updates[target_group][proposed_next_hop] > update_number:
            return
        else:
            self.__last_seen_updates[target_group][proposed_next_hop] = update_number

        current_route = self.__route_table.routes[target_group]
        proposed_metric = proposed_route.metric + self.__metric_service.get_direct_metric(proposed_next_hop)
        if proposed_metric < current_route.metric:
            current_route.next_hop = proposed_next_hop
            current_route.metric = proposed_metric
        elif current_route.next_hop == proposed_next_hop:
            old_metric = current_route.metric
            current_route.metric = proposed_metric
            if old_metric - current_route.metric >= self.__metric_service.get_emergency_metric_delta():
                emergency_update = routing_pb2.UpdateMessage(
                    update_number=self.__update_number,
                    source_node=self.__id,
                    update=routing_pb2.TargetedRoute(target_group=target_group, route=current_route),
                )
                self.__update_number += 1
                await self.__scatterer.scatter(emergency_update.SerializeToString())
