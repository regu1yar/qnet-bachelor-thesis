from . import routing_pb2
from timer import RandomShiftedTimer


class Router:
    SCATTER_RT_TIMEOUT = 25
    MAX_TIMER_SHIFT = 10

    def __init__(self, node_id, groups, metric_service, scatterer):
        self.__id = node_id
        self.__metric_service = metric_service
        self.__scatterer = scatterer
        self.__update_number = 0
        self.__last_seen_updates = {}
        self.__route_table = routing_pb2.RouteTable()
        for group in groups:
            best_direct_metric = None
            for node in group:
                if node == node_id:
                    self.__route_table[group] = routing_pb2.Route(next_hop=-1, metric=0)
                    self.group = group

                self.__last_seen_updates[group][node] = -1
                direct_metric = self.__metric_service.get_direct_metric(node)
                if best_direct_metric is None or best_direct_metric > direct_metric:
                    best_direct_metric = direct_metric
                    self.__route_table[group] = routing_pb2.Route(next_hop=node, metric=direct_metric)

        self.__scatter_rt_timer = RandomShiftedTimer(
            self.SCATTER_RT_TIMEOUT,
            self.MAX_TIMER_SHIFT,
            self.__scatter_rt_callback
        )

    async def __scatter_rt_callback(self):
        update = routing_pb2.UpdateMessage(
            update_number=self.__update_number,
            source_node=self.__id,
            update=self.__route_table,
        )
        await self.__scatterer.scatter(update.SerializeToString())

    def stop_scattering(self):
        if self.__scatter_rt_timer.is_active():
            self.__scatter_rt_timer.cancel()

    def start_scattering(self):
        if not self.__scatter_rt_timer.is_active():
            self.__scatter_rt_timer = RandomShiftedTimer(
                self.SCATTER_RT_TIMEOUT,
                self.MAX_TIMER_SHIFT,
                self.__scatter_rt_callback
            )

    def get_next_hop(self, target_group):
        if self.group == target_group:
            return None
        else:
            return self.__route_table[target_group].next_hop

    async def handle_update(self, update_message):
        if update_message.HasField('route_update'):
            await self.__update_route(
                update_message.route_update.target_group,
                update_message.source_node,
                update_message.route_update.route,
                update_message.update_number
            )
        elif update_message.HasField('route_table_update'):
            for group, route in update_message.route_table_update:
                await self.__update_route(group, update_message.source_node, route, update_message.update_number)

    async def __update_route(self, target_group, proposed_next_hop, proposed_route, update_number):
        if self.__last_seen_updates[target_group][proposed_next_hop] > update_number:
            return
        else:
            self.__last_seen_updates[target_group][proposed_next_hop] = update_number

        current_route = self.__route_table[target_group]
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
