import asyncio
import typing as tp

from . import routing_pb2
from .router import Router
from .net_config import Config
import network


class Scatterer:
    def __init__(self, group_id: int, group: tp.Set[int], groups: tp.Set[int], config: Config) -> None:
        self.__group_id = group_id
        self.__group = group
        self.__config = config
        self.__groups = groups
        self.__router: tp.Optional[Router] = None

    def set_router(self, router: Router) -> None:
        self.__router = router

    async def scatter(self, data: bytes) -> None:
        scatter_message = routing_pb2.ScatterMessage(
            dest_groups=[],
            data=data,
        )
        for group in self.__groups:
            scatter_message.dest_groups.append(group)

        await self.handle_scatter_message(scatter_message)

    async def handle_scatter_message(self, message: routing_pb2.ScatterMessage) -> None:
        assert self.__router is not None
        next_hops: tp.Dict[int, routing_pb2.ScatterMessage] = {}
        for dest_group in message.dest_groups:
            if dest_group == self.__group_id:
                await self.__scatter_within_group(message.data)
            else:
                next_hop = self.__router.get_next_hop(dest_group)
                assert next_hop is not None
                if next_hop in next_hops:
                    next_hops[next_hop].dest_groups.append(dest_group)
                else:
                    next_hops[next_hop] = routing_pb2.ScatterMessage(
                        dest_groups=[dest_group],
                        data=message.data,
                    )

        for next_hop, message in next_hops.items():
            await network.send_data(
                self.__config.get_ip_addr_by_node_id(next_hop),
                self.__config.get_app_port(),
                message.SerializeToString()
            )

    async def __scatter_within_group(self, data: bytes) -> None:
        for node in self.__group:
            await network.send_data(
                self.__config.get_ip_addr_by_node_id(node),
                self.__config.get_app_port(),
                data
            )
