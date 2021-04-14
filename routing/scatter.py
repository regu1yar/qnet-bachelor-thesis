import asyncio
import typing as tp
from google.protobuf.message import DecodeError

from . import routing_pb2
from .router import Router
from .net_config import Config
from . import network
from .network import DataHandler, UDPServer


class Scatterer:
    SCATTER_SERVER_PORT = 4201

    def __init__(self, config: Config) -> None:
        self.__config = config

        groups_to_nodes_dict = config.get_groups_to_nodes_dict()
        self.__group_id = config.get_local_group_id()
        self.__group = groups_to_nodes_dict[self.__group_id]
        self.__groups = groups_to_nodes_dict.keys()

        self.__ttl_init_value = self.__calc_ttl_init_value(groups_to_nodes_dict)

        self.__router: tp.Optional[Router] = None

        self.__transport: tp.Optional[asyncio.BaseTransport] = None
        self.__run_server_task = asyncio.create_task(self.__run_server())

    @staticmethod
    def __calc_ttl_init_value(groups_to_nodes_dict: tp.Dict[int, tp.Set[int]]) -> int:
        ttl_init_value = 0
        smallest_group_size: tp.Optional[int] = None
        for group_set in groups_to_nodes_dict.items():
            ttl_init_value += len(group_set)
            if smallest_group_size is None or len(group_set) < smallest_group_size:
                smallest_group_size = len(group_set)

        if ttl_init_value > 0 and smallest_group_size is not None:
            ttl_init_value -= smallest_group_size + 1

        return ttl_init_value

    async def __run_server(self) -> None:
        loop = asyncio.get_event_loop()
        self.__transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPServer(ScatterHandler(self)),
            local_addr=('0.0.0.0', self.SCATTER_SERVER_PORT))

    def set_router(self, router: Router) -> None:
        self.__router = router

    def stop(self) -> None:
        if self.__transport is not None:
            self.__transport.close()
        else:
            self.__run_server_task.cancel()

    async def scatter(self, data: bytes) -> None:
        scatter_message = routing_pb2.ScatterMessage(
            dest_groups=[],
            data=data,
            ttl=self.__ttl_init_value,
        )
        for group in self.__groups:
            scatter_message.dest_groups.append(group)

        await self.handle_scatter_message(scatter_message)

    async def handle_scatter_message(self, message: routing_pb2.ScatterMessage) -> None:
        assert self.__router is not None
        next_hops: tp.Dict[int, routing_pb2.ScatterMessage] = {}

        message.ttl -= 1
        if message.ttl <= 0:
            await self.__handle_expired_message(message)
            return

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
                        ttl=message.ttl
                    )

        for next_hop, message in next_hops.items():
            await network.send_data(
                self.__config.get_ip_addr_by_node_id(next_hop),
                self.SCATTER_SERVER_PORT,
                message.SerializeToString()
            )

    async def __handle_expired_message(self, message: routing_pb2.ScatterMessage) -> None:
        assert message.ttl <= 0
        if self.__group_id in message.dest_groups:
            await self.__scatter_within_group(message.data)

    async def __scatter_within_group(self, data: bytes) -> None:
        for node in self.__group:
            await network.send_data(
                self.__config.get_ip_addr_by_node_id(node),
                Router.get_server_port(),
                data
            )


class ScatterHandler(DataHandler):
    def __init__(self, scatterer: Scatterer):
        super().__init__()
        self.__scatterer = scatterer

    async def handle(self, data: bytes) -> None:
        scatter_message = routing_pb2.ScatterMessage()
        try:
            scatter_message.ParseFromString(data)
            await self.__scatterer.handle_scatter_message(scatter_message)
        except DecodeError:
            print('Can\'t parse ScatterMessage from data:', data)
