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

    def __init__(self, group_id: int, group: tp.Set[int], groups: tp.Set[int], config: Config) -> None:
        self.__group_id = group_id
        self.__group = group
        self.__config = config
        self.__groups = groups
        self.__router: tp.Optional[Router] = None

        self.__transport: tp.Optional[asyncio.BaseTransport] = None
        self.__run_server_task = asyncio.create_task(self.__run_server())

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
                self.SCATTER_SERVER_PORT,
                message.SerializeToString()
            )

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
