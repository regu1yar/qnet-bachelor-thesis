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

        self.__id = config.get_local_node_id()

        groups_to_nodes_dict = config.get_groups_to_nodes_dict()
        self.__group_id = config.get_local_group_id()
        self.__group = groups_to_nodes_dict[self.__group_id]
        self.__groups = groups_to_nodes_dict.keys()

        self.__ttl_init_value = self.__calc_ttl_init_value(groups_to_nodes_dict)

        self.__router: tp.Optional[Router] = None

        self.__topic_versions: tp.Dict[str, int] = {}
        self.__last_seen_topic_versions: tp.Dict[str, tp.Dict[int, int]] = {}

        self.__transport: tp.Optional[asyncio.BaseTransport] = None
        self.__run_server_task = asyncio.create_task(self.__run_server())

    @staticmethod
    def __calc_ttl_init_value(groups_to_nodes_dict: tp.Dict[int, tp.Set[int]]) -> int:
        return len(groups_to_nodes_dict) * 2 - 1

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

    async def scatter(self, data: bytes, topic: str) -> None:
        topic_version = 0
        if topic in self.__topic_versions:
            topic_version = self.__topic_versions[topic]

        self.__topic_versions[topic] = topic_version + 1
        scatter_message = routing_pb2.ScatterMessage(
            data=data,
            ttl=self.__ttl_init_value,
            topic=topic,
            source_node=self.__id,
            topic_version=topic_version,
            last_group=-1,
        )

        await self.handle_scatter_message(scatter_message)

    async def handle_scatter_message(self, message: routing_pb2.ScatterMessage) -> None:
        assert self.__router is not None

        if message.topic in self.__last_seen_topic_versions and \
                message.source_node in self.__last_seen_topic_versions[message.topic] and \
                message.topic_version <= self.__last_seen_topic_versions[message.topic][message.source_node]:
            return

        self.__last_seen_topic_versions[message.topic][message.source_node] = message.topic_version
        last_group = message.last_group
        message.ttl -= 1
        if message.ttl <= 0 and last_group != self.__group_id:
            await self.__scatter_within_group(message.data)
            return

        message.last_group = self.__group_id
        for group in self.__groups:
            if group == last_group:
                continue

            if group == self.__group_id:
                await self.__scatter_within_group(message.data)
                continue

            route_to_group = self.__router.get_route(group)
            if route_to_group.length == 1:
                await network.send_data(
                    self.__config.get_ip_addr_by_node_id(route_to_group.next_hop),
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
