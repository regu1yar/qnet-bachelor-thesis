import asyncio
import time
import typing as tp
from google.protobuf.message import DecodeError

from . import network_pb2
from .router import Router
from qnet2.config.net_config import Config
from . import network
from .network import DataHandler, UDPServer


class MessageHandlerStrategy:
    def handle(self, data: bytes, source_node: int) -> None:
        pass


class Scatterer:
    SCATTER_SERVER_PORT = 4201

    def __init__(self, config: Config, message_handlers: tp.Dict[str, MessageHandlerStrategy]) -> None:
        self.__config = config
        self.__message_handlers = message_handlers

        self.__id = config.get_local_node_id()

        groups_to_nodes_dict = config.get_groups_to_nodes_dict()
        self.__group_id = config.get_local_group_id()
        self.__group = groups_to_nodes_dict[self.__group_id]
        self.__groups = groups_to_nodes_dict.keys()

        self.__ttl_init_value = self.__calc_ttl_init_value(groups_to_nodes_dict)

        self.__router: tp.Optional[Router] = None

        self.__last_seen_topics_ts: tp.Dict[str, tp.Dict[int, int]] = {}

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

    def set_handler_strategy(self, topic: str, handler: MessageHandlerStrategy) -> None:
        self.__message_handlers[topic] = handler

    def stop(self) -> None:
        if self.__transport is not None:
            self.__transport.close()
        else:
            self.__run_server_task.cancel()

    def scatter_local(self, data: bytes, topic: str) -> None:
        self.handle_scatter_message(self.__construct_scatter_message(data, topic, 'global'))

    def scatter_global(self, data: bytes, topic: str) -> None:
        self.handle_scatter_message(self.__construct_scatter_message(data, topic, 'local'))

    def send_directly(self, data: bytes, topic: str, dest_node: int) -> None:
        asyncio.create_task(network.send_data(
            self.__config.get_ip_addr_by_node_id(dest_node),
            self.SCATTER_SERVER_PORT,
            self.__construct_scatter_message(data, topic, 'direct').SerializeToString()
        ))

    def __construct_scatter_message(self, data: bytes, topic: str, scatter_type: str) -> network_pb2.ScatterMessage:
        if topic not in self.__message_handlers:
            raise NotImplementedError('Message handler for topic \"{}\" is not provided'.format(topic))

        scatter_message = network_pb2.ScatterMessage(
            data=data,
            ttl=self.__ttl_init_value,
            topic=topic,
            source_node=self.__id,
            timestamp=int(time.time()),
            dest_groups=[],
        )

        if scatter_type == 'global':
            for group in self.__groups:
                scatter_message.dest_groups.append(group)
        elif scatter_type == 'local':
            scatter_message.dest_groups.append(self.__group_id)
        elif scatter_type == 'direct':
            pass
        else:
            raise NotImplementedError('Unexpected scatter type: {}'.format(scatter_type))

        return scatter_message

    def handle_scatter_message(self, message: network_pb2.ScatterMessage) -> None:
        assert self.__router is not None

        if message.topic in self.__last_seen_topics_ts and \
                message.source_node in self.__last_seen_topics_ts[message.topic] and \
                message.timestamp <= self.__last_seen_topics_ts[message.topic][message.source_node]:
            return

        if message.topic not in self.__last_seen_topics_ts:
            self.__last_seen_topics_ts[message.topic] = {}

        self.__last_seen_topics_ts[message.topic][message.source_node] = message.timestamp
        next_hops: tp.Dict[int, network_pb2.ScatterMessage] = {}
        message.ttl -= 1
        if message.ttl <= 0:
            self.__handle_expired_message(message)
            return

        for dest_group in message.dest_groups:
            if dest_group == self.__group_id:
                self.__scatter_within_group(message)
            else:
                next_hop = self.__router.get_next_hop(dest_group)
                if next_hop is None:
                    continue
                if next_hop in next_hops:
                    next_hops[next_hop].dest_groups.append(dest_group)
                else:
                    next_hops[next_hop].CopyFrom(message)
                    del next_hops[next_hop].dest_groups[:]
                    next_hops[next_hop].dest_groups.append(dest_group)

        for next_hop, message in next_hops.items():
            asyncio.create_task(network.send_data(
                self.__config.get_ip_addr_by_node_id(next_hop),
                self.SCATTER_SERVER_PORT,
                message.SerializeToString()
            ))

        if message.topic in self.__message_handlers:
            self.__message_handlers[message.topic].handle(message.data, message.source_node)

    def __handle_expired_message(self, message: network_pb2.ScatterMessage) -> None:
        assert message.ttl <= 0
        if self.__group_id in message.dest_groups:
            self.__scatter_within_group(message)

    def __scatter_within_group(self, message: network_pb2.ScatterMessage) -> None:
        internal_message = network_pb2.ScatterMessage(
            data=message.data,
            ttl=message.ttl,
            topic=message.topic,
            source_node=message.source_node,
            timestamp=message.timestamp,
            dest_groups=[],
        )

        for node in self.__group:
            if self.__id != node:
                asyncio.create_task(network.send_data(
                    self.__config.get_ip_addr_by_node_id(node),
                    self.SCATTER_SERVER_PORT,
                    internal_message.SerializeToString()
                ))


class ScatterHandler(DataHandler):
    def __init__(self, scatterer: Scatterer):
        super().__init__()
        self.__scatterer = scatterer

    def handle(self, data: bytes) -> None:
        scatter_message = network_pb2.ScatterMessage()
        try:
            scatter_message.ParseFromString(data)
            self.__scatterer.handle_scatter_message(scatter_message)
        except DecodeError:
            print('Can\'t parse ScatterMessage from data:', data)
