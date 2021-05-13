import asyncio
import typing as tp

from . import routing_pb2
from .net_config import Config
from .repeater import Repeater
from .scatter import Scatterer
from .routing_pb2 import OTHER, REDUCER, BACKUP_REDUCER


class ReducerDesignator:
    __SEND_HEARTBEAT_TIMEOUT = 10
    __DEAD_TIMEOUT = __SEND_HEARTBEAT_TIMEOUT * 3

    HEARTBEAT_TOPIC = 'heartbeat'

    def __init__(self, config: Config, scatterer: Scatterer):
        self.__id = config.get_local_group_id()
        self.__scatterer = scatterer
        self.__reducer = -1
        self.__backup_reducer = -1
        self.__received_heartbeats: tp.Dict[int, routing_pb2.HeartbeatMessage] = {}

        self.__heartbeat_timer = Repeater(self.__SEND_HEARTBEAT_TIMEOUT, self.__send_heartbeat)
        self.__availability_checker = Repeater(self.__DEAD_TIMEOUT, self.__check_availability)

    async def __send_heartbeat(self) -> None:
        heartbeat = routing_pb2.HeartbeatMessage(state=OTHER)
        if self.__id == self.__reducer:
            heartbeat.state = REDUCER
        elif self.__id == self.__backup_reducer:
            heartbeat.state = BACKUP_REDUCER

        await self.__scatterer.scatter_local(heartbeat.SerializeToString(), self.HEARTBEAT_TOPIC)

    async def __check_availability(self) -> None:
        if self.__reducer not in self.__received_heartbeats or self.__backup_reducer not in self.__received_heartbeats:
            await self.__reelect()

        self.__received_heartbeats.clear()

    def handle_heartbeat(self, heartbeat: routing_pb2.HeartbeatMessage, source_node: int) -> None:
        self.__received_heartbeats[source_node] = heartbeat
        if heartbeat.state == REDUCER:
            if self.__reducer < source_node:
                self.__reducer = source_node
                if self.__reducer == self.__backup_reducer:
                    self.__backup_reducer = -1
        elif heartbeat.state == BACKUP_REDUCER:
            if self.__backup_reducer < source_node:
                self.__backup_reducer = source_node
                if self.__reducer == self.__backup_reducer:
                    self.__reducer = -1
        elif heartbeat.state == OTHER:
            if self.__reducer == source_node:
                self.__reducer = -1
            elif self.__backup_reducer == source_node:
                self.__backup_reducer = -1

    async def __reelect(self) -> None:
        if self.__reducer not in self.__received_heartbeats:
            self.__reducer = self.__backup_reducer
            self.__backup_reducer = -1

        available_nodes = self.__received_heartbeats.keys()
        if len(available_nodes) <= 1:
            self.__reducer = self.__id
            self.__backup_reducer = --1
            await self.__send_heartbeat()
            return

        possible_backups = set(filter(
            lambda node: self.__received_heartbeats[node].state != REDUCER and node != self.__reducer,
            available_nodes,
        ))

        self_proclaimed_backups = set(filter(
            lambda node: self.__received_heartbeats[node].state == BACKUP_REDUCER,
            possible_backups
        ))

        if len(self_proclaimed_backups) != 0:
            self.__backup_reducer = max(self_proclaimed_backups)
        elif len(possible_backups) != 0:
            self.__backup_reducer = max(possible_backups)

        if self.__reducer not in self.__received_heartbeats:
            possible_reducers = set(filter(
                lambda node: node != self.__backup_reducer,
                available_nodes
            ))

            self_proclaimed_reducers = set(filter(
                lambda node: self.__received_heartbeats[node].state == REDUCER,
                possible_reducers,
            ))

            if len(self_proclaimed_reducers) != 0:
                self.__reducer = max(self_proclaimed_reducers)
            elif len(possible_reducers) != 0:
                self.__reducer = max(possible_reducers)

        if self.__id == self.__reducer or self.__id == self.__backup_reducer:
            await self.__send_heartbeat()
