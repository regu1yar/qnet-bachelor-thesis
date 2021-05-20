import asyncio
import time
import typing as tp

from . import routing_pb2
from .net_config import Config
from .timer import Repeater, RepeaterSyncCallback
from .scatter import Scatterer
from .routing_pb2 import OTHER, REDUCER, BACKUP_REDUCER


class NeighbourRevision:
    def __init__(self, node_id: int, start_ts: int):
        self.id = node_id
        self.start_ts = start_ts


class ReducerDesignator:
    __SEND_HEARTBEAT_TIMEOUT = 10
    __DEAD_TIMEOUT = __SEND_HEARTBEAT_TIMEOUT * 3
    __STATE_EXPIRATION = 1

    HEARTBEAT_TOPIC = 'heartbeat'

    def __init__(self, config: Config, scatterer: Scatterer):
        self.__id = config.get_local_group_id()
        self.__start_ts = int(time.time())
        self.__scatterer = scatterer
        self.__reducer = NeighbourRevision(-1, -1)
        self.__backup_reducer = NeighbourRevision(-1, -1)
        self.__reducer_till_expiration = -1
        self.__backup_till_expiration = -1
        self.__received_heartbeats: tp.Dict[int, routing_pb2.HeartbeatMessage] = {}

        self.__heartbeat_timer = Repeater(self.__SEND_HEARTBEAT_TIMEOUT, self.__send_heartbeat, True)
        self.__availability_checker = RepeaterSyncCallback(self.__DEAD_TIMEOUT, self.__check_availability, False)

    def handle_heartbeat(self, heartbeat: routing_pb2.HeartbeatMessage, source_node: int) -> None:
        self.__received_heartbeats[source_node] = heartbeat
        source_revision = NeighbourRevision(source_node, heartbeat.start_ts)
        if heartbeat.state == REDUCER:
            if self.__reducer.id < source_revision.id or \
                    self.__is_new_revision(self.__reducer, source_revision):
                self.__set_reducer(source_revision)
                if self.__reducer.id == self.__backup_reducer.id:
                    self.__set_backup(NeighbourRevision(-1, -1))
            elif self.__is_same(self.__reducer, source_revision):
                self.__reducer_till_expiration = self.__STATE_EXPIRATION
        elif heartbeat.state == BACKUP_REDUCER:
            if self.__is_same(self.__reducer, source_revision):
                return
            if self.__backup_reducer.id < source_revision.id or \
                    self.__is_new_revision(self.__backup_reducer, source_revision):
                self.__set_backup(source_revision)
            elif self.__is_same(self.__backup_reducer, source_revision):
                self.__backup_till_expiration = self.__STATE_EXPIRATION
        elif heartbeat.state == OTHER:
            if self.__is_new_revision(self.__reducer, source_revision):
                self.__set_reducer(NeighbourRevision(-1, -1))
            elif self.__is_new_revision(self.__backup_reducer, source_revision):
                self.__set_backup(NeighbourRevision(-1, -1))

    @staticmethod
    def __is_new_revision(prev: NeighbourRevision, cur: NeighbourRevision) -> bool:
        return prev.id == cur.id and prev.start_ts < cur.start_ts

    @staticmethod
    def __is_same(node_1: NeighbourRevision, node_2: NeighbourRevision) -> bool:
        return node_1.id == node_2.id and node_1.start_ts == node_2.start_ts

    async def __send_heartbeat(self) -> None:
        heartbeat = routing_pb2.HeartbeatMessage(state=OTHER, start_ts=self.__start_ts)
        if self.__reducer.id == self.__id:
            heartbeat.state = REDUCER
        elif self.__backup_reducer.id == self.__id:
            heartbeat.state = BACKUP_REDUCER

        await self.__scatterer.scatter_local(heartbeat.SerializeToString(), self.HEARTBEAT_TOPIC)

    def __check_availability(self) -> None:
        if not self.__is_node_alive(self.__reducer) or not self.__is_node_alive(self.__backup_reducer):
            self.__reelect()
        else:
            if not self.__is_in_state(self.__reducer.id, REDUCER):
                self.__reducer_till_expiration -= 1
            if not self.__is_in_state(self.__backup_reducer.id, BACKUP_REDUCER):
                self.__backup_till_expiration -= 1
            if self.__reducer_till_expiration < 0 or self.__backup_till_expiration < 0:
                self.__reelect()

        self.__received_heartbeats.clear()

    def __is_node_alive(self, node: NeighbourRevision) -> bool:
        return node.id in self.__received_heartbeats and \
               node.start_ts == self.__received_heartbeats[node.id].start_ts

    def __is_in_state(self, node_id: int, required_state: routing_pb2.NeighbourState.V) -> bool:
        if node_id not in self.__received_heartbeats:
            return False
        return self.__received_heartbeats[node_id].state == required_state

    def __reelect(self) -> None:
        if not self.__is_node_alive(self.__reducer) or self.__reducer_till_expiration < 0:
            self.__set_reducer(self.__backup_reducer)
            self.__set_backup(NeighbourRevision(-1, -1))

        available_nodes = self.__received_heartbeats.keys()
        if len(available_nodes) <= 1:
            self.__set_reducer(NeighbourRevision(self.__id, self.__start_ts))
            self.__set_backup(NeighbourRevision(-1, -1))
            return

        possible_backups = set(filter(
            lambda node: self.__received_heartbeats[node].state != REDUCER and (
                    node != self.__reducer.id or
                    self.__received_heartbeats[node].start_ts != self.__reducer.start_ts
            ),
            available_nodes,
        ))

        self_proclaimed_backups = set(filter(
            lambda node: self.__received_heartbeats[node].state == BACKUP_REDUCER,
            possible_backups
        ))

        self.__set_backup(self.__elect_from(possible_backups, self_proclaimed_backups))
        if self.__is_node_alive(self.__reducer):
            return

        possible_reducers = set(filter(
            lambda node: node != self.__backup_reducer.id,
            available_nodes,
        ))

        self_proclaimed_reducers = set(filter(
            lambda node: self.__received_heartbeats[node].state == REDUCER,
            possible_reducers,
        ))

        self.__set_reducer(self.__elect_from(possible_reducers, self_proclaimed_reducers))

    def __set_reducer(self, node: NeighbourRevision) -> None:
        self.__reducer = node
        if node.id >= 0:
            self.__reducer_till_expiration = self.__STATE_EXPIRATION
        else:
            self.__reducer_till_expiration = -1

    def __set_backup(self, node: NeighbourRevision) -> None:
        self.__backup_reducer = node
        if node.id >= 0:
            self.__backup_till_expiration = self.__STATE_EXPIRATION
        else:
            self.__backup_till_expiration = -1

    def __elect_from(self, possible: tp.Set[int], self_proclaimed: tp.Set[int]) -> NeighbourRevision:
        new_elect = -1
        if len(self_proclaimed) != 0:
            new_elect = max(self_proclaimed)
        elif len(possible) != 0:
            new_elect = max(possible)

        if new_elect != -1:
            return NeighbourRevision(
                new_elect,
                self.__received_heartbeats[new_elect].start_ts,
            )
        else:
            return NeighbourRevision(-1, -1)
