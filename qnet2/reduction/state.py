from bitarray import bitarray
import typing as tp

import reduction_pb2
from .reduction_pb2 import ReductionResult, NeighbourState, HashMapReductionValues, ArrayReductionValues, REDUCER, \
    BACKUP_REDUCER, OTHER
from .reduction_strategy import ReductionStrategy
from qnet2.config.net_config import Config
from qnet2.utils.timer import RepeaterSyncCallback
from .communication import ReductionScatterer, LocalValueSender
from .designator import ReducerDesignator


class ReducerContext:
    SEND_GROUP_RESULT_TIMEOUT = 5

    def __init__(self, config: Config, scatterer: ReductionScatterer, local_sender: LocalValueSender,
                 reduction_strategy: ReductionStrategy, init_state: State):
        self.__id = config.get_local_node_id()
        self.__max_node_id = self.__calc_max_node_id(config)
        self.__scatterer = scatterer
        self.local_sender = local_sender
        self.__reduction_strategy = reduction_strategy
        self.designator: tp.Optional[ReducerDesignator] = None

        self.__state: tp.Optional[State] = None

        self.__cur_reduction_result: tp.Union[HashMapReductionValues, ArrayReductionValues] = \
            self.__reduction_strategy.generate_neutral()
        self.__cur_reduction_set = bitarray(self.__max_node_id)
        self.__cur_reduction_set.setall(0)

        self.transition_to(init_state)
        self.__send_repeater = RepeaterSyncCallback(self.SEND_GROUP_RESULT_TIMEOUT, self.handle_send_timeout)

    @staticmethod
    def __calc_max_node_id(config: Config) -> int:
        max_node_id = 0
        for group in config.get_groups_to_nodes_dict().values():
            max_node_id = max(max_node_id, max(group))

        return max_node_id

    def transition_to(self, new_state: State) -> None:
        self.__state = new_state
        self.__state.context = self

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.__state is not None:
            self.__state.handle_reduction_message(message)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if self.__state is not None:
            self.__state.handle_state_transition(to_state)

    def handle_send_timeout(self) -> None:
        if self.__state is not None:
            self.__state.handle_send_timeout()

    def reduce_with(self, message: ReductionResult) -> None:
        message_reduction_set = bitarray()
        message_reduction_set.frombytes(message.reduction_set_mask)
        if not (self.__cur_reduction_set & message_reduction_set).any():
            self.__cur_reduction_set |= message_reduction_set
            if message.values.HasField('hash_map_values'):
                self.__cur_reduction_result = self.__reduction_strategy.reduce(
                    self.__cur_reduction_result,
                    message.values.hash_map_values
                )
            elif message.values.HasField('array_values'):
                self.__cur_reduction_result = self.__reduction_strategy.reduce(
                    self.__cur_reduction_result,
                    message.values.array_values
                )
            else:
                self.__cur_reduction_set ^= message_reduction_set

    def clear_reduction_result(self) -> None:
        self.__cur_reduction_set = bitarray(self.__max_node_id)
        self.__cur_reduction_set.setall(0)
        self.__cur_reduction_result = self.__reduction_strategy.generate_neutral()

    def scatter_reduction_result(self) -> None:
        reduction_values = reduction_pb2.ReductionValues()
        if isinstance(self.__cur_reduction_result, HashMapReductionValues):
            reduction_values.hash_map_values.CopyFrom(self.__cur_reduction_result)
        elif isinstance(self.__cur_reduction_result, ArrayReductionValues):
            reduction_values.array_values.CopyFrom(self.__cur_reduction_result)

        self.__scatterer.scatter_group_result(ReductionResult(
            reduction_set_mask=self.__cur_reduction_set.tobytes(),
            values=reduction_values,
        ))


class State:
    def __init__(self) -> None:
        self.context: tp.Optional[ReducerContext] = None

    def handle_reduction_message(self, message: ReductionResult) -> None:
        pass

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        pass

    def handle_send_timeout(self) -> None:
        pass


class ReducerState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.context is not None:
            self.context.reduce_with(message)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == REDUCER or self.context is None:
            return
        elif to_state == BACKUP_REDUCER:
            self.context.transition_to(PreBackupState())
        elif to_state == OTHER:
            self.context.transition_to(TempReducerState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )

    def handle_send_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.clear_reduction_result()


class PreBackupState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.context is not None:
            self.context.reduce_with(message)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == BACKUP_REDUCER or self.context is None:
            return
        elif to_state == REDUCER:
            self.context.transition_to(ReducerState())
        elif to_state == OTHER:
            self.context.transition_to(TempReducerState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )

    def handle_send_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.clear_reduction_result()
            self.context.transition_to(BackupState())


class TempReducerState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.context is not None:
            self.context.reduce_with(message)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == OTHER or self.context is None:
            return
        elif to_state == REDUCER:
            self.context.transition_to(ReducerState())
        elif to_state == BACKUP_REDUCER:
            self.context.transition_to(PreBackupState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )

    def handle_send_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.clear_reduction_result()
            self.context.transition_to(OtherState())


class BackupState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.context is not None:
            self.context.reduce_with(message)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == BACKUP_REDUCER or self.context is None:
            return
        elif to_state == REDUCER:
            self.context.transition_to(ReducerState())
        elif to_state == OTHER:
            self.context.clear_reduction_result()
            self.context.transition_to(OtherState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )

    def handle_send_timeout(self) -> None:
        if self.context is not None:
            self.context.clear_reduction_result()


class OtherState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_reduction_message(self, message: ReductionResult) -> None:
        if self.context is None:
            return
        if self.context.designator is None or self.context.designator.get_reducer() < 0:
            self.context.transition_to(TempReducerState())
            self.context.handle_reduction_message(message)
        else:
            self.context.local_sender.send_local_result(message, self.context.designator.get_reducer())

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == OTHER or self.context is None:
            return
        elif to_state == REDUCER:
            self.context.transition_to(ReducerState())
        elif to_state == BACKUP_REDUCER:
            self.context.transition_to(BackupState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )
