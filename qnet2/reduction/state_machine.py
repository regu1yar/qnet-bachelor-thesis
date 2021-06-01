import asyncio
import typing as tp

import reduction_pb2
from .reduction_pb2 import NeighbourState, IndividualValues, REDUCER, BACKUP_REDUCER, OTHER
from qnet2.utils.timer import RepeaterSyncCallback
from .communication import ReductionScatterer, IndividualValueSender, IndividualValuesHandler
from .designator import ReducerDesignator
from qnet2.network.network import UDPServer
from reduction import Reducer


class ReducerContext:
    __SCATTER_GROUP_RESULT_TIMEOUT = 10
    SERVER_PORT = 4202

    def __init__(self, local_reducer: Reducer, scatterer: ReductionScatterer,
                 local_sender: IndividualValueSender, init_state: State):
        self.local_reducer = local_reducer
        self.__scatterer = scatterer
        self.local_sender = local_sender
        self.designator: tp.Optional[ReducerDesignator] = None

        self.__state: tp.Optional[State] = None

        self.transition_to(init_state)
        self.__scatter_repeater = RepeaterSyncCallback(self.__SCATTER_GROUP_RESULT_TIMEOUT, self.handle_scatter_timeout)

        self.__transport: tp.Optional[asyncio.BaseTransport] = None
        self.__run_server_task = asyncio.create_task(self.__run_server())

    async def __run_server(self) -> None:
        loop = asyncio.get_event_loop()
        self.__transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPServer(IndividualValuesHandler(self)),
            local_addr=('0.0.0.0', self.SERVER_PORT))

    def transition_to(self, new_state: State) -> None:
        self.__state = new_state
        self.__state.context = self

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.__state is not None:
            self.__state.handle_individual_values(individual_values)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if self.__state is not None:
            self.__state.handle_state_transition(to_state)

    def handle_scatter_timeout(self) -> None:
        if self.__state is not None:
            self.__state.handle_scatter_timeout()

    def scatter_reduction_result(self) -> None:
        self.__scatterer.scatter_group_result(self.local_reducer.get_reduction_result())


class State:
    def __init__(self) -> None:
        self.context: tp.Optional[ReducerContext] = None

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        pass

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        pass

    def handle_scatter_timeout(self) -> None:
        pass


class ReducerState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.context is not None:
            self.context.local_reducer.add_individual_values(individual_values)

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

    def handle_scatter_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.local_reducer.clear_reduction_result()


class PreBackupState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.context is not None:
            self.context.local_reducer.add_individual_values(individual_values)

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

    def handle_scatter_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.local_reducer.clear_reduction_result()
            self.context.transition_to(BackupState())


class TempReducerState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.context is not None:
            self.context.local_reducer.add_individual_values(individual_values)

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

    def handle_scatter_timeout(self) -> None:
        if self.context is not None:
            self.context.scatter_reduction_result()
            self.context.local_reducer.clear_reduction_result()
            self.context.transition_to(OtherState())


class BackupState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.context is not None:
            self.context.local_reducer.add_individual_values(individual_values)

    def handle_state_transition(self, to_state: NeighbourState.V) -> None:
        if to_state == BACKUP_REDUCER or self.context is None:
            return
        elif to_state == REDUCER:
            self.context.transition_to(ReducerState())
        elif to_state == OTHER:
            self.context.local_reducer.clear_reduction_result()
            self.context.transition_to(OtherState())
        else:
            raise NotImplementedError(
                'Unexpected NeighbourState: {}'.format(reduction_pb2.NeighbourState.Name(to_state))
            )

    def handle_scatter_timeout(self) -> None:
        if self.context is not None:
            self.context.local_reducer.clear_reduction_result()


class OtherState(State):
    def __init__(self) -> None:
        super().__init__()

    def handle_individual_values(self, individual_values: IndividualValues) -> None:
        if self.context is None:
            return
        if self.context.designator is None or self.context.designator.get_reducer() < 0 or individual_values.ttl <= 0:
            self.context.transition_to(TempReducerState())
            self.context.handle_individual_values(individual_values)
        else:
            individual_values.ttl -= 1
            self.context.local_sender.send_individual_values(individual_values, self.context.designator.get_reducer())

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
