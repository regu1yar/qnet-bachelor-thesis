import asyncio

from google.protobuf.message import DecodeError

from .reduction_pb2 import ReductionResult, IndividualValues
from qnet2.network.scatter import Scatterer, MessageHandlerStrategy
from .reduction import GlobalReducer
from .state_machine import ReducerContext
from qnet2.network.network import DataHandler, send_data
from qnet2.config.net_config import Config


class ReductionScatterer:
    def scatter_group_result(self, reduction_result: ReductionResult) -> None:
        pass


class DefaultReductionMessageHandler(MessageHandlerStrategy):
    def __init__(self, global_reducer: GlobalReducer):
        self.__global_reducer = global_reducer

    def handle(self, data: bytes, source_node: int) -> None:
        group_reduction = ReductionResult()
        try:
            group_reduction.ParseFromString(data)
            self.__global_reducer.apply_group_reduction(group_reduction)
        except DecodeError:
            print('Can\'t parse ReductionResult from data:', data)


class DefaultReductionScatterer(ReductionScatterer):
    SCATTER_TOPIC = 'default_reduction'

    def __init__(self, scatterer: Scatterer, global_reducer: GlobalReducer):
        self.__scatterer = scatterer
        self.__scatterer.set_handler_strategy(self.SCATTER_TOPIC, DefaultReductionMessageHandler(global_reducer))

    def scatter_group_result(self, reduction_result: ReductionResult) -> None:
        self.__scatterer.scatter_global(reduction_result.SerializeToString(), self.SCATTER_TOPIC)


class IndividualValueSender:
    def __init__(self, config: Config):
        self.__config = config

    def send_individual_values(self, values: IndividualValues, dest_node: int) -> None:
        asyncio.create_task(send_data(
            self.__config.get_ip_addr_by_node_id(dest_node),
            ReducerContext.SERVER_PORT,
            values.SerializeToString(),
        ))


class IndividualValuesHandler(DataHandler):
    def __init__(self, context: ReducerContext):
        super().__init__()
        self.__context = context

    def handle(self, data: bytes) -> None:
        individual_values = IndividualValues()
        try:
            individual_values.ParseFromString(data)
            self.__context.handle_individual_values(individual_values)
        except DecodeError:
            print('Can\'t parse IndividualValues from data:', data)
