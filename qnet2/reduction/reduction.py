from bitarray import bitarray

from .reduction_pb2 import ReductionResult, IndividualValues, ReductionValues
from qnet2.config.net_config import Config
from .communication import IndividualValueSender
from .producer import ValuesProducer
from qnet2.utils.timer import RepeaterSyncCallback
from .designator import ReducerDesignator
from .consumer import ReductionConsumer
from .reduction_strategy import ReductionStrategy


class Reducer:
    def __init__(self, config: Config, reduction_strategy: ReductionStrategy):
        self.__max_node_id = self.__calc_max_node_id(config)
        self.__reduction_strategy = reduction_strategy
        self.__cur_reduction_result = self.__reduction_strategy.generate_neutral()
        self.__cur_reduction_set = bitarray(self.__max_node_id)
        self.__cur_reduction_set.setall(0)

    @staticmethod
    def __calc_max_node_id(config: Config) -> int:
        max_node_id = 0
        for group in config.get_groups_to_nodes_dict().values():
            max_node_id = max(max_node_id, max(group))

        return max_node_id

    def add_individual_values(self, individual_values: IndividualValues) -> None:
        if self.__cur_reduction_set[individual_values.node_id] != 1:
            self.__cur_reduction_set[individual_values.node_id] = 1
            self.__cur_reduction_result = self.__reduction_strategy.reduce(
                self.__cur_reduction_result,
                individual_values.values
            )

    def reduce_with(self, other: ReductionResult) -> None:
        other_reduction_set = bitarray()
        other_reduction_set.frombytes(other.reduction_set_mask)
        if not (self.__cur_reduction_set & other_reduction_set).any():
            self.__cur_reduction_set |= other_reduction_set
            self.__cur_reduction_result = self.__reduction_strategy.reduce(
                self.__cur_reduction_result,
                other.values
            )

    def clear_reduction_result(self) -> None:
        self.__cur_reduction_set = bitarray(self.__max_node_id)
        self.__cur_reduction_set.setall(0)
        self.__cur_reduction_result = self.__reduction_strategy.generate_neutral()

    def get_reduction_result(self) -> ReductionResult:
        return ReductionResult(
            reduction_set_mask=self.__cur_reduction_set.tobytes(),
            values=self.__cur_reduction_result,
        )

    def get_reduction_values(self) -> ReductionValues:
        return self.__cur_reduction_result


class GlobalReducer:
    __FLUSH_TIMEOUT = 10

    def __init__(self, consumer: ReductionConsumer, reducer: Reducer):
        self.__consumer = consumer
        self.__reducer = reducer

        self.__flush_repeater = RepeaterSyncCallback(self.__FLUSH_TIMEOUT, self.__flush_result)

    def apply_group_reduction(self, group_reduction: ReductionResult) -> None:
        self.__reducer.reduce_with(group_reduction)

    def __flush_result(self) -> None:
        self.__consumer.consume_global_reduction(self.__reducer.get_reduction_values())
        self.__reducer.clear_reduction_result()


class ReductionInitializer:
    __SEND_VALUES_TIMEOUT = 10

    def __init__(self, config: Config, values_producer: ValuesProducer,
                 sender: IndividualValueSender, designator: ReducerDesignator):
        self.__id = config.get_local_node_id()
        self.__values_producer = values_producer
        self.__sender = sender
        self.__designator = designator
        self.__init_ttl_value = self.__calc_ttl_init_value(config)
        self.__send_repeater = RepeaterSyncCallback(self.__SEND_VALUES_TIMEOUT, self.__send_individual_values)

    @staticmethod
    def __calc_ttl_init_value(config: Config) -> int:
        return len(config.get_groups_to_nodes_dict()[config.get_local_group_id()])

    def __send_individual_values(self) -> None:
        individual_values = IndividualValues(
            node_id=self.__id,
            values=self.__values_producer.fetch_values(),
            ttl=self.__init_ttl_value,
        )

        reducer = self.__designator.get_reducer()
        if reducer >= 0:
            self.__sender.send_individual_values(individual_values, reducer)
        else:
            self.__sender.send_individual_values(individual_values, self.__id)

        backup = self.__designator.get_backup()
        if backup >= 0:
            self.__sender.send_individual_values(individual_values, backup)
