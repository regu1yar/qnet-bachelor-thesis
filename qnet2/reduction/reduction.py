from bitarray import bitarray
from bitarray.util import serialize, deserialize
import typing as tp

from .reduction_pb2 import ReductionResult, IndividualValues, ReductionValues, GroupReductionResult
from qnet2.config.net_config import Config
from .communication import IndividualValueSender
from .producer import ValuesProducer
from qnet2.utils.timer import RepeaterSyncCallback, TimerSyncCallback
from .designator import ReducerDesignator
from .consumer import ReductionConsumer, GlobalReductionResult
from .reduction_strategy import ReductionStrategy


class Reducer:
    __MAX_OVERLAY_RATE = 0.5

    def __init__(self, config: Config, reduction_strategy: ReductionStrategy):
        self.__min_node_id, self.__max_node_id = self.__calc_min_max_node_id(config)
        self.__total_nodes = self.__max_node_id - self.__min_node_id + 1
        self.__reduction_strategy = reduction_strategy
        self.__cur_reduction_values: tp.Optional[ReductionValues] = None
        self.__cur_reduction_set = bitarray(self.__total_nodes)
        self.__cur_reduction_set.setall(0)
        self.__total_overlay = 0
        self.__total_reduced = 0

    @staticmethod
    def __calc_min_max_node_id(config: Config) -> tp.Tuple[int, int]:
        max_node_id = 0
        min_node_id: tp.Optional[int] = None
        for group in config.get_groups_to_nodes_dict().values():
            max_node_id = max(max_node_id, max(group))
            min_in_group = min(group)
            if min_node_id is None or min_node_id > min_in_group:
                min_node_id = min_in_group

        if min_node_id is None:
            min_node_id = max_node_id

        return min_node_id, max_node_id

    def add_individual_values(self, individual_values: IndividualValues) -> None:
        if self.__cur_reduction_values is None:
            self.__cur_reduction_values = self.__reduction_strategy.generate_neutral()

        if self.__cur_reduction_set[individual_values.node_id - self.__min_node_id] != 1:
            self.__cur_reduction_set[individual_values.node_id - self.__min_node_id] = 1
            self.__total_reduced += 1
            self.__cur_reduction_values = self.__reduction_strategy.reduce(
                self.__cur_reduction_values,
                individual_values.values
            )

    def reduce_with(self, other: ReductionResult) -> None:
        if self.__cur_reduction_values is None:
            self.__cur_reduction_values = \
                self.__reduction_strategy.generate_neutral()

        other_reduction_set = deserialize(other.reduction_set_mask)
        other_reduction_set_size = other_reduction_set.count(1)
        overlay = self.__cur_reduction_set & other_reduction_set
        overlay_size = overlay.count(1)
        if overlay_size <= other_reduction_set_size * self.__MAX_OVERLAY_RATE:
            self.__cur_reduction_set |= other_reduction_set
            self.__total_overlay += overlay_size
            self.__total_reduced += other_reduction_set_size
            self.__cur_reduction_values = self.__reduction_strategy.reduce(
                self.__cur_reduction_values,
                other.values
            )

    def clear_reduction_result(self) -> None:
        self.__cur_reduction_set = bitarray(self.__total_nodes)
        self.__cur_reduction_set.setall(0)
        self.__cur_reduction_values = None
        self.__total_reduced = 0
        self.__total_overlay = 0

    def get_reduction_result(self) -> ReductionResult:
        if self.__cur_reduction_values is None:
            return ReductionResult(
                reduction_set_mask=serialize(self.__cur_reduction_set),
                values=self.__reduction_strategy.generate_neutral(),
            )
        else:
            return ReductionResult(
                reduction_set_mask=serialize(self.__cur_reduction_set),
                values=self.__cur_reduction_values,
            )

    def get_reduction_values(self) -> ReductionValues:
        if self.__cur_reduction_values is None:
            return self.__reduction_strategy.generate_neutral()
        else:
            return self.__cur_reduction_values

    def get_global_reduction_result(self) -> GlobalReductionResult:
        if self.__cur_reduction_values is None:
            return GlobalReductionResult(
                self.__reduction_strategy.generate_neutral(),
                self.__total_reduced / self.__total_nodes,
                self.__total_overlay / self.__total_reduced,
            )
        else:
            return GlobalReductionResult(
                self.__cur_reduction_values,
                self.__total_reduced / self.__total_nodes,
                self.__total_overlay / self.__total_reduced,
            )

    def contains_all(self) -> bool:
        return self.__cur_reduction_set.all()


class ReducerFactory:
    def __init__(self, config: Config, reduction_strategy: ReductionStrategy):
        self.__config = config
        self.__reduction_strategy = reduction_strategy

    def create_reducer(self) -> Reducer:
        return Reducer(self.__config, self.__reduction_strategy)


class GlobalReducer:
    __FLUSH_TIMEOUT = 10
    __MAX_BUFFERED_REDUCTIONS = 3
    __WAIT_TIMEOUT = 5

    def __init__(self, consumer: ReductionConsumer, reducer_factory: ReducerFactory):
        assert self.__WAIT_TIMEOUT < self.__FLUSH_TIMEOUT
        self.__consumer = consumer
        self.__reducer_factory = reducer_factory
        self.__global_reducer = self.__reducer_factory.create_reducer()
        self.__waiting_reducer: tp.Optional[Reducer] = None
        self.__buffered_reductions: tp.List[ReductionResult] = []

        self.__flush_repeater = RepeaterSyncCallback(self.__FLUSH_TIMEOUT, self.__flush_result)
        self.__waiting_timer: tp.Optional[TimerSyncCallback] = None

    def apply_group_reduction(self, group_reduction: GroupReductionResult) -> None:
        if self.__waiting_reducer is not None:
            self.__waiting_reducer.reduce_with(group_reduction.reduction_result)
            if self.__waiting_reducer.contains_all():
                self.__flush_waiting_result()

        if not group_reduction.from_temporary_reducer or \
                len(self.__buffered_reductions) >= self.__MAX_BUFFERED_REDUCTIONS:
            self.__global_reducer.reduce_with(group_reduction.reduction_result)
        else:
            self.__buffered_reductions.append(group_reduction.reduction_result)

    def __apply_buffered(self, reducer: Reducer) -> None:
        for reduction_result in self.__buffered_reductions:
            reducer.reduce_with(reduction_result)

    def __flush_result(self) -> None:
        self.__apply_buffered(self.__global_reducer)
        del self.__buffered_reductions[:]
        if self.__global_reducer.contains_all():
            self.__consumer.consume_global_reduction(self.__global_reducer.get_global_reduction_result())
            self.__global_reducer.clear_reduction_result()
        else:
            if self.__waiting_reducer is not None:
                self.__flush_waiting_result()
            self.__waiting_reducer = self.__global_reducer
            self.__global_reducer = self.__reducer_factory.create_reducer()
            self.__waiting_timer = TimerSyncCallback(self.__WAIT_TIMEOUT, self.__flush_waiting_result)

    def __flush_waiting_result(self) -> None:
        if self.__waiting_timer is not None:
            self.__waiting_timer.cancel()
            self.__waiting_timer = None

        if self.__waiting_reducer is None:
            return

        self.__apply_buffered(self.__waiting_reducer)
        self.__consumer.consume_global_reduction(self.__waiting_reducer.get_global_reduction_result())
        self.__waiting_reducer = None


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
