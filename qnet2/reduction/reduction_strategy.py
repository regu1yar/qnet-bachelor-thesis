import typing as tp

from .reduction_pb2 import HashMapReductionValues, ArrayReductionValues


class ReductionStrategy:
    def reduce(self, v1: tp.Union[HashMapReductionValues, ArrayReductionValues],
               v2: tp.Union[HashMapReductionValues, ArrayReductionValues]) -> \
            tp.Union[HashMapReductionValues, ArrayReductionValues]:
        pass

    def generate_neutral(self) -> tp.Union[HashMapReductionValues, ArrayReductionValues]:
        pass
