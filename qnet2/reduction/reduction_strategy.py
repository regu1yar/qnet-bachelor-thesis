from .reduction_pb2 import ReductionValues


class ReductionStrategy:
    def reduce(self, v1: ReductionValues, v2: ReductionValues) -> ReductionValues:
        pass

    def generate_neutral(self) -> ReductionValues:
        pass
