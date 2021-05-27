from .reduction_pb2 import ReductionValues


class ReductionConsumer:
    def consume_global_reduction(self, result: ReductionValues) -> None:
        pass
