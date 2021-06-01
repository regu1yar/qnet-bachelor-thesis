from .reduction_pb2 import ReductionValues


class GlobalReductionResult:
    def __init__(self, values: ReductionValues, fullness_rate: float, overlay_rate: float):
        self.values = values
        self.fullness__rate = fullness_rate
        self.overlay_rate = overlay_rate


class ReductionConsumer:
    def consume_global_reduction(self, result: GlobalReductionResult) -> None:
        pass
