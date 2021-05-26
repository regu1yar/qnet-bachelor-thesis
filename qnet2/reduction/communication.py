from .reduction_pb2 import ReductionResult


class ReductionScatterer:
    def scatter_group_result(self, reduction_result: ReductionResult) -> None:
        pass


class LocalValueSender:
    def send_local_result(self, result: ReductionResult, dest_node: int) -> None:
        pass
