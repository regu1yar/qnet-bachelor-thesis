import typing as tp


class MetricService:
    def __init__(self) -> None:
        self.direct_metrics: tp.Dict[int, float] = {}

    async def update_metrics(self) -> None:
        pass

    def get_direct_metric(self, dest: int) -> float:
        pass
        # if dest not in self.direct_metrics:
        #     raise KeyError("No such destination id : {}".format(dest))
        #
        # raise NotImplementedError()

    def get_emergency_metric_delta(self) -> float:
        pass
