class MetricService:
    def __init__(self):
        self.direct_metrics = {}

    async def update_metrics(self):
        pass

    def get_direct_metric(self, dest):
        if dest not in self.direct_metrics:
            raise KeyError("No such destination id : {}".format(dest))

    def get_emergency_metric_delta(self):
        pass
