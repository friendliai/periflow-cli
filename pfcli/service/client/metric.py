# Copyright (C) 2022 FriendliAI

"""PeriFlow MetricClient Service"""

import json
from typing import List

from pfcli.service.client.base import ClientService, safe_request


class MetricClientService(ClientService):
    """PeriFlow MetricClientService class"""

    def list_metrics(
        self,
        job_id: int,
    ) -> List[str]:
        """Show available metric names."""
        fields = f'task: "{job_id}", telemetry: null, start: null, until: null'
        query = "{ sampleQuery(fields: {" + fields + "}) {name} }"
        resp = safe_request(
            self.post, err_prefix=f"Failed to get metrics of ({job_id})."
        )(
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
        )
        data = resp.json()["data"]["sampleQuery"]
        names = set(sample.get("name", None) for sample in data)
        return [{"name": name} for name in names if name]

    def get_metric_values(
        self,
        job_id: int,
        metric_name: str,
        limit: int = 10,
    ) -> dict[str, str]:
        fields = (
            f'task: "{job_id}", telemetry: "{metric_name}", limit: {limit}, goal: LAST'
        )
        query = "{ representativeSamples(fields: {" + fields + "}) {created, value} }"
        resp = safe_request(
            self.post, err_prefix=f"Failed to get metrics of ({job_id})."
        )(
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
        )
        samples = resp.json()["data"]["representativeSamples"]

        metrics = []
        for sample in samples:
            created = sample.get("created", None)
            value_json = sample.get("value", None)
            value = json.loads(value_json).get("data", None)
            metrics.append({"name": metric_name, "created": created, "value": value})
        return metrics
