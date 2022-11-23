# Copyright (C) 2022 FriendliAI

"""PeriFlow MetricsClient Service"""

import json
from datetime import datetime
from typing import List, TypeVar

from pfcli.service.client.base import ClientService, safe_request
from pfcli.utils.format import datetime_to_simple_string, utc_to_local

MetricsType = TypeVar("MetricsType")


class MetricsClientService(ClientService):
    """PeriFlow MetricsClientService class"""

    def list_metrics(
        self,
        job_id: int,
    ) -> List[dict[str, str]]:
        """Show available metrics."""
        fields = f'task: "{job_id}", telemetry: null, start: null, until: null'
        query = f"{{ sampleQuery(fields: {{ {fields} }}) {{name}} }}"
        resp = safe_request(
            self.post, err_prefix=f"Failed to get metrics of ({job_id})."
        )(
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
        )
        data = resp.json()["data"]["sampleQuery"]
        names = set(sample.get("name", None) for sample in data)
        return [{"name": name} for name in names if name]

    def get_metrics_values(
        self,
        job_id: int,
        name: str,
        limit: int = 10,
    ) -> List[dict[str, MetricsType]]:
        fields = f'task: "{job_id}", telemetry: "{name}", limit: {limit}, goal: LAST'
        output_type = "created, value, labels {key, value}"
        query = f"{{ representativeSamples(fields:{{ {fields} }}){{ {output_type} }} }}"
        resp = safe_request(
            self.post, err_prefix=f"Failed to get metrics of ({job_id})."
        )(
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
        )
        samples = resp.json()["data"]["representativeSamples"]

        metrics = []
        for sample in samples:
            created = utc_to_local(datetime.fromisoformat(sample.get("created", None)))

            value_json = sample.get("value", None)
            value = json.loads(value_json).get("data", None)
            if isinstance(value, float):
                value = "{:.3f}".format(value).rstrip("0").rstrip(".")

            labels = sample.get("labels", [])
            iteration = None
            for label in labels:
                if label["key"] == "iteration":
                    iteration = label["value"]

            metrics.append(
                {
                    "name": name,
                    "iteration": iteration,
                    "created": datetime_to_simple_string(created),
                    "value": value,
                }
            )
        return metrics
