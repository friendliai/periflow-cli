# Copyright (C) 2022 FriendliAI

"""PeriFlow ExperimentClient Service"""


from typing import List

from pfcli.service.client.base import ClientService, T, safe_request


class ExperimentClientService(ClientService):
    def list_jobs_in_experiment(self, experiment_id: T) -> List[dict]:
        response = safe_request(self.list, prefix="Failed to fetch jobs in the experiment.")(
            path=f"{experiment_id}/job/"
        )
        return response.json()['results']

    def delete_experiment(self, experiment_id: T) -> None:
        safe_request(self.delete, prefix="Failed to delete experiment.")(
            pk=experiment_id
        )

    def update_experiment_name(self, experiment_id: T, name: str) -> dict:
        response = safe_request(self.partial_update, prefix=f"Failed to update the name of experiment to {name}")(
            pk=experiment_id,
            json={"name": name}
        )
        return response.json()
