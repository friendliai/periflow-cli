# Copyright (C) 2022 FriendliAI

"""PeriFlow DeploymentClient Service"""

from typing import List
from string import Template

from pfcli.service.client.base import ClientService, safe_request, ProjectRequestMixin
 

class DeploymentClientService(ClientService[str]):
    def get_deployment(self, deployment_id: str) -> dict:
        response = safe_request(
            self.retrieve,
            err_prefix=f"Deployment ({deployment_id}) is not found. You may enter wrongID.",
        )(pk=deployment_id)
        return response.json()

    def create_deployment(self, config: dict) -> dict:
        response = safe_request(self.post, err_prefix="Failed to post new deployment.")(
            json=config
        )
        return response.json()

    def list_deployments(self, project_id: str) -> dict:
        response = safe_request(self.list, err_prefix="Failed to list deployments.")(
            params={"project_id": project_id}
        )
        return response.json()

    def delete_deployment(self, deployment_id: str) -> None:
        safe_request(self.delete, err_prefix="Failed to delete deployment.")(
            pk=deployment_id
        )

class DeploymentMetricsClientService(ClientService):
    def get_metrics(self, deployment_id: str, time_window: int) -> dict:
        response = safe_request(
            self.list,
            err_prefix=f"Deployment ({deployment_id}) is not found. You may enter wrongID.",
        )(data=str(time_window))
        return response.json()

class PFSProjectUsageClientService(ClientService[str], ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def get_deployment_usage(self) -> dict:
        response = safe_request(
            self.list,
            err_prefix=f"Deployment usages are not found in the project.",
        )()
        return response.json()
