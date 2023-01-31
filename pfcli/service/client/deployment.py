# Copyright (C) 2022 FriendliAI

"""PeriFlow DeploymentClient Service"""

from typing import Any, Dict, List
from string import Template

from pfcli.service.client.base import ClientService, safe_request, ProjectRequestMixin


class DeploymentClientService(ClientService[str]):
    def get_deployment(self, deployment_id: str) -> Dict[str, Any]:
        response = safe_request(
            self.retrieve,
            err_prefix=f"Deployment ({deployment_id}) is not found. You may entered wrong ID.",
        )(pk=deployment_id)
        return response.json()

    def create_deployment(self, config: Dict[str, Any]) -> Dict[str, Any]:
        response = safe_request(self.post, err_prefix="Failed to post new deployment.")(
            json=config
        )
        return response.json()

    def scale_deployment(self, deployment_id: str, replicas: int) -> None:
        safe_request(
            self.partial_update,
            err_prefix=f"Deployment ({deployment_id}) cannot be scaled.",
        )(pk=deployment_id, path=f"scale", data=str(replicas))

    def list_deployments(self, project_id: str) -> Dict[str, Any]:
        response = safe_request(self.list, err_prefix="Failed to list deployments.")(
            params={"project_id": project_id}
        )
        return response.json()

    def delete_deployment(self, deployment_id: str) -> None:
        safe_request(self.delete, err_prefix="Failed to delete deployment.")(
            pk=deployment_id
        )


class DeploymentLogClientService(ClientService[str]):
    def get_deployment_log(
        self, deployment_id: str, replica_index: int
    ) -> Dict[str, Any]:
        response = safe_request(
            self.list,
            err_prefix=f"Log is not available for Deployment ({deployment_id})"
            f"with replica {replica_index}."
            "You may entered wrong ID or the replica is not running.",
        )(params={"replica_index": replica_index})
        return response.json()


class DeploymentMetricsClientService(ClientService):
    def get_metrics(self, deployment_id: str, time_window: int) -> Dict[str, Any]:
        response = safe_request(
            self.list,
            err_prefix=f"Deployment ({deployment_id}) is not found. You may entered wrong ID.",
        )(data=str(time_window))
        return response.json()


class PFSProjectUsageClientService(ClientService[str], ProjectRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_project()
        super().__init__(template, project_id=self.project_id, **kwargs)

    def get_deployment_usage(self) -> Dict[str, Any]:
        response = safe_request(
            self.list,
            err_prefix=f"Deployment usages are not found in the project.",
        )()
        return response.json()


class PFSVMClientService(ClientService):
    def list_vms(self) -> List[Dict[str, Any]]:
        response = safe_request(
            self.list,
            err_prefix="Cannot get available vm list from PFS server.",
        )()
        return response.json()
