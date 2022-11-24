# Copyright (C) 2022 FriendliAI

"""PeriFlow DeploymentClient Service"""

from typing import List

from pfcli.service.client.base import ClientService, safe_request


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

    def list_deployments(self) -> dict:
        response = safe_request(self.list, err_prefix="Failed to list deployments.")()
        return response.json()

    def delete_deployment(self, deployment_id: str) -> None:
        safe_request(self.delete, err_prefix="Failed to delete deployment.")(
            pk=deployment_id
        )
