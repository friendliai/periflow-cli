# Copyright (C) 2022 FriendliAI

"""PeriFlow CredentialClient Service"""


from typing import Optional
from uuid import UUID

from pfcli.service import CredType, cred_type_map
from pfcli.service.client.base import ClientService, safe_request


class CredentialClientService(ClientService[UUID]):
    def get_credential(self, credential_id: UUID) -> dict:
        response = safe_request(self.retrieve, err_prefix="Credential is not found.")(
            pk=credential_id
        )
        return response.json()

    def update_credential(
        self,
        credential_id: UUID,
        *,
        name: Optional[str] = None,
        type_version: Optional[str] = None,
        value: Optional[dict] = None
    ) -> dict:
        request_data = {}
        if name is not None:
            request_data["name"] = name
        if type_version is not None:
            request_data["type_version"] = type_version
        if value is not None:
            request_data["value"] = value
        response = safe_request(
            self.partial_update, err_prefix="Failed to updated credential"
        )(pk=credential_id, json=request_data)
        return response.json()

    def delete_credential(self, credential_id: UUID) -> None:
        safe_request(self.delete, err_prefix="Failed to delete credential")(
            pk=credential_id
        )


class CredentialTypeClientService(ClientService):
    def get_schema_by_type(self, cred_type: CredType) -> Optional[dict]:
        type_name = cred_type_map[cred_type]
        response = safe_request(
            self.list, err_prefix="Failed to get credential schema."
        )()
        for cred_type_json in response.json():
            if cred_type_json["type_name"] == type_name:
                return cred_type_json["versions"][-1][
                    "schema"
                ]  # use the latest version
        return None
