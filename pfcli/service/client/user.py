# Copyright (C) 2022 FriendliAI

"""PeriFlow UserClient Service"""

from string import Template
from typing import List
from uuid import UUID

from pfcli.service import GroupRole, ProjectRole
from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    UserRequestMixin,
    safe_request,
)
from pfcli.utils.request import paginated_get


class UserMFAService(ClientService):
    def initiate_mfa(self, mfa_type: str, mfa_token: str) -> None:
        safe_request(self.bare_post, err_prefix="Failed to verify MFA token.")(
            path=f"challenge/{mfa_type}", headers={"x-mfa-token": mfa_token}
        )


class UserSignUpService(ClientService):
    def sign_up(self, username: str, name: str, email: str, password: str) -> None:
        safe_request(self.bare_post, err_prefix="Failed to signup")(
            json={
                "username": username,
                "name": name,
                "email": email,
                "password": password,
            }
        )

    def verify(self, token: str, key: str) -> None:
        safe_request(self.bare_post, err_prefix="Failed to verify")(
            path="confirm", json={"email_token": token, "key": key}
        )


class UserClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, **kwargs)

    def change_password(self, old_password: str, new_password: str) -> None:
        safe_request(self.update, err_prefix="Failed to change password.")(
            pk=self.user_id,
            path="password",
            json={"old_password": old_password, "new_password": new_password},
        )

    def set_group_privilege(
        self, pf_group_id: UUID, pf_user_id: UUID, privilege_level: GroupRole
    ) -> None:
        safe_request(
            self.partial_update, err_prefix="Failed to update privilege level in group"
        )(
            pk=pf_user_id,
            path=f"pf_group/{pf_group_id}/privilege_level",
            json={"privilege_level": privilege_level.value},
        )

    def get_project_membership(self, pf_project_id: UUID) -> dict:
        response = safe_request(
            self.retrieve, err_prefix="Failed identify member in project"
        )(
            pk=self.user_id,
            path=f"pf_project/{pf_project_id}",
        )
        return response.json()

    def add_to_project(
        self, pf_user_id: UUID, pf_project_id: UUID, access_level: ProjectRole
    ) -> None:
        safe_request(self.post, err_prefix="Failed to add user to project")(
            path=f"{pf_user_id}/pf_project/{pf_project_id}",
            json={"access_level": access_level.value},
        )

    def delete_from_project(
        self, pf_user_id: UUID, pf_project_id: UUID,
    ) -> None:
        safe_request(self.delete, err_prefix="Failed to remove user from proejct")(
            pk=pf_user_id,
            path=f"pf_project/{pf_project_id}",
        )

    def set_project_privilege(
        self, pf_user_id: UUID, pf_project_id: UUID, access_level: ProjectRole
    ) -> None:
        safe_request(
            self.partial_update,
            err_prefix="Failed to update privilege level in project",
        )(
            pk=pf_user_id,
            path=f"pf_project/{pf_project_id}/access_level",
            json={"access_level": access_level.value},
        )


class UserGroupClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, pf_user_id=self.user_id, **kwargs)

    def get_group_info(self) -> dict:
        response = safe_request(self.list, err_prefix="Failed to get my group info.")()
        return response.json()[0]


class UserGroupProjectClientService(ClientService, UserRequestMixin, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        self.initialize_group()
        super().__init__(
            template, pf_user_id=self.user_id, pf_group_id=self.group_id, **kwargs
        )

    def list_projects(self) -> List[dict]:
        get_response_dict = safe_request(
            self.list, err_prefix="Failed to list projects."
        )
        return paginated_get(get_response_dict)
