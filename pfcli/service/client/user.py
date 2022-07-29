# Copyright (C) 2022 FriendliAI

"""PeriFlow UserClient Service"""

from string import Template
from typing import List

from pfcli.service import GroupRole, ProjectRole
from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    UserRequestMixin,
    safe_request,
)
from pfcli.utils import paginated_get


class UserSignUpService(ClientService):
    def sign_up(self, username: str, name: str, email: str, password: str) -> None:
        safe_request(self.bare_post, err_prefix="Failed to signup")(
            json={"username": username, "name": name, "email": email, "password": password}
        )

    def verify(self, token: str) -> None:
        safe_request(self.bare_post, err_prefix="Failed to verify")(
            path="confirm",
            json={"email_token": token}
        )


class UserClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, **kwargs)

    def change_password(self, old_password: str, new_password: str) -> None:
        safe_request(self.update, err_prefix="Failed to change password.")(
            pk=self.user_id,
            path="password",
            json={
                "old_password": old_password,
                "new_password": new_password
            }
        )

    def set_group_privilege(self, pf_group_id: str, pf_user_id: str, privilege_level: GroupRole) -> None:
        safe_request(self.partial_update, err_prefix="Failed to update privilege level in group")(
            pk=pf_user_id,
            path=f"pf_group/{pf_group_id}/privilege_level",
            json={
                "privilege_level": privilege_level.value
            }
        )

    def get_project_membership(self, pf_project_id: str) -> dict:
        response = safe_request(self.retrieve, err_prefix="Failed identify member in project")(
            pk=self.user_id,
            path=f"pf_project/{pf_project_id}",
        )
        return response.json()

    def add_to_project(self, pf_user_id: str, pf_project_id: str, access_level: ProjectRole) -> None:
        safe_request(self.post, err_prefix="Failed to add user to project")(
            path=f"{pf_user_id}/pf_project/{pf_project_id}",
            json={
                "access_level": access_level.value
            }
        )

    def set_project_privilege(self, pf_user_id: str, pf_project_id: str, access_level: ProjectRole) -> None:
        safe_request(self.partial_update, err_prefix="Failed to update privilege level in project")(
            pk=pf_user_id,
            path=f"pf_project/{pf_project_id}/access_level",
            json={
                "access_level": access_level.value
            }
        )


class UserGroupClientService(ClientService, UserRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        super().__init__(template, pf_user_id=self.user_id, **kwargs)

    def get_group_info(self) -> list:
        response = safe_request(self.list, err_prefix="Failed to get my group info.")()
        return response.json()


class UserGroupProjectClientService(ClientService, UserRequestMixin, GroupRequestMixin):
    def __init__(self, template: Template, **kwargs):
        self.initialize_user()
        self.initialize_group()
        super().__init__(template, pf_user_id=self.user_id, pf_group_id=self.group_id, **kwargs)

    def list_projects(self) -> List[dict]:
        get_response_dict = safe_request(self.list, err_prefix="Failed to list projects.")
        return paginated_get(get_response_dict)
