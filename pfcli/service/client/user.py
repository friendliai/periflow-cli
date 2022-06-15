# Copyright (C) 2022 FriendliAI

"""PeriFlow UserClient Service"""

from string import Template
from typing import List

from pfcli.service.client.base import ClientService, GroupRequestMixin, UserRequestMixin, safe_request


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

        response_dict = get_response_dict().json()
        projects = response_dict['results']
        next_cursor = response_dict['next_cursor']
        while next_cursor is not None:
            response_dict = get_response_dict(
                params={"cursor": next_cursor}
            ).json()

            # TODO (taebum): delete
            if any(x in projects for x in response_dict['results']):
                break

            projects.extend(response_dict['results'])
            next_cursor = response_dict['next_cursor']

        return projects
