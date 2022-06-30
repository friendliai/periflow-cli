# Copyright (C) 2022 FriendliAI

from typing import List

from pfcli.service.client.base import ClientService, T, safe_request


class BillingSummaryClientService(ClientService):

    def get_summary(self, group_id: T = None, project_id: T = None) -> List[dict]:
        response = safe_request(self.list, err_prefix=f"Failed to get billing summary.")(
            json={
                "group_id": str(group_id) if group_id else "",
                "project_id": project_id
            }
        )
        return response.json()
