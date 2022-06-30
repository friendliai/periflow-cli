# Copyright (C) 2022 FriendliAI

from typing import List

from pfcli.service.client.base import ClientService, T, safe_request


class BillingSummaryClientService(ClientService):

    def get_summary(self,
                    year: int,
                    month: int,
                    day: int = None,
                    group_id: T = None,
                    project_id: T = None) -> List[dict]:
        response = safe_request(self.list, err_prefix=f"Failed to get billing summary.")(
            params={
                "group_id": str(group_id) if group_id else None,
                "project_id": str(project_id) if project_id else None,
                "year": year,
                "month": month,
                "day": day
            }
        )
        return response.json()
