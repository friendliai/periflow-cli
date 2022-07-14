# Copyright (C) 2022 FriendliAI

from typing import List

from pfcli.service.client.base import ClientService, T, safe_request
from pfcli.utils import paginated_get


class BillingClientService(ClientService):

    def list_prices(self,
                    year: int,
                    month: int,
                    day: int = None,
                    group_id: T = None,
                    project_id: T = None) -> List[dict]:
        params = {
            "group_id": str(group_id) if group_id else None,
            "project_id": str(project_id) if project_id else None,
            "year": year,
            "month": month,
            "day": day
        }
        get_response_dict = safe_request(self.list, err_prefix=f"Failed to get billing summary.")
        return paginated_get(
            get_response_dict,
            **params
        )
