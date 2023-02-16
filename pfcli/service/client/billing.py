# Copyright (C) 2022 FriendliAI

from __future__ import annotations

from enum import Enum
from string import Template
from typing import Any, Dict, List, Optional

from pfcli.service.client.base import (
    ClientService,
    GroupRequestMixin,
    ProjectRequestMixin,
    UserRequestMixin,
    safe_request,
)


class TimeGranularity(str, Enum):
    hour = "hour"
    day = "day"
    week = "week"


class Scope(str, Enum):
    ORG = "organization"
    PRJ = "project"
    USR = "user"


class PFTBillingClientService(
    ClientService, GroupRequestMixin, ProjectRequestMixin, UserRequestMixin
):
    def __init__(self, template: Template, **kwargs):
        self.initialize_group()
        self.initialize_project()
        self.initialize_user()
        super().__init__(template, **kwargs)

    def list_prices(
        self,
        start_date: str,
        end_date: str,
        scope: Scope,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> List[Dict[str, Any]]:
        params = {
            "organization_id": str(self.group_id),
            "project_id": str(self.project_id),
            "user_id": str(self.user_id),
            "start_date": start_date,
            "end_date": end_date,
            "time_unit": time_granularity,
            "scope": scope.value,
        }
        return safe_request(self.list, err_prefix=f"Failed to get billing summary.")(
            params=params
        ).json()["results"]
