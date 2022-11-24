# Copyright (C) 2022 FriendliAI

"""Context (Organization / Project) managing"""

import uuid
from typing import Optional

from pfcli.utils.fs import get_periflow_directory


org_context_path = get_periflow_directory() / "organization"
project_context_path = get_periflow_directory() / "project"


def get_current_group_id() -> Optional[uuid.UUID]:
    if not org_context_path.exists():
        return None

    with open(org_context_path, "r", encoding="utf-8") as f:
        group_id = uuid.UUID(f.read())
        return group_id


def set_current_group_id(pf_group_id: uuid.UUID):
    with open(org_context_path, "w", encoding="utf-8") as f:
        f.write(str(pf_group_id))


def get_current_project_id() -> Optional[uuid.UUID]:
    if not project_context_path.exists():
        return None

    with open(project_context_path, "r", encoding="utf-8") as f:
        project_id = uuid.UUID(f.read())
        return project_id


def set_current_project_id(pf_project_id: uuid.UUID):
    with open(project_context_path, "w", encoding="utf-8") as f:
        f.write(str(pf_project_id))
