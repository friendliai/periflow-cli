# Copyright (C) 2022 FriendliAI

"""PeriFlow VMClient Service"""


from typing import List

from pfcli.service import LockType
from pfcli.service.client.base import ClientService, T, safe_request


class VMConfigClientService(ClientService):
    def list_vm_locks(self, vm_config_id: T, lock_type: LockType) -> List[dict]:
        response = safe_request(self.list, prefix="Failed to inspect locked VMs.")(
            path=f"{vm_config_id}/vm_lock/",
            params={"lock_type": lock_type}
        )
        return response.json()

    def get_active_vm_count(self, vm_config_id: T) -> int:
        vm_locks = self.list_vm_locks(vm_config_id, LockType.ACTIVE)
        return len(vm_locks)
