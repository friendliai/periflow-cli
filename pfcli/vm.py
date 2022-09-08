# Copyright (C) 2021 FriendliAI

"""PeriFlow VM CLI"""

from typing import Optional

import typer

from pfcli.service import CloudType, ServiceType
from pfcli.service.client import (
    GroupVMConfigClientService,
    ProjectVMQuotaClientService,
    build_client,
)
from pfcli.service.client.project import ProjectVMConfigClientService
from pfcli.service.formatter import TableFormatter
from pfcli.utils import validate_cloud_region


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

formatter = TableFormatter(
    name="VM Instances",
    fields=[
        "vm_config_type.code",
        "vm_config_type.vendor",
        "vm_config_type.device_type",
        "vm_config_type.num_devices_per_vm",
        "vm_config_type.per_gpu_memory",
        "vm_config_type.vcpu",
        "vm_config_type.cpu_memory",
        "quota",
        "vm_config_type.is_spot",
    ],
    headers=["VM", "Cloud", "GPU", "GPU COUNT",
             "Per GPU Memory [GiB]", "vCPU", "CPU Memory [GiB]",
             "Quota (Available / Total)", "Spot"],
)


@app.command("list")
def list(
    cloud: Optional[CloudType] = typer.Option(
        None, "--cloud", "-c", help="Filter list by cloud vendor."
    ),
    device_type: Optional[str] = typer.Option(
        None, "--device-type", "-d", help="Filter list by device type."
    ),
):
    """List all VM quota information."""
    vm_quota_client: ProjectVMQuotaClientService = build_client(
        ServiceType.PROJECT_VM_QUOTA
    )
    vm_config_client: ProjectVMConfigClientService = build_client(
        ServiceType.PROJECT_VM_CONFIG
    )
    group_vm_config_client: GroupVMConfigClientService = build_client(
        ServiceType.GROUP_VM_CONFIG
    )

    vm_dict_list = vm_quota_client.list_vm_quotas(
        vendor=cloud, device_type=device_type
    )
    vm_id_map = group_vm_config_client.get_vm_config_id_map()
    available_vm_dict_list = []
    for vm_dict in vm_dict_list:
        vm_instance_name = vm_dict["vm_config_type"]["code"]
        try:
            vm_count_in_use = vm_config_client.get_vm_count_in_use(
                vm_id_map[vm_instance_name]
            )
            vm_dict[
                "quota"
            ] = f"{vm_dict['quota'] - vm_count_in_use} / {vm_dict['quota']}"
        except KeyError:
            continue
        available_vm_dict_list.append(vm_dict)

    available_vm_dict_list = sorted(
        available_vm_dict_list, key=lambda d: d['vm_config_type']['code']
    )

    formatter.render(available_vm_dict_list)
