# Copyright (C) 2021 FriendliAI

"""PeriFlow VM CLI"""

from typing import Optional

import typer

from pfcli.service import CloudType, ServiceType
from pfcli.service.client import (
    GroupVMConfigClientService,
    ProjectVMQuotaClientService,
    build_client
)
from pfcli.service.client.project import ProjectVMConfigClientService
from pfcli.service.formatter import TableFormatter
from pfcli.utils import validate_cloud_region


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)

formatter = TableFormatter(
    name="VM Instances",
    fields=[
        'vm_config_type.code',
        'vm_config_type.vm_instance_type.vendor',
        'vm_config_type.vm_instance_type.region',
        'vm_config_type.vm_instance_type.device_type',
        'quota'
    ],
    headers=['VM', 'Cloud', 'Region', 'Device', 'Quota (Available / Total)']
)


@app.command("list")
def list(
    cloud: Optional[CloudType] = typer.Option(
        None,
        '--cloud',
        '-c',
        help="Filter list by cloud vendor."
    ),
    region: Optional[str] = typer.Option(
        None,
        '--region',
        '-r',
        help="Filter list by region."
    ),
    device_type: Optional[str] = typer.Option(
        None,
        '--device-type',
        '-d',
        help="Filter list by device type."
    )
):
    """List all VM quota information.
    """
    if cloud is not None and region is not None:
        validate_cloud_region(cloud, region)

    vm_quota_client: ProjectVMQuotaClientService = build_client(ServiceType.PROJECT_VM_QUOTA)
    vm_config_client: ProjectVMConfigClientService = build_client(ServiceType.PROJECT_VM_CONFIG)
    group_vm_config_client: GroupVMConfigClientService = build_client(ServiceType.GROUP_VM_CONFIG)

    vm_dict_list = vm_quota_client.list_vm_quotas(vendor=cloud, region=region, device_type=device_type)
    vm_id_map = group_vm_config_client.get_vm_config_id_map()
    for vm_dict in vm_dict_list:
        vm_instance_name = vm_dict['vm_config_type']['code']
        active_vm_count = vm_config_client.get_active_vm_count(vm_id_map[vm_instance_name])
        vm_dict['quota'] = f"{vm_dict['quota'] - active_vm_count} / {vm_dict['quota']}"

    formatter.render(vm_dict_list)
