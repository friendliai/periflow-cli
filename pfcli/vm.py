# Copyright (C) 2021 FriendliAI

"""PeriFlow VM CLI"""

from typing import Optional

import typer

from pfcli.service import CloudType, ServiceType
from pfcli.service.client import GroupVMQuotaClientService, build_client
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
        'vm_instance_type.code',
        'vm_instance_type.vendor',
        'vm_instance_type.region',
        'vm_instance_type.device_type',
        'quota'
    ],
    headers=['VM', 'Cloud', 'Region', 'Device', 'Quota']
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

    client: GroupVMQuotaClientService = build_client(ServiceType.GROUP_VM_QUOTA)
    vm_dict_list = client.list_vm_quotas(vendor=cloud, region=region, device_type=device_type)

    formatter.render(vm_dict_list)
