# Copyright (C) 2021 FriendliAI

"""PeriFlow VM CLI"""

from typing import Optional, List, Dict

import tabulate
import typer

from pfcli.service import CloudType, ServiceType
from pfcli.service.client import GroupVMQuotaClientService, build_client

app = typer.Typer()


def _print_vms(vm_dict_list: List[Dict]):
    headers = ["vm", "cloud", "region", "device_type", "quota"]
    results = [
        [
            d['vm_instance_type']['code'],
            d['vm_instance_type']['vendor'],
            d['vm_instance_type']['region'],
            d['vm_instance_type']['device_type'],
            d['quota']
        ] for d in vm_dict_list
    ]

    typer.echo(tabulate.tabulate(results, headers=headers))


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
    client: GroupVMQuotaClientService = build_client(ServiceType.GROUP_VM_QUOTA)
    vm_dict_list = client.list_vm_quotas(vendor=cloud, device_type=device_type)
    _print_vms(vm_dict_list)
