# Copyright (C) 2021 FriendliAI

"""PeriFlow VM CLI"""

from typing import Optional

import typer

from pfcli.service import CloudType, PeriFlowService, ServiceType
from pfcli.service.client import (
    GroupProjectClientService,
    GroupProjectVMQuotaClientService,
    PFTGroupVMConfigClientService,
    PFTProjectVMConfigClientService,
    PFTProjectVMQuotaClientService,
    build_client,
)
from pfcli.service.client.project import (
    PFTProjectVMConfigClientService,
    find_project_id,
)
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils.format import secho_error_and_exit


app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)
quota_app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

app.add_typer(quota_app, name="quota", help="Manage quota.")

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
    headers=[
        "VM",
        "Cloud",
        "GPU",
        "GPU COUNT",
        "Per GPU Memory [GiB]",
        "vCPU",
        "CPU Memory [GiB]",
        "Quota (Available / Total)",
        "Spot",
    ],
)
quota_detail_panel = PanelFormatter(
    name="VM Quota",
    fields=[
        "code",
        "in_use",
        "project_limit",
        "organization_limit",
    ],
    headers=[
        "VM",
        "In Use",
        "Project Limit",
        "Organization Limit",
    ],
)


def vm_list_for_train(cloud: Optional[CloudType], device_type: Optional[str]) -> None:
    """List all VM information for training."""
    vm_quota_client: PFTProjectVMQuotaClientService = build_client(
        ServiceType.PFT_PROJECT_VM_QUOTA
    )
    vm_config_client: PFTProjectVMConfigClientService = build_client(
        ServiceType.PFT_PROJECT_VM_CONFIG
    )
    group_vm_config_client: PFTGroupVMConfigClientService = build_client(
        ServiceType.PFT_GROUP_VM_CONFIG
    )

    vm_dict_list = vm_quota_client.list_vm_quotas(vendor=cloud, device_type=device_type)
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
        available_vm_dict_list, key=lambda d: d["vm_config_type"]["code"]
    )
    formatter.render(available_vm_dict_list)


def vm_list_for_serve(cloud: Optional[CloudType], device_type: Optional[str]) -> None:
    # TODO: FILL ME
    secho_error_and_exit(
        "VM list for the deployment is not supported yet. Please contact the support team."
    )


@app.command("list", help="list up available VMs")
def list(
    service: PeriFlowService = typer.Option(
        ...,
        "--service",
        "-s",
        help="PeriFlow service type. Training and serving services are provided with different VM types.",
    ),
    cloud: Optional[CloudType] = typer.Option(
        None, "--cloud", "-c", help="Filter list by cloud vendor."
    ),
    device_type: Optional[str] = typer.Option(
        None, "--device-type", "-d", help="Filter list by device type."
    ),
):
    handler_map = {
        PeriFlowService.TRAIN: vm_list_for_train,
        PeriFlowService.SERVE: vm_list_for_serve,
    }
    handler_map[service](cloud, device_type)


@quota_app.command("view", help="view quota detail of a VM")
def view(
    vm_instance_name: str = typer.Argument(..., help="vm type"),
):
    vm_quota_client: PFTProjectVMQuotaClientService = build_client(
        ServiceType.PFT_PROJECT_VM_QUOTA
    )
    vm_config_client: PFTProjectVMConfigClientService = build_client(
        ServiceType.PFT_PROJECT_VM_CONFIG
    )
    group_vm_config_client: PFTGroupVMConfigClientService = build_client(
        ServiceType.PFT_GROUP_VM_CONFIG
    )

    vm_dict_list = vm_quota_client.list_vm_quotas()
    vm_id_map = group_vm_config_client.get_vm_config_id_map()
    for vm_dict in vm_dict_list:
        if vm_instance_name == vm_dict["vm_config_type"]["code"]:
            vm_count_in_use = vm_config_client.get_vm_count_in_use(
                vm_id_map[vm_instance_name]
            )
            panel_dict = {
                "code": vm_instance_name,
                "in_use": vm_count_in_use,
                "project_limit": "-"
                if vm_dict["quota"] == vm_dict["global_quota"]
                else vm_dict["quota"],
                "organization_limit": vm_dict["global_quota"],
            }
            quota_detail_panel.render(panel_dict)
            return

    secho_error_and_exit(f"VM instance {vm_instance_name} does not exist")


@quota_app.command("create", help="Create quota limitation of the project")
def create(
    vm_instance_name: str = typer.Option(..., help="vm type"),
    project_name: str = typer.Option(
        ..., help="name of the project where quota will be created"
    ),
    quota: int = typer.Option(..., help="number of VM quota"),
):
    project_client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    project_id = find_project_id(project_client.list_projects(), project_name)

    vm_quota_client: GroupProjectVMQuotaClientService = build_client(
        ServiceType.GROUP_VM_QUOTA
    )

    vm_quota_client.create_project_quota(vm_instance_name, project_id, quota)
    typer.secho(
        f"VM quota of VM {vm_instance_name} in project {project_name} is set to {quota}!",
        fg=typer.colors.BLUE,
    )


@quota_app.command("update", help="Update quota limitation of the project")
def update(
    vm_instance_name: str = typer.Option(..., help="vm type"),
    project_name: str = typer.Option(
        ..., help="name of the project where quota will be created"
    ),
    quota: int = typer.Option(..., help="number of VM quota"),
):
    project_client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    project_id = find_project_id(project_client.list_projects(), project_name)

    vm_quota_client: GroupProjectVMQuotaClientService = build_client(
        ServiceType.GROUP_VM_QUOTA
    )
    quotas = vm_quota_client.list_quota(vm_instance_name, project_id)
    if not quotas:
        secho_error_and_exit(f"Cannot find project quota of vm {vm_instance_name}")

    # unique
    quota_id = quotas[0]["id"]
    vm_quota_client.update_project_quota(quota_id, quota)
    typer.secho(
        f"VM quota of VM {vm_instance_name} in project {project_name} is updated to {quota}!",
        fg=typer.colors.BLUE,
    )


@quota_app.command("delete", help="Delete quota limitation of the project")
def delete(
    vm_instance_name: str = typer.Option(..., help="vm type"),
    project_name: str = typer.Option(
        ..., help="name of the project where quota will be created"
    ),
):
    project_client: GroupProjectClientService = build_client(ServiceType.GROUP_PROJECT)
    project_id = find_project_id(project_client.list_projects(), project_name)
    vm_quota_client: GroupProjectVMQuotaClientService = build_client(
        ServiceType.GROUP_VM_QUOTA
    )
    quotas = vm_quota_client.list_quota(vm_instance_name, project_id)
    if not quotas:
        secho_error_and_exit(f"Cannot find project quota of vm {vm_instance_name}")

    # unique
    quota_id = quotas[0]["id"]
    vm_quota_client.delete_quota(quota_id)
    typer.secho(
        f"VM quota of VM {vm_instance_name} in project {project_name} is deleted!",
        fg=typer.colors.BLUE,
    )
