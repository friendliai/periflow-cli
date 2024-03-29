# Copyright (C) 2022 FriendliAI

from __future__ import annotations

from string import Template
from typing import Dict, Tuple, Type, TypeVar

from pfcli.service import ServiceType
from pfcli.service.client.base import ClientService
from pfcli.service.client.billing import PFTBillingClientService
from pfcli.service.client.checkpoint import (
    CheckpointClientService,
    CheckpointFormClientService,
)
from pfcli.service.client.credential import (
    CredentialClientService,
    CredentialTypeClientService,
)
from pfcli.service.client.data import DataClientService
from pfcli.service.client.deployment import (
    DeploymentClientService,
    DeploymentEventClientService,
    DeploymentLogClientService,
    DeploymentMetricsClientService,
    DeploymentReqRespClientService,
    PFSProjectUsageClientService,
    PFSVMClientService,
)
from pfcli.service.client.file import FileClientService, GroupProjectFileClientService
from pfcli.service.client.group import (
    GroupClientService,
    GroupProjectCheckpointClientService,
    GroupProjectClientService,
    GroupProjectVMQuotaClientService,
    PFTGroupVMConfigClientService,
)
from pfcli.service.client.job import (
    JobTemplateClientService,
    JobWebSocketClientService,
    ProjectJobArtifactClientService,
    ProjectJobCheckpointClientService,
    ProjectJobClientService,
)
from pfcli.service.client.metrics import MetricsClientService
from pfcli.service.client.project import (
    PFTProjectVMConfigClientService,
    PFTProjectVMQuotaClientService,
    ProjectClientService,
    ProjectCredentialClientService,
    ProjectDataClientService,
    ProjectVMLockClientService,
)
from pfcli.service.client.user import (
    UserAccessKeyClientService,
    UserClientService,
    UserGroupClientService,
    UserGroupProjectClientService,
    UserMFAService,
    UserSignUpService,
)
from pfcli.utils.url import (
    get_auth_uri,
    get_meter_uri,
    get_mr_uri,
    get_observatory_uri,
    get_pfs_uri,
    get_uri,
    get_wss_uri,
)

client_template_map: Dict[ServiceType, Tuple[Type[ClientService], Template]] = {
    ServiceType.MFA: (
        UserMFAService,
        Template(get_auth_uri("mfa")),
    ),
    ServiceType.SIGNUP: (
        UserSignUpService,
        Template(get_auth_uri("pf_user/self_signup")),
    ),
    ServiceType.USER: (UserClientService, Template(get_auth_uri("pf_user"))),
    ServiceType.USER_GROUP: (
        UserGroupClientService,
        Template(get_auth_uri("pf_user/$pf_user_id/pf_group")),
    ),
    ServiceType.USER_ACCESS_KEY: (
        UserAccessKeyClientService,
        Template(get_auth_uri("pf_user")),
    ),
    ServiceType.USER_GROUP_PROJECT: (
        UserGroupProjectClientService,
        Template(get_auth_uri("pf_user/$pf_user_id/pf_group/$pf_group_id/pf_project")),
    ),  # pylint: disable=line-too-long
    ServiceType.PROJECT: (ProjectClientService, Template(get_auth_uri("pf_project"))),
    ServiceType.GROUP: (GroupClientService, Template(get_auth_uri("pf_group"))),
    ServiceType.GROUP_PROJECT: (
        GroupProjectClientService,
        Template(get_auth_uri("pf_group/$pf_group_id/pf_project")),
    ),
    ServiceType.PROJECT_JOB_CHECKPOINT: (
        ProjectJobCheckpointClientService,
        Template(get_uri("project/$project_id/job/$job_number/checkpoint/")),
    ),
    ServiceType.PROJECT_JOB_ARTIFACT: (
        ProjectJobArtifactClientService,
        Template(get_uri("project/$project_id/job/$job_number/artifact/")),
    ),
    ServiceType.PROJECT_JOB: (
        ProjectJobClientService,
        Template(get_uri("project/$project_id/job/")),
    ),
    ServiceType.JOB_TEMPLATE: (
        JobTemplateClientService,
        Template(get_uri("job_template/")),
    ),
    ServiceType.CREDENTIAL: (
        CredentialClientService,
        Template(get_auth_uri("credential")),
    ),
    ServiceType.PROJECT_CREDENTIAL: (
        ProjectCredentialClientService,
        Template(get_auth_uri("pf_project/$project_id/credential")),
    ),  # pylint: disable=line-too-long
    ServiceType.CREDENTIAL_TYPE: (
        CredentialTypeClientService,
        Template(get_uri("credential_type/")),
    ),
    ServiceType.DATA: (DataClientService, Template(get_uri("datastore/"))),
    ServiceType.PROJECT_DATA: (
        ProjectDataClientService,
        Template(get_uri("project/$project_id/datastore/")),
    ),
    ServiceType.GROUP_VM_QUOTA: (
        GroupProjectVMQuotaClientService,
        Template(get_uri("group/$group_id/vm_quota/")),
    ),
    ServiceType.PFT_PROJECT_VM_QUOTA: (
        PFTProjectVMQuotaClientService,
        Template(get_uri("project/$project_id/vm_quota/")),
    ),
    ServiceType.CHECKPOINT: (CheckpointClientService, Template(get_mr_uri("models/"))),
    ServiceType.CHECKPOINT_FORM: (
        CheckpointFormClientService,
        Template(get_mr_uri("model_forms/")),
    ),
    ServiceType.GROUP_PROJECT_CHECKPOINT: (
        GroupProjectCheckpointClientService,
        Template(get_mr_uri("orgs/$group_id/prjs/$project_id/models/")),
    ),  # pylint: disable=line-too-long
    ServiceType.PROJECT_VM_LOCK: (
        ProjectVMLockClientService,
        Template(get_uri("project/$project_id/vm_lock/")),
    ),
    ServiceType.PFT_PROJECT_VM_CONFIG: (
        PFTProjectVMConfigClientService,
        Template(get_uri("project/$project_id/vm_config/")),
    ),
    ServiceType.PFT_GROUP_VM_CONFIG: (
        PFTGroupVMConfigClientService,
        Template(get_uri("group/$group_id/vm_config/")),
    ),
    ServiceType.JOB_WS: (JobWebSocketClientService, Template(get_wss_uri("job/"))),
    ServiceType.DEPLOYMENT: (
        DeploymentClientService,
        Template(get_pfs_uri("deployment/")),
    ),
    ServiceType.DEPLOYMENT_LOG: (
        DeploymentLogClientService,
        Template(get_pfs_uri("deployment/$deployment_id/log/")),
    ),
    ServiceType.DEPLOYMENT_METRICS: (
        DeploymentMetricsClientService,
        Template(get_pfs_uri("deployment/$deployment_id/metrics/")),
    ),
    ServiceType.DEPLOYMENT_EVENT: (
        DeploymentEventClientService,
        Template(get_pfs_uri("deployment/$deployment_id/event/")),
    ),
    ServiceType.DEPLOYMENT_REQ_RESP: (
        DeploymentReqRespClientService,
        Template(get_pfs_uri("deployment/$deployment_id/req_resp/download/")),
    ),
    ServiceType.PFS_PROJECT_USAGE: (
        PFSProjectUsageClientService,
        Template(get_pfs_uri("usage/project/$project_id/duration")),
    ),
    ServiceType.PFS_VM: (
        PFSVMClientService,
        Template(get_pfs_uri("vm/")),
    ),
    ServiceType.PFT_BILLING_SUMMARY: (
        PFTBillingClientService,
        Template(get_meter_uri("training/instances/price/")),
    ),
    ServiceType.METRICS: (
        MetricsClientService,
        Template(get_observatory_uri("graphql")),
    ),
    ServiceType.FILE: (FileClientService, Template(get_mr_uri("files/"))),
    ServiceType.GROUP_FILE: (
        GroupProjectFileClientService,
        Template(get_mr_uri("orgs/$group_id/prjs/$project_id/files/")),
    ),
}


_ClientService = TypeVar("_ClientService", bound=ClientService)


def build_client(request_type: ServiceType, **kwargs) -> _ClientService:
    """Factory function to post client service.

    Args:
        request_type (RequestAPI):

    Returns:
        ClientService: created client service
    """
    cls, template = client_template_map[request_type]
    return cls(template, **kwargs)
