# Copyright (C) 2022 FriendliAI

from string import Template
from typing import Dict, Tuple, Type, TypeVar

from pfcli.service import ServiceType
from pfcli.service.client.base import ClientService
from pfcli.service.client.billing import BillingClientService
from pfcli.service.client.checkpoint import CheckpointClientService
from pfcli.service.client.credential import CredentialClientService, CredentialTypeClientService
from pfcli.service.client.data import DataClientService
from pfcli.service.client.experiment import ExperimentClientService
from pfcli.service.client.group import (
    GroupClientService,
    GroupProjectCheckpointClientService,
    GroupProjectClientService,
    GroupVMConfigClientService
)
from pfcli.service.client.job import (
    JobArtifactClientService,
    JobCheckpointClientService,
    JobClientService,
    JobTemplateClientService,
    JobWebSocketClientService
)
from pfcli.service.client.project import (
    ProjectClientService,
    ProjectCredentialClientService,
    ProjectDataClientService,
    ProjectExperimentClientService,
    ProjectJobClientService,
    ProjectVMConfigClientService,
    ProjectVMQuotaClientService
)
from pfcli.service.client.serve import ServeClientService
from pfcli.service.client.user import (
    UserClientService,
    UserGroupClientService,
    UserGroupProjectClientService,
    UserSignUpService
)
from pfcli.utils import get_auth_uri, get_meter_uri, get_mr_uri, get_pfs_uri, get_uri, get_wss_uri

client_template_map: Dict[ServiceType, Tuple[Type[ClientService], Template]] = {
    ServiceType.SIGNUP: (UserSignUpService, Template(get_auth_uri('pf_user/self_signup'))),
    ServiceType.USER: (UserClientService, Template(get_auth_uri('pf_user'))),
    ServiceType.USER_GROUP: (UserGroupClientService, Template(get_auth_uri('pf_user/$pf_user_id/pf_group'))),
    ServiceType.USER_GROUP_PROJECT: (UserGroupProjectClientService, Template(get_auth_uri('pf_user/$pf_user_id/pf_group/$pf_group_id/pf_project'))),  # pylint: disable=line-too-long
    ServiceType.PROJECT: (ProjectClientService, Template(get_auth_uri('pf_project'))),
    ServiceType.GROUP: (GroupClientService, Template(get_auth_uri('pf_group'))),
    ServiceType.GROUP_PROJECT: (GroupProjectClientService, Template(get_auth_uri('pf_group/$pf_group_id/pf_project'))),
    ServiceType.EXPERIMENT: (ExperimentClientService, Template(get_uri('experiment/'))),
    ServiceType.PROJECT_EXPERIMENT: (ProjectExperimentClientService, Template(get_uri('project/$project_id/experiment/'))),  # pylint: disable=line-too-long
    ServiceType.JOB: (JobClientService, Template(get_uri('job/'))),
    ServiceType.JOB_CHECKPOINT: (JobCheckpointClientService, Template(get_uri('job/$job_id/checkpoint/'))),
    ServiceType.JOB_ARTIFACT: (JobArtifactClientService, Template(get_uri('job/$job_id/artifact/'))),
    ServiceType.PROJECT_JOB: (ProjectJobClientService, Template(get_uri('project/$project_id/job/'))),
    ServiceType.JOB_TEMPLATE: (JobTemplateClientService, Template(get_uri('job_template/'))),
    ServiceType.CREDENTIAL: (CredentialClientService, Template(get_auth_uri('credential'))),
    ServiceType.PROJECT_CREDENTIAL: (ProjectCredentialClientService, Template(get_auth_uri('pf_project/$project_id/credential'))),  # pylint: disable=line-too-long
    ServiceType.CREDENTIAL_TYPE: (CredentialTypeClientService, Template(get_uri('credential_type/'))),
    ServiceType.DATA: (DataClientService, Template(get_uri('datastore/'))),
    ServiceType.PROJECT_DATA: (ProjectDataClientService, Template(get_uri('project/$project_id/datastore/'))),
    ServiceType.PROJECT_VM_QUOTA: (ProjectVMQuotaClientService, Template(get_uri('project/$project_id/vm_quota/'))),
    ServiceType.CHECKPOINT: (CheckpointClientService, Template(get_mr_uri('models/'))),
    ServiceType.GROUP_PROJECT_CHECKPOINT: (GroupProjectCheckpointClientService, Template(get_mr_uri('orgs/$group_id/prjs/$project_id/models/'))),  # pylint: disable=line-too-long
    ServiceType.PROJECT_VM_CONFIG: (ProjectVMConfigClientService, Template(get_uri('project/$project_id/vm_config/'))),
    ServiceType.GROUP_VM_CONFIG: (GroupVMConfigClientService, Template(get_uri('group/$group_id/vm_config/'))),
    ServiceType.JOB_WS: (JobWebSocketClientService, Template(get_wss_uri('job/'))),
    ServiceType.SERVE: (ServeClientService, Template(get_pfs_uri('deployment/'))),
    ServiceType.BILLING_SUMMARY: (BillingClientService, Template(get_meter_uri('training/instances/price/')))
}


_ClientService = TypeVar('_ClientService', bound=ClientService)


def build_client(request_type: ServiceType, **kwargs) -> _ClientService:
    """Factory function to post client service.

    Args:
        request_type (RequestAPI):

    Returns:
        ClientService: created client service
    """
    cls, template = client_template_map[request_type]
    return cls(template, **kwargs)
