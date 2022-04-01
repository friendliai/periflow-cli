# Copyright (C) 2021 FriendliAI

"""PeriFlow YAML File Configuration Service"""

from dataclasses import dataclass
from typing import Tuple, TypeVar, Optional, Union, Any, List

import typer
from jsonschema import Draft7Validator, ValidationError

from pfcli.service import ServiceType, CredType, cred_type_map_inv
from pfcli.service.client import (
    CredentialClientService,
    CredentialTypeClientService,
    GroupCredentialClientService,
    JobTemplateClientService,
    build_client,
)
from pfcli.utils import secho_error_and_exit


DEFAULT_TEMPLATE_CONFIG = """\
# The name of experiment
experiment:

# The name of job
name:

# The name of vm type
vm:

# The number of GPU devices
num_devices:
"""

DATA_CONFIG = """
# Configure dataset
data:
  # The name of dataset
  name:
"""

DATA_MOUNT_CONFIG = """\
  # Path to mount your dataset volume
  mount_path:
"""


JOB_SETTING_CONFIG = """
# Configure your job!
job_setting:
"""

CUSTOM_JOB_SETTING_CONFIG = JOB_SETTING_CONFIG + """\
  type: custom

  # Docker config
  docker:
    # Docker image you want to use in the job
    image:
    # Bash shell command to run the job
    command:
"""

PRIVATE_DOCKER_IMG_CONFIG = """\
    credential_id:
"""

PREDEFINED_JOB_SETTING_CONFIG = JOB_SETTING_CONFIG + """\
  type: predefined
"""

DIST_CONFIG = """
# Distributed training config
dist:
  dp_degree:
  pp_degree:
  mp_degree:
"""

CHECKPOINT_CONFIG = """
# Checkpoint config
checkpoint:
"""

INPUT_CHECKPOINT_CONFIG = """\
  input:
    # UUID of input checkpoint
    id:
"""

INPUT_CHECKPOINT_MOUNT_PATH = """\
    # Input checkpoint mount path
    mount_path:
"""

OUTPUT_CHECKPOINT_CONFIG = """\
  # Path to output checkpoint
  output_checkpoint_dir:
"""

PLUGIN_CONFIG = """
# Additional plugin for job monitoring and push notification
plugin:
"""

WANDB_PLUGIN_CONFIG = """\
  wandb:
    # W&B API key
    credential_id:
"""

SLACK_PLUGIN_CONFIG = """\
  slack:
    credential_id:
    channel:
"""


J = TypeVar('J', bound='JobConfigService')
D = TypeVar('D', bound='DataConfigService')
T = TypeVar('T', bound=Union[str, Tuple[Any]])


class InteractiveConfigMixin:
    def start_interaction(self) -> None:
        raise NotImplementedError   # prama: no cover

    def render(self) -> T:
        raise NotImplementedError   # pragma: no cover


@dataclass
class JobConfigService(InteractiveConfigMixin):
    """Interface of job template configuration service
    """
    ready: bool = False
    use_data: bool = False
    use_input_checkpoint: bool = False
    use_wandb: bool = False
    use_slack: bool = False

    def _render_plugins(self) -> str:
        yaml_str = ""
        if any((self.use_wandb, self.use_slack)):
            yaml_str += PLUGIN_CONFIG
            if self.use_wandb:
                yaml_str += WANDB_PLUGIN_CONFIG
            if self.use_slack:
                yaml_str += SLACK_PLUGIN_CONFIG

        return yaml_str


@dataclass
class CustomJobConfigService(JobConfigService):
    """Custom job template configuration service
    """
    # TODO: Support workspace, artifact
    use_private_image: bool = False
    use_dist: bool = False
    use_output_checkpoint: bool = False

    def start_interaction(self):
        self.use_private_img = typer.confirm(
            "Will you use your private docker image? (You should provide credential).", prompt_suffix="\n>>"
        )
        self.use_dist = typer.confirm(
            "Will you run distributed training job?", prompt_suffix="\n>>"
        )
        self.use_data = typer.confirm(
            "Will you use dataset for the job?", prompt_suffix="\n>>"
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use input checkpoint for the job?", prompt_suffix="\n>>"
        )
        self.use_output_checkpoint = typer.confirm(
            "Does your job generate model checkpoint file?", prompt_suffix="\n>>"
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?", prompt_suffix="\n>>"
        )
        self.use_slack = typer.confirm(
            "Do you want to get slack notifaction for the job?", prompt_suffix="\n>>"
        )
        self.ready = True

    def render(self) -> str:
        assert self.ready

        yaml_str = DEFAULT_TEMPLATE_CONFIG
        if self.use_private_image:
            yaml_str += PRIVATE_DOCKER_IMG_CONFIG
        if self.use_dist:
            yaml_str += DIST_CONFIG
        if self.use_data:
            yaml_str += DATA_CONFIG
            yaml_str += DATA_MOUNT_CONFIG
        if any((self.use_input_checkpoint, self.use_output_checkpoint)):
            yaml_str += CHECKPOINT_CONFIG
            if self.use_input_checkpoint:
                yaml_str += INPUT_CHECKPOINT_CONFIG
                yaml_str += INPUT_CHECKPOINT_MOUNT_PATH
            if self.use_output_checkpoint:
                yaml_str += OUTPUT_CHECKPOINT_CONFIG
        yaml_str += self._render_plugins()

        return yaml_str


@dataclass
class PredefinedJobConfigService(JobConfigService):
    """Predefined job template configuration service
    """
    model_name: Optional[str] = None
    model_config: Optional[dict] = None

    def start_interaction(self) -> None:
        job_template_client_service: JobTemplateClientService = build_client(ServiceType.JOB_TEMPLATE)

        template_names = job_template_client_service.list_job_template_names()
        self.model_name = typer.prompt(
            "Which job do you want to run? Choose one in the following catalog:\n",
            f"Options: {', '.join(template_names)}"
            , prompt_suffix="\n>>"
        )
        if self.model_name not in template_names:
            secho_error_and_exit("Invalid job template name...!")
        template = job_template_client_service.get_job_template_by_name(self.model_name)
        assert template is not None
        self.model_config = template["data_example"]

        self.use_data = typer.confirm(
            "Will you use dataset for the job?", prompt_suffix="\n>>"
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use input checkpoint for the job?", prompt_suffix="\n>>"
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?", prompt_suffix="\n>>"
        )
        self.use_slack = typer.confirm(
            "Do you want to get slack notifaction for the job?", prompt_suffix="\n>>"
        )
        self.ready = True

    def render(self) -> str:
        assert self.ready

        yaml_str = DEFAULT_TEMPLATE_CONFIG
        yaml_str += PREDEFINED_JOB_SETTING_CONFIG
        yaml_str += f"  template_name: {self.model_name}\n"
        yaml_str += f"  model_config:\n"
        for k, v in self.model_config.items():
            yaml_str += f"    {k}: {v}\n"

        if self.use_data:
            yaml_str += DATA_CONFIG
        if self.use_input_checkpoint:
            yaml_str += CHECKPOINT_CONFIG
            yaml_str += INPUT_CHECKPOINT_CONFIG
        yaml_str += self._render_plugins()

        return yaml_str


@dataclass
class CredentialConfigService(InteractiveConfigMixin):
    ready: bool = False
    name: Optional[str] = None
    cred_type: Optional[CredType] = None
    value: Optional[dict] = None

    def start_interaction(self) -> None:
        self.name = typer.prompt(
            "Enter the name of your new credential.", prompt_suffix="\n>>"
        )
        cred_type_options = [ e.value for e in CredType ]
        self.cred_type = typer.prompt(
            "What kind of credential do you want to create?\n",
            f"Options: {', '.join(cred_type_options)}",
            prompt_suffix="\n>>"
        )
        if self.cred_type not in cred_type_options:
            secho_error_and_exit(
                f"You should choose the type in the following Options: {', '.join(cred_type_options)}"
            )
        cred_type_client: CredentialTypeClientService = build_client(ServiceType.CREDENTIAL_TYPE)
        schema = cred_type_client.get_schema_by_type(self.cred_type)
        properties: dict = schema['properties']
        self.value = {}
        typer.echo("Please fill in the following fields")
        for field, field_info in properties.items():
            field_info: dict
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            hide_input = True if "password" in field else False
            entered = typer.prompt(f"  {field}:\n{field_info_str}", prompt_suffix="\n  >>", hide_input=hide_input)
            self.value[field] = entered

        self._validate_schema(schema, self.value)
        self.ready = True

    def start_interaction_for_update(self, credential_id: str) -> None:
        cred_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
        prev_cred = cred_client.get_credential(credential_id)

        self.name = typer.prompt(
            "Enter the NEW name of your credential. Press ENTER if you don't want to update this.\n"
            f"Current: {prev_cred['name']}",
            prompt_suffix="\n>>",
            default=prev_cred['name'],
            show_default=False
        )
        self.cred_type = cred_type_map_inv[prev_cred['type']]
        cred_type_client: CredentialTypeClientService = build_client(ServiceType.CREDENTIAL_TYPE)
        schema = cred_type_client.get_schema_by_type(self.cred_type)
        properties: dict = schema['properties']
        self.value = {}
        typer.echo("Please fill in the following fields")
        for field, field_info in properties.items():
            field_info: dict
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            hide_input = True if "password" in field else False
            entered = typer.prompt(
                f"  {field} (Current: {prev_cred['value'][field]}):\n{field_info_str}",
                prompt_suffix="\n  >>",
                default=prev_cred['value'][field],
                show_default=False,
                hide_input=hide_input
            )
            self.value[field] = entered

        self._validate_schema(schema, self.value)
        self.ready = True

    def _validate_schema(self, schema: dict, value: dict) -> None:
        try:
            Draft7Validator(schema).validate(value)
        except ValidationError as exc:
            secho_error_and_exit(f"Format of credential value is invalid...! ({exc.message})")

    def render(self) -> Tuple[Any]:
        assert self.ready
        return self.name, self.cred_type, self.value


class DataConfigService(InteractiveConfigMixin):
    ready: bool = False
    name: Optional[str] = None
    vendor: Optional[CredType] = None
    region: Optional[str] = None
    storage_name: Optional[str] = None
    credential_id: Optional[str] = None
    metadata: Optional[dict] = None

    def _list_available_credentials(self, vendor_type: CredType) -> List[str]:
        user_cred_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
        group_cred_client: GroupCredentialClientService = build_client(ServiceType.GROUP_CREDENTIAL)

        creds = []
        creds.extend(user_cred_client.list_credentials(cred_type=vendor_type))
        creds.extend(group_cred_client.list_credentials(cred_type=vendor_type))

        return creds

    def start_interaction_common(self) -> None:
        self.name = typer.prompt(
            "Enter the name of your new credential.", prompt_suffix="\n>>",
        )
        vendor_options: List[CredType] = [CredType.S3, CredType.BLOB, CredType.GCS]
        self.vendor = typer.prompt(
            "Enter the cloud vendor where your dataset is uploaded.",
            f"Options: {', '.join(vendor_options)}",
            prompt_suffix="\n>>"
        )
        if self.vendor not in vendor_options:
            secho_error_and_exit(f"Invalid cloud vendor provided. Choose in {[e.value for e in vendor_options]}.")
        self.region = typer.prompt(
            "Enter the region of cloud storage where your dataset is uploaded."
        )
        self.storage_name = typer.prompt(
            "Enter the storage name where your dataset is uploaded.", prompt_suffix="\n>>"
        )
        available_creds = self._list_available_credentials(self.vendor)
        cloud_cred_options = "\n".join(f"  - {cred['id']}: {cred['name']}" for cred in available_creds)
        self.credential_id = typer.prompt(
            "Enter credential UUID to access your cloud storage. "
            f"Your available credentials for cloud storages are:\n{cloud_cred_options}",
            prompt_suffix="\n>>"
        )
        if self.credential_id not in [cred['id'] for cred in available_creds]:
            secho_error_and_exit(f"The credential ({self.credential_id}) cannot be used to create a datastore.")

    def render(self) -> Tuple[Any]:
        assert self.ready
        return self.name, self.vendor, self.region, self.storage_name, self.credential_id, self.metadata


class PredefinedDataConfigService(DataConfigService):
    def start_interaction(self) -> None:
        self.start_interaction_common()

        job_template_client_service: JobTemplateClientService = build_client(ServiceType.JOB_TEMPLATE)
        template_names = job_template_client_service.list_job_template_names()
        self.model_name = typer.prompt(
            "Which job would you like to use this datastore? Choose one in the following catalog:\n",
            f"Options: {', '.join(template_names)}"
            , prompt_suffix="\n>>"
        )
        if self.model_name not in template_names:
            secho_error_and_exit("Invalid job template name...!")
        template = job_template_client_service.get_job_template_by_name(self.model_name)
        assert template is not None

        schema = template["data_store_template"]["metadata_schema"]
        properties: dict = schema['properties']
        self.metadata = {}
        typer.echo("Please fill in the following fields (NOTE: Enter comma-separated string for array values)")
        for field, field_info in properties.items():
            field_info: dict
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            entered = typer.prompt(f"  {field}:\n{field_info_str}", prompt_suffix="\n  >>")
            if field_info['type'] == 'array':
                entered.split(',')
            self.metadata[field] = entered

        try:
            Draft7Validator(schema).validate(self.metadata)
        except ValidationError as exc:
            secho_error_and_exit(f"Format of credential value is invalid...! ({exc.message})")

        self.ready = True


class CustomDataConfigService(DataConfigService):
    def start_interaction(self) -> None:
        self.start_interaction_common()


def build_job_configurator(job_type: str) -> J:
    if job_type == "custom":
        configurator = CustomJobConfigService()
    elif job_type == "predefined":
        configurator = PredefinedJobConfigService()
    else:
        secho_error_and_exit("Invalid job type...!")

    configurator.start_interaction()
    return configurator


def build_data_configurator(job_type: str) -> D:
    if job_type == "custom":
        configurator = CustomDataConfigService()
    elif job_type == "predefined":
        configurator = PredefinedDataConfigService()
    else:
        secho_error_and_exit("Invalid job type...!")

    configurator.start_interaction()
    return configurator
