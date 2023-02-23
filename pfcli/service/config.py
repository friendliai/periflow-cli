# Copyright (C) 2021 FriendliAI

"""PeriFlow YAML File Configuration Service"""

from __future__ import annotations

import io
import os
import tempfile
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar, Union
from uuid import UUID

import typer
import yaml
from click import Choice
from jsonschema import Draft7Validator, ValidationError
from typing_extensions import TypeAlias

from pfcli.service import (
    CredType,
    EngineType,
    JobType,
    ServiceType,
    StorageType,
    cred_type_map_inv,
    storage_region_map,
)
from pfcli.service.client import (
    CredentialClientService,
    CredentialTypeClientService,
    JobTemplateClientService,
    PFTGroupVMConfigClientService,
    ProjectCredentialClientService,
    ProjectDataClientService,
    build_client,
)
from pfcli.service.cloud import build_storage_helper
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.prompt import get_default_editor, open_editor

DEFAULT_TEMPLATE_CONFIG = """\
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

CUSTOM_JOB_SETTING_CONFIG = (
    JOB_SETTING_CONFIG
    + """\
  type: custom

  # Docker config
  docker:
    # Docker image you want to use in the job
    image:
    # Bash shell command to run the job.
    #
    # NOTE: PeriFlow automatically sets the following environment variables for PyTorch DDP.
    #   - MASTER_ADDR: Address of rank 0 node.
    #   - WORLD_SIZE: The total number of GPUs participating in the task.
    #   - NODE_RANK: Index of the current node.
    #   - NPROC_PER_NODE: The number of processes in the current node.
    command:
"""
)

PRIVATE_DOCKER_IMG_CONFIG = """\
    credential_id:
"""

JOB_WORKSPACE_CONFIG = """\
  # Path to mount your workspace volume. If not specified, '/workspace' will be used by default.
  workspace:
    mount_path:
"""

PREDEFINED_JOB_SETTING_CONFIG = (
    JOB_SETTING_CONFIG
    + """\
  type: predefined
"""
)

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


INFERENCE_SERVER_CONFIG = """
# Inference server type config
inference_server_type:
  name:
  repo:
  tag:
"""

ORCA_CONFIG = """
# Orca engine config
orca_config:
  max_batch_size:
  max_token_count:
  kv_cache_size:
"""

CKPT_CONFIG = """
# Orca ckeckpoint config
ckpt_config:
  version:
  catagory:
  data_type:
"""

SCALER_CONFIG = """
# Keda scaler config
scaler_config:
  min_deployment_count:
  max_deployment_count:
"""


T = TypeVar("T", bound=Union[str, Tuple[Any, ...]])


class InteractiveConfigMixin(Generic[T]):
    def start_interaction(self) -> None:
        raise NotImplementedError  # prama: no cover

    def render(self) -> T:
        raise NotImplementedError  # pragma: no cover


@dataclass
class JobConfigService(Generic[T], InteractiveConfigMixin[T]):
    """Interface of job template configuration service"""

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
class CustomJobConfigService(JobConfigService[str]):
    """Custom job template configuration service"""

    # TODO: Support artifact
    use_private_image: bool = False
    use_dist: bool = False
    use_output_checkpoint: bool = False

    def start_interaction(self):
        self.use_private_image = typer.confirm(
            "Will you use your private docker image? (You should provide a credential).",
            prompt_suffix="\n>> ",
        )
        self.use_workspace = typer.confirm(
            "Do you want to run the job with the scripts in your local directory?",
            prompt_suffix="\n>> ",
        )
        self.use_data = typer.confirm(
            "Will you use a dataset for the job?", prompt_suffix="\n>> "
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use an input checkpoint for the job?", prompt_suffix="\n>> "
        )
        self.use_output_checkpoint = typer.confirm(
            "Does your job generate model checkpoint files?", prompt_suffix="\n>> "
        )
        self.use_dist = typer.confirm(
            "Will you run distributed training job?", prompt_suffix="\n>> "
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?", prompt_suffix="\n>> "
        )
        self.use_slack = typer.confirm(
            "Do you want to get a Slack notification for the job?",
            prompt_suffix="\n>> ",
        )
        self.ready = True

    def render(self) -> str:
        assert self.ready

        yaml_str = DEFAULT_TEMPLATE_CONFIG
        yaml_str += CUSTOM_JOB_SETTING_CONFIG
        if self.use_private_image:
            yaml_str += PRIVATE_DOCKER_IMG_CONFIG
        if self.use_workspace:
            yaml_str += JOB_WORKSPACE_CONFIG
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
        if self.use_dist:
            yaml_str += DIST_CONFIG
        yaml_str += self._render_plugins()

        return yaml_str


@dataclass
class PredefinedJobConfigService(JobConfigService[str]):
    """Predefined job template configuration service"""

    template_id: Optional[UUID] = None
    model_config: Optional[Dict[str, Any]] = None

    def start_interaction(self) -> None:
        job_template_client_service: JobTemplateClientService = build_client(
            ServiceType.JOB_TEMPLATE
        )

        self.template_id = typer.prompt(
            "Enter the predefined job template ID",
            prompt_suffix="\n>> ",
            type=UUID,
        )
        template = job_template_client_service.get_job_template(self.template_id)
        assert template is not None
        self.model_config = template["data_example"]

        self.use_data = typer.confirm(
            "Will you use dataset for the job?", prompt_suffix="\n>> "
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use input checkpoint for the job?", prompt_suffix="\n>> "
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?", prompt_suffix="\n>> "
        )
        self.use_slack = typer.confirm(
            "Do you want to get slack notifaction for the job?", prompt_suffix="\n>> "
        )
        self.ready = True

    def render(self) -> str:
        assert self.ready

        yaml_str = DEFAULT_TEMPLATE_CONFIG
        yaml_str += PREDEFINED_JOB_SETTING_CONFIG
        yaml_str += f"  template_id: {self.template_id}\n"
        if self.model_config:
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
class CredentialConfigService(InteractiveConfigMixin[Tuple[Any, ...]]):
    """Credential configuration service"""

    ready: bool = False
    name: Optional[str] = None
    cred_type: Optional[CredType] = None
    value: Optional[Dict[str, Any]] = None

    def start_interaction(self) -> None:
        self.name = typer.prompt(
            "Enter the name of your new credential.", prompt_suffix="\n>> "
        )
        self.cred_type = typer.prompt(
            "What kind of credential do you want to create?\n",
            type=Choice([e.value for e in CredType]),
            prompt_suffix="\n>> ",
        )
        cred_type_client: CredentialTypeClientService = build_client(
            ServiceType.CREDENTIAL_TYPE
        )
        schema = cred_type_client.get_schema_by_type(self.cred_type)
        assert schema is not None
        properties: Dict[str, Any] = schema["properties"]
        self.value = {}
        typer.echo("Please fill in the following fields")
        for field, field_info in properties.items():
            field_info: Dict[str, Any]
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            hide_input = True if "password" in field else False
            entered = typer.prompt(
                f"  {field}:\n{field_info_str}",
                prompt_suffix="\n  >> ",
                hide_input=hide_input,
            )
            self.value[field] = entered

        self._validate_schema(schema)
        self.ready = True

    def start_interaction_for_update(self, credential_id: UUID) -> None:
        cred_client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
        prev_cred = cred_client.get_credential(credential_id)

        self.name = typer.prompt(
            "Enter the NEW name of your credential. Press ENTER if you don't want to update this.\n"
            f"Current: {prev_cred['name']}",
            prompt_suffix="\n>> ",
            default=prev_cred["name"],
            show_default=False,
        )
        self.cred_type = cred_type_map_inv[prev_cred["type"]]
        cred_type_client: CredentialTypeClientService = build_client(
            ServiceType.CREDENTIAL_TYPE
        )
        schema = cred_type_client.get_schema_by_type(self.cred_type)
        assert schema is not None
        properties: Dict[str, Any] = schema["properties"]
        self.value = {}
        typer.echo("Please fill in the following fields")
        for field, field_info in properties.items():
            field_info: Dict[str, Any]
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            hide_input = True if "password" in field else False
            entered = typer.prompt(
                f"  {field} (Current: {prev_cred['value'][field]}):\n{field_info_str}",
                prompt_suffix="\n  >> ",
                default=prev_cred["value"][field],
                show_default=False,
                hide_input=hide_input,
            )
            self.value[field] = entered

        self._validate_schema(schema)
        self.ready = True

    def _validate_schema(self, schema: Dict[str, Any]) -> None:
        try:
            Draft7Validator(schema).validate(self.value)
        except ValidationError as exc:
            secho_error_and_exit(
                f"Format of credential value is invalid...! ({exc.message})"
            )

    def render(self) -> Tuple[Any, ...]:
        assert self.ready
        return self.name, self.cred_type, self.value


@dataclass
class DataConfigService(InteractiveConfigMixin[Tuple[Any, ...]]):
    ready: bool = False
    name: Optional[str] = None
    vendor: Optional[StorageType] = None
    region: Optional[str] = None
    storage_name: Optional[str] = None
    credential_id: Optional[UUID] = None
    metadata: Optional[Dict[str, Any]] = field(default_factory=dict)
    files: Optional[List[Dict[str, Any]]] = field(default_factory=list)

    def _list_available_credentials(
        self, vendor_type: StorageType
    ) -> List[Dict[str, Any]]:
        cred_type: CredType = CredType(vendor_type.value)
        project_cred_client: ProjectCredentialClientService = build_client(
            ServiceType.PROJECT_CREDENTIAL
        )

        creds = project_cred_client.list_credentials(cred_type=cred_type)

        return creds

    def _get_credential(self) -> Dict[str, Any]:
        client: CredentialClientService = build_client(ServiceType.CREDENTIAL)
        assert self.credential_id is not None
        return client.get_credential(self.credential_id)["value"]

    def start_interaction_common(self) -> None:
        self.name = typer.prompt(
            "Enter the name of your new dataset.",
            prompt_suffix="\n>> ",
        )
        self.vendor = typer.prompt(
            "Enter the cloud vendor where your dataset is uploaded.",
            type=Choice([e.value for e in StorageType]),
            prompt_suffix="\n>> ",
        )
        self.region = typer.prompt(
            "Enter the region of cloud storage where your dataset is uploaded.",
            type=Choice(storage_region_map[self.vendor]),
            prompt_suffix="\n>> ",
        )
        self.storage_name = typer.prompt(
            "Enter the storage name where your dataset is uploaded.",
            prompt_suffix="\n>> ",
        )
        available_creds = self._list_available_credentials(self.vendor)
        cloud_cred_options = "\n".join(
            f"  - {cred['id']}: {cred['name']}" for cred in available_creds
        )
        self.credential_id = typer.prompt(
            "Enter credential UUID to access your cloud storage. "
            f"Your available credentials for cloud storages are:\n{cloud_cred_options}",
            type=Choice([cred["id"] for cred in available_creds]),
            show_choices=False,
            prompt_suffix="\n>> ",
        )
        self.credential_id = UUID(self.credential_id)
        credential_value = self._get_credential()
        storage_helper = build_storage_helper(self.vendor, credential_value)
        self.files = storage_helper.list_storage_files(self.storage_name)

    def render(self) -> Tuple[Any, ...]:
        assert self.ready
        return (
            self.name,
            self.vendor,
            self.region,
            self.storage_name,
            self.credential_id,
            self.metadata,
            self.files,
        )


@dataclass
class PredefinedDataConfigService(DataConfigService):
    model_name: Optional[str] = None

    def start_interaction(self) -> None:
        self.start_interaction_common()

        # Configure metdata
        job_template_client_service: JobTemplateClientService = build_client(
            ServiceType.JOB_TEMPLATE
        )
        template_names = job_template_client_service.list_job_template_names()
        self.model_name = typer.prompt(
            "Which job would you like to use this dataset? Choose one in the following catalog:\n",
            type=Choice(template_names),
            prompt_suffix="\n>> ",
        )
        template = job_template_client_service.get_job_template_by_name(self.model_name)
        assert template is not None

        schema = template["data_store_template"]["metadata_schema"]
        properties: Dict[str, Any] = schema["properties"]
        self.metadata = {}
        typer.echo(
            "Please fill in the following fields (NOTE: Enter comma-separated string for array values)"
        )
        for field, field_info in properties.items():
            field_info: Dict[str, Any]
            field_info_str = "\n".join(f"    - {k}: {v}" for k, v in field_info.items())
            entered = typer.prompt(
                f"  {field}:\n{field_info_str}", prompt_suffix="\n  >> "
            )
            if field_info["type"] == "array":
                entered = entered.split(",")
            self.metadata[field] = entered

        try:
            Draft7Validator(schema).validate(self.metadata)
        except ValidationError as exc:
            secho_error_and_exit(
                f"Format of credential value is invalid...! ({exc.message})"
            )

        self.ready = True


@dataclass
class CustomDataConfigService(DataConfigService):
    def start_interaction(self) -> None:
        self.start_interaction_common()

        # Configure metdata
        exist_metadata = typer.confirm(
            "[Optional] Do you want to add metadata for your dataset? "
            "You can use this metadata in PeriFlow serving service.",
        )
        if exist_metadata:
            editor = typer.prompt(
                "Editor will be opened in the current terminal to edit the metadata. "
                "The metadata should be described in YAML format.\n"
                f"Your default editor is '{get_default_editor()}'. "
                "If you want to use another editor, enter the name of your preferred editor.",
                default=get_default_editor(),
                prompt_suffix="\n>> ",
            )
            with tempfile.TemporaryDirectory() as dir:
                path = os.path.join(dir, "metadata.yaml")
                open_editor(path, editor)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        self.metadata = yaml.safe_load(f)
                except yaml.YAMLError as exc:
                    secho_error_and_exit(
                        f"Error occurred while parsing metadata file... {exc}"
                    )
        self.ready = True


@dataclass
class DeploymentConfigService(InteractiveConfigMixin[str]):
    """Deployment template configuration service."""

    ready: bool = False
    use_specific_image: bool = False
    use_ckpt_config: bool = False

    def _render(self) -> str:
        yaml_str = ""
        if self.use_specific_image:
            yaml_str += INFERENCE_SERVER_CONFIG
        if self.use_ckpt_config:
            yaml_str += CKPT_CONFIG

        return yaml_str


@dataclass
class OrcaDeploymentConfigService(DeploymentConfigService):
    """Orca deployment template configuration service"""

    use_scaler: bool = False

    def start_interaction(self):
        self.ready = True

    def render(self) -> str:
        assert self.ready

        yaml_str = ORCA_CONFIG
        yaml_str += self._render()
        if self.use_scaler:
            yaml_str += SCALER_CONFIG

        return yaml_str


def build_job_configurator(job_type: str) -> JobConfigService:
    if job_type == "custom":
        configurator = CustomJobConfigService()
    elif job_type == "predefined":
        configurator = PredefinedJobConfigService()
    else:
        secho_error_and_exit("Invalid job type...!")

    configurator.start_interaction()
    return configurator


def build_data_configurator(job_type: JobType) -> DataConfigService:
    if job_type == JobType.CUSTOM:
        configurator = CustomDataConfigService()
    elif job_type == JobType.PREDEFINED:
        configurator = PredefinedDataConfigService()
    else:
        secho_error_and_exit("Invalid job type...!")

    configurator.start_interaction()
    return configurator


def build_deployment_configurator(engine_type: EngineType) -> DeploymentConfigService:
    if engine_type == EngineType.ORCA:
        configurator = OrcaDeploymentConfigService()
    else:
        secho_error_and_exit("Only orca engine type is supported!")
    configurator.start_interaction()
    return configurator


_IO: TypeAlias = Union[io.TextIOWrapper, io.FileIO, io.BytesIO]


class JobConfigManager:
    """Job configuration manager"""

    _default_json_schema = {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
            },
            "vm": {"type": "string"},
            "num_devices": {"type": "integer"},
            "job_setting": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["custom", "predefined"],
                    },
                    "docker": {
                        "type": "object",
                        "properties": {
                            "image": {"type": "string"},
                            "command": {
                                "anyOf": [
                                    {
                                        "type": "object",
                                        "properties": {
                                            "setup": {"type": "string"},
                                            "run": {"type": "string"},
                                        },
                                        "required": ["run"],
                                    },
                                    {
                                        "type": "string",
                                    },
                                ],
                            },
                            "env_var": {
                                "type": "object",
                            },
                        },
                        "required": ["image", "command"],
                    },
                    "workspace": {
                        "type": "object",
                        "properties": {"mount_path": {"type": "string"}},
                        "required": ["mount_path"],
                    },
                    "template_id": {
                        "type": "string",
                    },
                    "model_config": {
                        "type": "string",
                    },
                },
                "required": [
                    "type",
                ],
            },
            "checkpoint": {
                "type": "object",
                "properties": {
                    "input": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "mount_path": {"type": "string"},
                        },
                        "required": [
                            "id",
                            "mount_path",
                        ],
                    },
                    "output_checkpoint_dir": {"type": "string"},
                },
            },
            "data": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "mount_path": {"type": "string"},
                },
                "required": [
                    "name",
                    "mount_path",
                ],
            },
            "dist": {
                "type": "object",
                "properties": {
                    "dp_degree": {
                        "type": "integer",
                    },
                    "pp_degree": {
                        "type": "integer",
                    },
                    "mp_degree": {
                        "type": "integer",
                    },
                },
                "required": [
                    "dp_degree",
                    "pp_degree",
                    "mp_degree",
                ],
            },
            "plugin": {
                "type": "object",
                "properties": {
                    "wandb": {
                        "type": "object",
                        "properties": {"credential_id": {"type": "string"}},
                        "required": ["credential_id"],
                    }
                },
            },
        },
        "required": [
            "vm",
            "num_devices",
            "job_setting",
        ],
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config

    @classmethod
    def from_file(cls, f: _IO) -> JobConfigManager:
        """Create a new `JobConfigManager` object from a job configuration YAML file.

        Args:
            f (io.TextIOWrapper): File descriptor of the job configuration YAML file.

        Returns:
            JobConfigManager: Object created from the YAML file.

        """
        try:
            config: Dict[str, Any] = yaml.safe_load(f)
        except yaml.YAMLError as e:
            secho_error_and_exit(f"Error occurred while parsing config file: {e!r}")

        return cls(config)

    def update_config(
        self,
        vm: Optional[str] = None,
        num_devices: Optional[int] = None,
        name: Optional[str] = None,
    ) -> None:
        """In-place update the job configuration.

        Args:
            vm (Optional[str], optional): VM name. Defaults to None.
            num_devices (Optional[int], optional): The number of devices. Defaults to None.
            name (Optional[str], optional): Job name. Defaults to None.

        """
        if num_devices is not None:
            self._config["num_devices"] = num_devices
        if name is not None:
            self._config["name"] = name
        if vm is not None:
            self._config["vm"] = vm

    def validate(self) -> None:
        """Run format validation.

        Returns:
            bool: True if the format is valid, otherwise False.

        """
        try:
            Draft7Validator(self._default_json_schema).validate(self._config)
        except ValidationError as exc:
            secho_error_and_exit(f"Invalid job configuration: {exc.message!r}")

    def get_job_request_body(self) -> Dict[str, Any]:
        """Get a request body for the REST API call.

        Returns:
            Dict[str, Any]: Post-processed job request body.

        """
        body = deepcopy(self._config)
        if (
            body["job_setting"]["type"] == "custom"
            and "workspace" not in body["job_setting"]
        ):
            body["job_setting"]["workspace"] = {"mount_path": "/workspace"}

        data_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)
        vm_client: PFTGroupVMConfigClientService = build_client(
            ServiceType.PFT_GROUP_VM_CONFIG
        )

        vm_name = body["vm"]
        vm_config_id = vm_client.get_id_by_name(vm_name)
        if vm_config_id is None:
            secho_error_and_exit(f"VM({vm_name}) is not found.")
        del body["vm"]
        body["vm_config_id"] = vm_config_id

        if "data" in body:
            data_name: str = body["data"]["name"]
            if data_name.startswith("huggingface:"):
                body["public_source"] = {
                    "data": {
                        "provider": "huggingface",
                        "name": data_name.lstrip("huggingface:"),
                        "mount_path": body["data"]["mount_path"],
                    }
                }
                del body["data"]
            else:
                data_id = data_client.get_id_by_name(data_name)
                if data_id is None:
                    secho_error_and_exit(f"Dataset ({data_name}) is not found.")
                body["data"]["id"] = data_id
                del body["data"]["name"]

        if body["job_setting"]["type"] == "custom":
            if "launch_mode" not in body["job_setting"]:
                body["job_setting"]["launch_mode"] = "node"

            if "docker" in body["job_setting"]:
                docker_command = body["job_setting"]["docker"]["command"]
                if isinstance(docker_command, str):
                    body["job_setting"]["docker"]["command"] = {
                        "setup": "",
                        "run": docker_command,
                    }
                elif isinstance(docker_command, dict) and "setup" not in docker_command:
                    body["job_setting"]["docker"]["command"]["setup"] = ""

        return body
