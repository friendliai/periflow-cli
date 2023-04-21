# Copyright (C) 2021 FriendliAI

"""PeriFlow Job Configurator."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, Type, Union
from uuid import UUID

import typer
import yaml

from pfcli.configurator.base import IO, Configurator, InteractiveConfigurator, T
from pfcli.service import JobType, ServiceType
from pfcli.service.client import JobTemplateClientService, build_client
from pfcli.service.client.group import PFTGroupVMConfigClientService
from pfcli.service.client.project import ProjectDataClientService
from pfcli.utils.format import secho_error_and_exit

DEFAULT_JOB_TEMPLATE_CONFIG = """\
# The name of job
name:
"""

DEFAULT_CUSTOM_JOB_TEMPLATE_CONFIG = (
    DEFAULT_JOB_TEMPLATE_CONFIG
    + """
# The name of vm type
vm:

# The number of GPU devices
num_devices:
"""
)

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
      setup:
      run:
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


@dataclass
class JobInteractiveConfigurator(Generic[T], InteractiveConfigurator[T]):
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
class CustomJobInteractiveConfigurator(JobInteractiveConfigurator[str]):
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

        yaml_str = DEFAULT_CUSTOM_JOB_TEMPLATE_CONFIG
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
class PredefinedJobInteractiveConfigurator(JobInteractiveConfigurator[str]):
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

        yaml_str = DEFAULT_JOB_TEMPLATE_CONFIG
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


def build_job_interactive_configurator(job_type: JobType) -> JobInteractiveConfigurator:
    handler_map: Dict[JobType, Type[JobInteractiveConfigurator]] = {
        JobType.CUSTOM: CustomJobInteractiveConfigurator,
        JobType.PREDEFINED: PredefinedJobInteractiveConfigurator,
    }
    configurator = handler_map[job_type]()

    configurator.start_interaction()
    return configurator


class JobConfigurator(Configurator):
    """Job configuration manager."""

    @classmethod
    def from_file(cls, f: IO) -> JobConfigurator:
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

        return cls(config)  # type: ignore


class CustomJobConfigurator(JobConfigurator):
    """Custom job configuration manager."""

    @property
    def validation_schema(self) -> dict:
        return {
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
                        "type": {"const": "custom"},
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
                                            "additionalProperties": False,
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
                            "additionalProperties": False,
                        },
                    },
                    "required": [
                        "type",
                        "docker",
                    ],
                    "additionalProperties": False,
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
                            "additionalProperties": False,
                        },
                        "output_checkpoint_dir": {"type": "string"},
                    },
                    "additionalProperties": False,
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
                    "additionalProperties": False,
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
                    "additionalProperties": False,
                },
                "plugin": {
                    "type": "object",
                    "properties": {
                        "wandb": {
                            "type": "object",
                            "properties": {"credential_id": {"type": "string"}},
                            "required": ["credential_id"],
                        },
                        "slack": {
                            "type": "object",
                            "properties": {
                                "channel": {"type": "string"},
                                "credential_id": {"type": "string"},
                            },
                            "required": ["channel", "credential_id"],
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "required": [
                "vm",
                "num_devices",
                "job_setting",
            ],
            "additionalProperties": False,
        }

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


class PredefinedJobConfigurator(JobConfigurator):
    """Predefined job configuration manager."""

    @property
    def validation_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                },
                "job_setting": {
                    "type": "object",
                    "properties": {
                        "type": {"const": "predefined"},
                        "template_id": {
                            "type": "string",
                        },
                        "model_config": {
                            "type": "object",
                            "minProperties": 1,
                        },
                    },
                    "required": [
                        "type",
                        "template_id",
                    ],
                    "additionalProperties": False,
                },
                "checkpoint": {
                    "type": "object",
                    "properties": {
                        "input": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                            },
                            "required": [
                                "id",
                            ],
                            "additionalProperties": False,
                        },
                    },
                    "additionalProperties": False,
                },
                "data": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                    },
                    "required": [
                        "name",
                    ],
                    "additionalProperties": False,
                },
                "plugin": {
                    "type": "object",
                    "properties": {
                        "wandb": {
                            "type": "object",
                            "properties": {"credential_id": {"type": "string"}},
                            "required": ["credential_id"],
                        },
                        "slack": {
                            "type": "object",
                            "properties": {
                                "channel": {"type": "string"},
                                "credential_id": {"type": "string"},
                            },
                            "required": ["channel", "credential_id"],
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "required": [
                "job_setting",
            ],
            "additionalProperties": False,
        }

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
        if num_devices is not None or vm is not None:
            secho_error_and_exit("Cannot configure VM settings for the predefined job.")
        if name is not None:
            self._config["name"] = name

    def get_job_request_body(self) -> Dict[str, Any]:
        """Get a request body for the REST API call.

        Returns:
            Dict[str, Any]: Post-processed job request body.

        """
        body = deepcopy(self._config)

        data_client: ProjectDataClientService = build_client(ServiceType.PROJECT_DATA)

        if "data" in body:
            data_name: str = body["data"]["name"]
            if data_name.startswith("huggingface:"):
                body["public_source"] = {
                    "data": {
                        "provider": "huggingface",
                        "name": data_name.lstrip("huggingface:"),
                    }
                }
                del body["data"]
            else:
                data_id = data_client.get_id_by_name(data_name)
                if data_id is None:
                    secho_error_and_exit(f"Dataset ({data_name}) is not found.")
                body["data"]["id"] = data_id
                del body["data"]["name"]

        return body


def get_configurator(f: IO) -> Union[CustomJobConfigurator, PredefinedJobConfigurator]:
    try:
        config: Dict[str, Any] = yaml.safe_load(f)
    except yaml.YAMLError as e:
        secho_error_and_exit(f"Error occurred while parsing config file: {e!r}")

    type_to_cls = {
        "custom": CustomJobConfigurator,
        "predefined": PredefinedJobConfigurator,
    }
    try:
        cls = type_to_cls[config["job_setting"]["type"]]
    except KeyError:
        secho_error_and_exit(
            f"Job config file({f.name}) should have 'type' field under 'job_setting'."
        )
    return cls(config)
