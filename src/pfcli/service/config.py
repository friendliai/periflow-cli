# Copyright (C) 2021 FriendliAI

"""PeriFlow YAML File Configuration Service"""

from dataclasses import dataclass
from typing import TypeVar, Optional

import typer
from pfcli.service import ServiceType

from pfcli.service.client import JobTemplateClientService, build_client
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


J = TypeVar('J', bound='JobTemplateConfigService')


@dataclass
class JobTemplateConfigService:
    """Interface of job template configuration service
    """
    ready: bool = False
    use_data: bool = False
    use_input_checkpoint: bool = False
    use_wandb: bool = False
    use_slack: bool = False

    def start_interaction(self) -> None:
        raise NotImplementedError   # prama: no cover

    def render(self) -> str:
        raise NotImplementedError   # pragma: no cover

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
class CustomJobTemplateConfigService(JobTemplateConfigService):
    """Custom job template configuration service
    """
    # TODO: Support workspace, artifact
    use_private_image: bool = False
    use_dist: bool = False
    use_output_checkpoint: bool = False

    def start_interaction(self):
        self.use_private_img = typer.confirm(
            "Will you use your private docker image? (You should provide credential)."
        )
        self.use_dist = typer.confirm(
            "Will you run distributed training job?"
        )
        self.use_data = typer.confirm(
            "Will you use dataset for the job?"
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use input checkpoint for the job?"
        )
        self.use_output_checkpoint = typer.confirm(
            "Does your job generate model checkpoint file?"
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?"
        )
        self.use_slack = typer.confirm(
            "Do you want to get slack notifaction for the job?"
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
class PredefinedJobTemplateConfigService(JobTemplateConfigService):
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
        )
        if self.model_name not in template_names:
            secho_error_and_exit("Invalid job template name...!")
        template = job_template_client_service.get_job_template_by_name(self.model_name)
        assert template is not None
        self.model_config = template["data_example"]

        self.use_data = typer.confirm(
            "Will you use dataset for the job?"
        )
        self.use_input_checkpoint = typer.confirm(
            "Will you use input checkpoint for the job?"
        )
        self.use_wandb = typer.confirm(
            "Will you use W&B monitoring for the job?"
        )
        self.use_slack = typer.confirm(
            "Do you want to get slack notifaction for the job?"
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


def build_job_template_configurator(job_type: str) -> J:
    if job_type == "custom":
        configurator = CustomJobTemplateConfigService()
    elif job_type == "predefined":
        configurator = PredefinedJobTemplateConfigService()
    else:
        secho_error_and_exit("Invalid job type...!")

    configurator.start_interaction()
    return configurator


def build_data_template_configurator():
    ...
