"""Test Job Configurator."""

from __future__ import annotations

from tempfile import TemporaryFile

import pytest
import yaml
from _pytest.fixtures import SubRequest

from pfcli.configurator.job import JobConfigurator
from pfcli.utils.testing import merge_yaml_strings


@pytest.fixture
def required_config_custom() -> str:
    return """vm: az.NC24ads_A100_v4
num_devices: 2
job_setting:
  type: custom
"""


@pytest.fixture
def required_config_predefined() -> str:
    return """vm: az.NC24ads_A100_v4
num_devices: 2
job_setting:
  type: predefined
"""


@pytest.fixture
def name_config(request: SubRequest) -> str:
    return "name: my-job" if request.param else ""


@pytest.fixture
def docker_config(request: SubRequest) -> str:
    return (
        """job_setting:
  docker:
    image: friendliai/periflow:sdk
    command:
      setup: cd /workspace && pip install -r requirements.txt
      run: |
        cd /workspace && torchrun --nnodes $NUM_NODES --node_rank $NODE_RANK --master_addr $MASTER_ADDR --master_port 6000 --nproc_per_node $NPROC_PER_NODE train.py
          --config_name bert-base-uncased \
          --tokenizer_name bert-base-uncased \
          --dataset_name wikitext \
          --dataset_config_name wikitext-2-raw-v1 \
          --per_device_train_batch_size 16 \
          --per_device_eval_batch_size 8 \
          --max_seq_length 256 \
          --do_train \
          --do_eval \
          --max_steps 700 \
          --save_steps 100 \
          --report_to wandb \
          --logging_steps 10
    env_var:
      WANDB_PROJECT: hf-mlm
"""
        if request.param
        else ""
    )


@pytest.fixture
def workspace_config(request: SubRequest) -> str:
    return (
        """job_setting:
  workspace:
    mount_path: /workspace
"""
        if request.param
        else ""
    )


@pytest.fixture
def input_checkpoint_config(request: SubRequest) -> str:
    return (
        """checkpoint:
  input:
    id: 83198783-d13b-40b6-b9ca-8bfad2fde8da
    mount_path: /ckpt
"""
        if request.param
        else ""
    )


@pytest.fixture
def output_checkpoint_config(request: SubRequest) -> str:
    return (
        """checkpoint:
  output_checkpoint_dir: /ckpt
"""
        if request.param
        else ""
    )


@pytest.fixture
def data_config(request: SubRequest) -> str:
    return (
        """data:
  name: my-data
  mount_path: /data
"""
        if request.param
        else ""
    )


@pytest.fixture
def wandb_config(request: SubRequest) -> str:
    return (
        """plugin:
  wandb:
    # W&B API key
    credential_id: 3b2cbea1-482d-47d4-8f63-7c6fca29e8ff
"""
        if request.param
        else ""
    )


@pytest.fixture
def predefined_config(request: SubRequest) -> str:
    return (
        """job_settings:
  template_id: e039f470-ed78-47ea-8ca1-e7f3ee1831db
  model_config:
    lr: 1e-4
    weight_deacy: 1e-3
"""
        if request.param
        else ""
    )


class TestCustomJobConfig:
    @pytest.mark.parametrize("name_config", [True, False], indirect=True)
    @pytest.mark.parametrize("docker_config", [True, False], indirect=True)
    @pytest.mark.parametrize("workspace_config", [True, False], indirect=True)
    @pytest.mark.parametrize("input_checkpoint_config", [True, False], indirect=True)
    @pytest.mark.parametrize("output_checkpoint_config", [True, False], indirect=True)
    @pytest.mark.parametrize("data_config", [True, False], indirect=True)
    @pytest.mark.parametrize("wandb_config", [True, False], indirect=True)
    def test_validation(
        self,
        required_config_custom: str,
        name_config: str,
        docker_config: str,
        workspace_config: str,
        input_checkpoint_config: str,
        output_checkpoint_config: str,
        data_config: str,
        wandb_config: str,
    ):
        config_yaml_string = merge_yaml_strings(
            [
                required_config_custom,
                name_config,
                docker_config,
                workspace_config,
                input_checkpoint_config,
                output_checkpoint_config,
                data_config,
                wandb_config,
            ]
        )
        with TemporaryFile(prefix="periflow-cli-unittest", mode="r+") as f:
            yaml.dump(yaml.safe_load(config_yaml_string), f)
            f.seek(0)  # Move cursor to the start of the file
            cfg_manager = JobConfigurator.from_file(f)  # type: ignore
            cfg_manager.validate()


class TestPredefinedJobConfig:
    @pytest.mark.parametrize("name_config", [True, False], indirect=True)
    @pytest.mark.parametrize("input_checkpoint_config", [True, False], indirect=True)
    @pytest.mark.parametrize("data_config", [True, False], indirect=True)
    @pytest.mark.parametrize("wandb_config", [True, False], indirect=True)
    @pytest.mark.parametrize("predefined_config", [True, False], indirect=True)
    def test_validation(
        self,
        required_config_predefined: str,
        name_config: str,
        input_checkpoint_config: str,
        data_config: str,
        wandb_config: str,
        predefined_config: str,
    ):
        config_yaml_string = merge_yaml_strings(
            [
                required_config_predefined,
                name_config,
                input_checkpoint_config,
                data_config,
                wandb_config,
                predefined_config,
            ]
        )
        with TemporaryFile(prefix="periflow-cli-unittest", mode="r+") as f:
            yaml.dump(yaml.safe_load(config_yaml_string), f)
            f.seek(0)  # Move cursor to the start of the file
            cfg_manager = JobConfigurator.from_file(f)  # type: ignore
            cfg_manager.validate()
