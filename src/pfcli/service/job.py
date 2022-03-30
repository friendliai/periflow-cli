# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI Job Service"""

import io
from dataclasses import dataclass

import yaml

from pfcli.service import CLIService
from pfcli.utils import secho_error_and_exit


@dataclass
class JobCLIService(CLIService):
    def run(self, config_file: io.TextIOWrapper) -> None:
        config = self._read_job_config_file(config_file)

    def _read_job_config_file(self, config_file: io.TextIOWrapper) -> dict:
        try:
            return yaml.safe_load(config_file)
        except yaml.YAMLError as e:
            secho_error_and_exit(f"Error occurred while parsing config file... {e}")

    def _lint_config(self, config: dict):
        assert "vm" in config
        assert "num_devices" in config
        assert "experiment" in config

    def _refine_config(self, config: dict) -> None:
        experiment_name = config["experiment"]
        experiment_id = infer_experiment_id_from_name(experiment_name)
        del config["experiment"]
        config["experiment_id"] = experiment_id

        vm_name = config["vm"]
        vm_config_id = infer_vm_config_id_from_name(vm_name)
        del config["vm"]
        config["vm_config_id"] = vm_config_id

        if "data" in config:
            data_name = config["data"]["name"]
            data_id = infer_data_id_from_name(data_name)
            del config["data"]["name"]
            config["data"]["id"] = data_id

        if config["job_setting"]["type"] == "custom":
            config["job_setting"]["launch_mode"] = "node"
        else:
            job_template_name = config["job_setting"]["template_name"]
            job_template_config = infer_job_template_config_from_name(job_template_name)
            del config["job_setting"]["template_name"]
            config["job_setting"]["engine_code"] = job_template_config["engine_code"]
            config["job_setting"]["model_code"] = job_template_config["model_code"]