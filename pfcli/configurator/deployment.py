# Copyright (C) 2021 FriendliAI

"""PeriFlow Deployment Configurator."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict, Type

from pfcli.configurator.base import IO, Configurator, InteractiveConfigurator
from pfcli.service import EngineType
from pfcli.utils.format import secho_error_and_exit

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


@dataclass
class DeploymentInteractiveConfigurator(InteractiveConfigurator[str]):
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
class OrcaDeploymentInteractiveConfigurator(DeploymentInteractiveConfigurator):
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


def build_deployment_interactive_configurator(
    engine_type: EngineType,
) -> DeploymentInteractiveConfigurator:
    handler_map: Dict[EngineType, Type[DeploymentInteractiveConfigurator]] = {
        EngineType.ORCA: OrcaDeploymentInteractiveConfigurator,
    }
    configurator = handler_map[engine_type]()

    configurator.start_interaction()
    return configurator


class DRCConfigurator(Configurator):
    """Configurator for default request config."""

    @property
    def validation_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "stop": {"type": "array", "items": {"type": "string"}},
                "stop_tokens": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tokens": {"type": "array", "items": {"type": "integer"}}
                        },
                        "required": ["tokens"],
                    },
                },
                "bad_words": {"type": "array", "items": {"type": "string"}},
                "bad_word_tokens": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tokens": {"type": "array", "items": {"type": "integer"}}
                        },
                        "required": ["tokens"],
                    },
                },
            },
            "required": [],
            "oneOf": [
                {"required": ["stop"], "not": {"required": ["stop_tokens"]}},
                {"required": ["stop_tokens"], "not": {"required": ["stop"]}},
                {"required": ["bad_words"], "not": {"required": ["bad_word_tokens"]}},
                {"required": ["bad_word_tokens"], "not": {"required": ["bad_words"]}},
            ],
        }

    @classmethod
    def from_file(cls, f: IO) -> Configurator:
        try:
            config: Dict[str, Any] = json.load(f)
        except json.JSONDecodeError as e:
            secho_error_and_exit(f"Error occurred while parsing config file: {e!r}")

        return cls(config)  # type: ignore
