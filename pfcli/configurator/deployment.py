# Copyright (C) 2021 FriendliAI

"""PeriFlow Deployment Configurator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Type

from pfcli.configurator.base import InteractiveConfigurator
from pfcli.service import EngineType

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
