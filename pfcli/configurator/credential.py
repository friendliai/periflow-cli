# Copyright (C) 2021 FriendliAI

"""PeriFlow Credential Configurator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import typer
from click import Choice
from jsonschema import Draft7Validator, ValidationError

from pfcli.configurator.base import InteractiveConfigurator
from pfcli.service import CredType, ServiceType, cred_type_map_inv
from pfcli.service.client import (
    CredentialClientService,
    CredentialTypeClientService,
    build_client,
)
from pfcli.utils.format import secho_error_and_exit


@dataclass
class CredentialInteractiveConfigurator(InteractiveConfigurator[Tuple[Any, ...]]):
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
