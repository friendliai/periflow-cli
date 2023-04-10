# Copyright (C) 2021 FriendliAI

"""PeriFlow Data Configurator."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type
from uuid import UUID

import typer
import yaml
from click import Choice
from jsonschema import Draft7Validator, ValidationError

from pfcli.configurator.base import InteractiveConfigurator
from pfcli.service import (
    CredType,
    JobType,
    ServiceType,
    StorageType,
    storage_region_map,
)
from pfcli.service.client import (
    CredentialClientService,
    JobTemplateClientService,
    ProjectCredentialClientService,
    build_client,
)
from pfcli.service.cloud import build_storage_helper
from pfcli.utils.format import secho_error_and_exit
from pfcli.utils.prompt import get_default_editor, open_editor


@dataclass
class DataInteractiveConfigurator(InteractiveConfigurator[Tuple[Any, ...]]):
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
class PredefinedDataInteractiveConfigurator(DataInteractiveConfigurator):
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
class CustomDataInteractiveConfigurator(DataInteractiveConfigurator):
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


def build_data_interactive_configurator(
    job_type: JobType,
) -> DataInteractiveConfigurator:
    handler_map: Dict[JobType, Type[DataInteractiveConfigurator]] = {
        JobType.CUSTOM: CustomDataInteractiveConfigurator,
        JobType.PREDEFINED: PredefinedDataInteractiveConfigurator,
    }
    configurator = handler_map[job_type]()

    configurator.start_interaction()
    return configurator
