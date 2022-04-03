# Copyright (C) 2021 FriendliAI

from enum import Enum
from typing import Dict


class ServiceType(str, Enum):
    USER_GROUP = "USER_GROUP"
    JOB = "JOB"
    JOB_CHECKPOINT = "JOB_CHECKPOINT"
    JOB_ARTIFACT = "JOB_ARTIFACT"
    GROUP_JOB = "GROUP_JOB"
    JOB_TEMPLATE = "JOB_TEMPLATE"
    GROUP_EXPERIMENT = "GROUP_EXPERIMENT"
    GROUP_VM = "GROUP_VM"
    GROUP_VM_QUOTA = "GROUP_VM_QUOTA"
    CREDENTIAL = "CREDENTIAL"
    GROUP_CREDENTIAL = "GROUP_CREDENTIAL"
    CREDENTIAL_TYPE = "CREDENTIAL_TYPE"
    DATA = "DATA"
    GROUP_DATA = "GROUP_DATA"
    CHECKPOINT = "CHECKPOINT"
    GROUP_CHECKPOINT = "GROUP_CHECKPOINT"
    JOB_WS = "JOB_WS"


class JobType(str, Enum):
    PREDEFINED = "predefined"
    CUSTOM = "custom"


class LogType(str, Enum):
    STDOUT = "stdout"
    STDERR = "stderr"
    VMLOG = "vmlog"


class CloudType(str, Enum):
    S3 = "s3"
    BLOB = "azure-blob"
    GCS = "gcs"


class CheckpointCategory(str, Enum):
    USER_PROVIDED = "user_provided"
    JOB_GENERATED = "job_generated"


class CredType(str, Enum):
    DOCKER = "docker"
    S3 = "s3"
    BLOB = "azure-blob"
    GCS = "gcs"
    WANDB = "wandb"
    SLACK = "slack"


cred_type_map: Dict[CredType, str] = {
    CredType.DOCKER: "docker",
    CredType.S3: "aws",
    CredType.BLOB: "azure.blob",
    CredType.GCS: "gcp",
    CredType.WANDB: "wandb",
    CredType.SLACK: "slack",
}


cred_type_map_inv: Dict[CredType, str] = {
    "docker": CredType.DOCKER,
    "aws": CredType.S3,
    "azure.blob": CredType.BLOB,
    "gcp": CredType.GCS,
    "wandb": CredType.WANDB,
    "slack": CredType.SLACK,
}
