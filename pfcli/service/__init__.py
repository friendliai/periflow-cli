# Copyright (C) 2021 FriendliAI

from enum import Enum
from typing import Dict


class ServiceType(str, Enum):
    USER_GROUP = "USER_GROUP"
    EXPERIMENT = "EXPERIMENT"
    GROUP_EXPERIMENT = "GROUP_EXPERIMENT"
    JOB = "JOB"
    JOB_CHECKPOINT = "JOB_CHECKPOINT"
    JOB_ARTIFACT = "JOB_ARTIFACT"
    GROUP_JOB = "GROUP_JOB"
    JOB_TEMPLATE = "JOB_TEMPLATE"
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
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"


class StorageType(str, Enum):
    S3 = "s3"
    BLOB = "azure-blob"
    GCS = "gcs"
    FAI = "fai"


storage_type_map: Dict[StorageType, str] = {
    StorageType.S3: "aws",
    StorageType.BLOB: "azure.blob",
    StorageType.GCS: "gcp",
    StorageType.FAI: "fai",
}


storage_type_map_inv: Dict[StorageType, str] = {
    "aws": StorageType.S3,
    "azure.blob": StorageType.BLOB,
    "gcp": StorageType.GCS,
    "fai": StorageType.FAI,
}


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

GCP_REGION_NAMES = [
    "asia-east1-a",
    "asia-east1-b",
    "asia-east1-c",
    "asia-east2-a",
    "asia-east2-b",
    "asia-east2-c",
    "asia-northeast1-a",
    "asia-northeast1-b",
    "asia-northeast1-c",
    "asia-northeast2-a",
    "asia-northeast2-b",
    "asia-northeast2-c",
    "asia-northeast3-a",
    "asia-northeast3-b",
    "asia-northeast3-c",
    "asia-south1-a",
    "asia-south1-b",
    "asia-south1-c",
    "asia-south2-a",
    "asia-south2-b",
    "asia-south2-c",
    "asia-southeast1-a",
    "asia-southeast1-b",
    "asia-southeast1-c",
    "asia-southeast2-a",
    "asia-southeast2-b",
    "asia-southeast2-c",
    "australia-southeast1-a",
    "australia-southeast1-b",
    "australia-southeast1-c",
    "australia-southeast2-a",
    "australia-southeast2-b",
    "australia-southeast2-c",
    "europe-central2-a",
    "europe-central2-b",
    "europe-central2-c",
    "europe-north1-a",
    "europe-north1-b",
    "europe-north1-c",
    "europe-west1-b",
    "europe-west1-c",
    "europe-west1-d",
    "europe-west2-a",
    "europe-west2-b",
    "europe-west2-c",
    "europe-west3-a",
    "europe-west3-b",
    "europe-west3-c",
    "europe-west4-a",
    "europe-west4-b",
    "europe-west4-c",
    "europe-west6-a",
    "europe-west6-b",
    "europe-west6-c",
    "northamerica-northeast1-a",
    "northamerica-northeast1-b",
    "northamerica-northeast1-c",
    "northamerica-northeast2-a",
    "northamerica-northeast2-b",
    "northamerica-northeast2-c",
    "southamerica-east1-a",
    "southamerica-east1-b",
    "southamerica-east1-c",
    "southamerica-west1-a",
    "southamerica-west1-b",
    "southamerica-west1-c",
    "us-central1-a",
    "us-central1-b",
    "us-central1-c",
    "us-central1-f",
    "us-east1-b",
    "us-east1-c",
    "us-east1-d",
    "us-east4-a",
    "us-east4-b",
    "us-east4-c",
    "us-west1-a",
    "us-west1-b",
    "us-west1-c",
    "us-west2-a",
    "us-west2-b",
    "us-west2-c",
    "us-west3-a",
    "us-west3-b",
    "us-west3-c",
    "us-west4-a",
    "us-west4-b",
]

AZURE_REGION_NAMES = [
    'eastus',
    'eastus2',
    'southcentralus',
    'westus2',
    'westus3',
    'australiaeast',
    'southeastasia',
    'northeurope',
    'swedencentral',
    'uksouth',
    'westeurope',
    'centralus',
    'northcentralus',
    'westus',
    'southafricanorth',
    'centralindia',
    'eastasia',
    'japaneast',
    'jioindiawest',
    'koreacentral',
    'canadacentral',
    'francecentral',
    'germanywestcentral',
    'norwayeast',
    'switzerlandnorth',
    'uaenorth',
    'brazilsouth',
    'centralusstage'
]

AWS_REGION_NAMES = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-south-1",
    "sa-east-1",
]

FAI_REGION_NAMES = [
    '',
]

storage_region_map = {
    StorageType.S3: AWS_REGION_NAMES,
    StorageType.BLOB: AZURE_REGION_NAMES,
    StorageType.GCS: GCP_REGION_NAMES,
    StorageType.FAI: FAI_REGION_NAMES,
}


cloud_region_map = {
    CloudType.AWS: AWS_REGION_NAMES,
    CloudType.AZURE: AZURE_REGION_NAMES,
    CloudType.GCP: GCP_REGION_NAMES,
}