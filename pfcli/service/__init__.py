# Copyright (C) 2021 FriendliAI

from enum import Enum
from typing import Dict, Tuple


class ServiceType(str, Enum):
    MFA = "MFA"
    SIGNUP = "SIGNUP"
    USER = "USER"
    USER_GROUP = "USER_GROUP"
    USER_GROUP_PROJECT = "USER_GROUP_PROJECT"
    PROJECT = "PROJECT"
    GROUP = "GROUP"
    GROUP_PROJECT = "GROUP_PROJECT"
    PROJECT_JOB_CHECKPOINT = "PROJECT_JOB_CHECKPOINT"
    PROJECT_JOB_ARTIFACT = "PROJECT_JOB_ARTIFACT"
    PROJECT_JOB = "PROJECT_JOB"
    JOB_TEMPLATE = "JOB_TEMPLATE"
    GROUP_VM_QUOTA = "GROUP_VM_QUOTA"
    PFT_PROJECT_VM_QUOTA = "PROJECT_VM_QUOTA"
    PFT_PROJECT_VM_CONFIG = "PROJECT_VM_CONFIG"
    PFT_GROUP_VM_CONFIG = "GROUP_VM_CONFIG"
    CREDENTIAL = "CREDENTIAL"
    PROJECT_CREDENTIAL = "PROJECT_CREDENTIAL"
    CREDENTIAL_TYPE = "CREDENTIAL_TYPE"
    DATA = "DATA"
    PROJECT_DATA = "PROJECT_DATA"
    CHECKPOINT = "CHECKPOINT"
    GROUP_PROJECT_CHECKPOINT = "GROUP_PROJECT_CHECKPOINT"
    JOB_WS = "JOB_WS"
    DEPLOYMENT = "DEPLOYMENT"
    DEPLOYMENT_METRICS = "DEPLOYMENT_METRICS"
    PFS_PROJECT_USAGE = "PFS_PROJECT_USAGE"
    PFT_BILLING_SUMMARY = "BILLING_SUMMARY"
    METRICS = "METRICS"


class GroupRole(str, Enum):
    OWNER = "owner"
    MEMBER = "member"


class ProjectRole(str, Enum):
    ADMIN = "admin"
    MAINTAIN = "maintain"
    DEVELOP = "develop"
    GUEST = "guest"


class JobStatus(str, Enum):
    WAITING = "waiting"
    ENQUEUED = "enqueued"
    STARTED = "started"
    ALLOCATING = "allocating"
    PREPARING = "preparing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TERMINATED = "terminated"
    TERMINATING = "terminating"
    CANCELLED = "cancelled"
    CANCELLING = "cancelling"


class SimpleJobStatus(str, Enum):
    """Simplified job status delivered to users"""

    WAITING = "waiting"
    ALLOCATING = "allocating"
    PREPARING = "preparing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


job_status_map: Dict[JobStatus, SimpleJobStatus] = {
    JobStatus.WAITING: SimpleJobStatus.WAITING,
    JobStatus.ENQUEUED: SimpleJobStatus.WAITING,
    JobStatus.STARTED: SimpleJobStatus.WAITING,
    JobStatus.ALLOCATING: SimpleJobStatus.ALLOCATING,
    JobStatus.PREPARING: SimpleJobStatus.PREPARING,
    JobStatus.RUNNING: SimpleJobStatus.RUNNING,
    JobStatus.SUCCESS: SimpleJobStatus.SUCCESS,
    JobStatus.FAILED: SimpleJobStatus.FAILED,
    JobStatus.TERMINATED: SimpleJobStatus.STOPPED,
    JobStatus.TERMINATING: SimpleJobStatus.STOPPING,
    JobStatus.CANCELLED: SimpleJobStatus.STOPPED,
    JobStatus.CANCELLING: SimpleJobStatus.STOPPING,
}


job_status_map_inv: Dict[SimpleJobStatus, Tuple[JobStatus, ...]] = {
    SimpleJobStatus.WAITING: (
        JobStatus.WAITING,
        JobStatus.ENQUEUED,
        JobStatus.STARTED,
    ),
    SimpleJobStatus.ALLOCATING: (JobStatus.ALLOCATING,),
    SimpleJobStatus.PREPARING: (JobStatus.PREPARING,),
    SimpleJobStatus.RUNNING: (JobStatus.RUNNING,),
    SimpleJobStatus.SUCCESS: (JobStatus.SUCCESS,),
    SimpleJobStatus.FAILED: (JobStatus.FAILED,),
    SimpleJobStatus.STOPPING: (
        JobStatus.TERMINATING,
        JobStatus.CANCELLING,
    ),
    SimpleJobStatus.STOPPED: (
        JobStatus.TERMINATED,
        JobStatus.CANCELLED,
    ),
}


class PeriFlowService(str, Enum):
    TRAIN = "train"
    SERVE = "serve"


class JobType(str, Enum):
    PREDEFINED = "predefined"
    CUSTOM = "custom"


class LockStatus(str, Enum):
    ACTIVE = "active"
    STALE = "stale"
    DELETING = "deleting"


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


class ModelFormCategory(str, Enum):
    MEGATRON = "MEGATRON"
    ORCA = "ORCA"
    HF = "HF"
    ETC = "ETC"


class GpuType(str, Enum):
    A10G = "a10g"


class EngineType(str, Enum):
    ORCA = "orca"
    TRITON = "triton"


class DeploymentType(str, Enum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"


storage_type_map: Dict[StorageType, str] = {
    StorageType.S3: "aws",
    StorageType.BLOB: "azure.blob",
    StorageType.GCS: "gcp",
    StorageType.FAI: "fai",
}


storage_type_map_inv: Dict[str, StorageType] = {
    "aws": StorageType.S3,
    "azure.blob": StorageType.BLOB,
    "gcp": StorageType.GCS,
    "fai": StorageType.FAI,
}


class CheckpointCategory(str, Enum):
    USER_PROVIDED = "USER"
    JOB_GENERATED = "JOB"


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


cred_type_map_inv: Dict[str, CredType] = {
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
    "eastus",
    "eastus2",
    "southcentralus",
    "westus2",
    "westus3",
    "australiaeast",
    "southeastasia",
    "northeurope",
    "swedencentral",
    "uksouth",
    "westeurope",
    "centralus",
    "northcentralus",
    "westus",
    "southafricanorth",
    "centralindia",
    "eastasia",
    "japaneast",
    "jioindiawest",
    "koreacentral",
    "canadacentral",
    "francecentral",
    "germanywestcentral",
    "norwayeast",
    "switzerlandnorth",
    "uaenorth",
    "brazilsouth",
    "centralusstage",
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
    "",
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
