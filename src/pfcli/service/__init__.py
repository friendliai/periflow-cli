# Copyright (C) 2021 FriendliAI

from enum import Enum

class ServiceType(str, Enum):
    USER_GROUP = "USER_GROUP"
    JOB = "JOB"
    JOB_CHECKPOINT = "JOB_CHECKPOINT"
    JOB_ARTIFACT = "JOB_ARTIFACT"
    GROUP_JOB = "GROUP_JOB"
    JOB_TEMPLATE = "JOB_TEMPLATE"
    GROUP_EXPERIMENT = "GROUP_EXPERIMENT"
    GROUP_VM = "GROUP_VM"
    CREDENTIAL = "CREDENTIAL"
    GROUP_DATA = "GROUP_DATA"
    CHECKPOINT = "CHECKPOINT"
    GROUP_CHECKPOINT = "GROUP_CHECKPOINT"
    JOB_WS = "JOB_WS"


class LogType(str, Enum):
    STDOUT = "stdout"
    STDERR = "stderr"
    VMLOG = "vmlog"


class VendorType(str, Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"


class CheckpointCategory(str, Enum):
    USER_PROVIDED = "user_provided"
    JOB_GENERATED = "job_generated"
