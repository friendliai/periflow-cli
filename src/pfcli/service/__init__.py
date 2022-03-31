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
    GROUP_DATA = "GROUP_DATA"
    JOB_WS = "JOB_WS"


class LogType(str, Enum):
    STDOUT = "stdout"
    STDERR = "stderr"
    VMLOG = "vmlog"
