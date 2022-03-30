# Copyright (C) 2021 FriendliAI

from enum import Enum

class ServiceType(str, Enum):
    USER_GROUP = "USER_GROUP"
    JOB = "JOB"
    JOB_CHECKPOINT = "JOB_CHECKPOINT"
    JOB_ARTIFACT = "JOB_ARTIFACT"
    GROUP_JOB = "GROUP_JOB"
