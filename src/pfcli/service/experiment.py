# Copyright (C) 2021 FriendliAI

"""PeriFlow Experiment CLI Service"""

from pfcli.service import CLIService


class ExperimentCLIService(CLIService):
    def infer_id_from_name(self):
        ...
