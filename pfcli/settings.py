# Copyright (C) 2022 FriendliAI

"""Periflow CLI Settings"""

from pathlib import Path

class Settings():
    pfs_only: bool = False

    # uris
    periflow_directory = Path.home() / ".periflow"
    periflow_api_server = "https://api-staging.friendli.ai/api/"
    periflow_ws_server = "wss://api-ws-staging.friendli.ai/ws/"
    periflow_discuss_url = "https://discuss-staging.friendli.ai/"
    periflow_mr_server = "https://pfmodelregistry-staging.friendli.ai/"
    periflow_serve_server = "http://0.0.0.0:8000/"
    periflow_auth_server = "https://pfauth-staging.friendli.ai/"
    periflow_meter_server = "https://pfmeter-staging.friendli.ai/"
    periflow_observatory_server = "https://pfo-staging.friendli.ai/"


settings = Settings()
