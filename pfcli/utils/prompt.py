# Copyright (c) 2022-present, FriendliAI Inc. All rights reserved.

"""PeriFlow CLI Interactive Prompt Utilities"""

from typing import Optional
from subprocess import CalledProcessError, check_call
import os

import typer


def open_editor(path: str, editor: Optional[str] = None):
    default_editor = editor or get_default_editor()
    try:
        check_call([default_editor, path])
    except CalledProcessError:
        typer.secho("", fg=typer.colors.RED)


def get_default_editor() -> str:
    return os.environ.get("PERIFLOW_CLI_EDITOR", "vim")
