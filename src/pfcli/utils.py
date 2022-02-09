from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional
import math
import json

import typer
from pfcli import autoauth

# Variables
periflow_api_server = "https://api-dev.friendli.ai/api/"
periflow_ws_server = "wss://api-ws-dev.friendli.ai/ws/"


def datetime_to_pretty_str(past: Optional[datetime], long_list: bool):
    cur = datetime.now().astimezone()
    delta = cur - past
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{delta.seconds % 60}s ago'
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {round((delta.seconds % 3600) / 60)}m {delta.seconds % 60}s ago'
        elif delta < timedelta(days=3):
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{round((delta.seconds % 3600) / 60)}m ago'
        else:
            return past.astimezone(tz=cur.tzinfo).strftime("%Y-%d-%m %H:%M:%S")
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins ago'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours ago'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days ago'


def timedelta_to_pretty_str(start: datetime, finish: datetime, long_list: bool):
    delta = finish - start
    if long_list:
        if delta < timedelta(minutes=1):
            return f'{(delta.seconds % 60)}s'
        if delta < timedelta(hours=1):
            return f'{(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        elif delta < timedelta(days=1):
            return f'{delta.seconds // 3600}h {(delta.seconds % 3600) // 60}m {(delta.seconds % 60)}s'
        else:
            return f'{delta.days}d {delta.seconds // 3600}h ' \
                   f'{(delta.seconds % 3600) // 60}m {delta.seconds % 60}s'
    else:
        if delta < timedelta(hours=1):
            return f'{round((delta.seconds % 3600) / 60)} mins'
        elif delta < timedelta(days=1):
            return f'{round(delta.seconds / 3600)} hours'
        else:
            return f'{delta.days + round(delta.seconds / (3600 * 24))} days'


def get_uri(path):
    return periflow_api_server + path


def get_wss_uri(path):
    return periflow_ws_server + path


def secho_error_and_exit(text: str, color=typer.colors.RED):
    typer.secho(text, err=True, fg=color)
    raise typer.Exit(1)


def get_group_id() -> int:
    r = autoauth.get(get_uri("user/group/"))
    if r.status_code != 200:
        secho_error_and_exit(
            f"Cannot acquire group info. Error Code = {r.status_code} detail = {r.text}")
    groups = r.json()["results"]
    if len(groups) == 0:
        secho_error_and_exit("You are not assigned to any group... Please contact to admin")
    if len(groups) > 1:
        secho_error_and_exit(
            "Currently we do not support users with more than two groups... Please contact admin")
    return groups[0]['id']


def cred_id_by_name(cred_name: str, cred_type: str):
    creds = []
    
    request_data = {"type": cred_type}
    r_user = autoauth.get(get_uri("credential/"), params=request_data)
    try:
        r_user.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Credential listing failed... Error Code = {r_user.status_code}, Detail = {r_user.text}")
    creds_user = r_user.json()
    for cred_user in creds_user:
        creds.append(cred_user) 
    
    group_id = get_group_id()
    r_group = autoauth.get(get_uri(f"group/{group_id}/credential/"))
    try:
        r_group.raise_for_status()
    except HTTPError:
        secho_error_and_exit(f"Credential listing failed... Error Code = {r_group.status_code}, Detail = {r_group.text}")
    creds_group = r_group.json()
    for cred_group in creds_group:
        creds.append(cred_group) 

    try:
        cred_id = next(cred["id"] for cred in creds if cred["name"] == cred_name)
        return cred_id
    except:
        secho_error_and_exit(f"Cannot find a credential with such name")