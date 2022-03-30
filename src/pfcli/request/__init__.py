# Copyright (C) 2021 FriendliAI

"""PeriFlow API Request"""

from string import Template
from typing import TypeVar, Type

from pfcli.autoauth import auto_token_refresh
from pfcli.request.job import JobAPIRequest


T = TypeVar('T', bound='APIRequest')


request_template_map = {
    JobAPIRequest: Template('')
}


class APIRequest:
    def __init__(self, url_pattern: Template):
        self.url_pattern = url_pattern

    @staticmethod
    def from_factory(cls: Type[T]) -> T:
        ...

    @auto_token_refresh
    def create():
        ...


class GroupAPIRequestMixin:
    def __init__(self, group_url_pattern: Template):
        self.group_url_pattern = group_url_pattern
