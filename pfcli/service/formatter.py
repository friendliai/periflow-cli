# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI Output Formatter"""

from dataclasses import dataclass, field
from typing import List, TypeVar, Union

import tabulate


T = TypeVar('T', bound=Union[int, str])


@dataclass
class Formatter:
    fields: List[str]
    headers: List[str]
    extra_fields: List[str] = field(default_factory=list)
    extra_headers: List[str] = field(default_factory=list)

    def __post_init__(self):
        assert len(self.fields) == len(self.headers)
        assert len(self.extra_fields) == len(self.extra_headers)

    def render(self, data: List[dict], show_detail: bool = False, in_list: bool = False) -> str:
        raise NotImplementedError   # pragma: no cover


def get_value(data: dict, key: str) -> T:
    value = data        
    keys = key.split('.')
    for key in keys:
        value = value.get(key)

    return value


@dataclass
class TableFormatter(Formatter):
    def render(self, data: List[dict], show_detail: bool = False, in_list: bool = False) -> str:
        results = []
        for d in data:
            if isinstance(d, list):
                results.append(d)
                continue
            info = [ get_value(d, f) for f in self.fields ]
            if show_detail:
                info.extend([ get_value(d, f) for f in self.extra_fields ])
            results.append(info)

        headers = self.headers + self.extra_headers if show_detail else self.headers

        if in_list:
            for result in results:
                return "\n".join(f"{k}: {v}" for k, v in zip(headers, result))

        return tabulate.tabulate(results, headers=headers)
