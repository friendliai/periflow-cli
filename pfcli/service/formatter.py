# Copyright (C) 2021 FriendliAI

"""PeriFlow CLI Output Formatter"""

import json
from dataclasses import dataclass, field
from typing import List, TypeVar, Union, Optional, Dict, Any

from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich.text import Text
from rich.json import JSON
from rich.filesize import decimal
from rich.console import Console, RenderableType


T = TypeVar('T', bound=Union[int, str])


def get_value(data: dict, key: str) -> T:
    value = data
    keys = key.split('.')
    for key in keys:
        value = value.get(key)

    return str(value)


@dataclass
class Formatter:
    name: str

    def __post_init__(self):
        self._console = Console()


@dataclass
class ListFormatter(Formatter):
    fields: List[str]
    headers: List[str]
    extra_fields: List[str] = field(default_factory=list)
    extra_headers: List[str] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.fields) == len(self.headers)
        assert len(self.extra_fields) == len(self.extra_headers)

        self._styling_map: Dict[str, Dict[str, Any]] = {}
        self._substitution_rule: Dict[str, str] = {}

    def render(self, data: List[dict], show_detail: bool = False) -> None:
        raise NotImplementedError   # pragma: no cover

    def get_renderable(self, data: List[dict], show_detail: bool = False) -> RenderableType:
        raise NotImplementedError   # pragma: no cover


@dataclass
class TableFormatter(ListFormatter):
    caption: Optional[str] = None

    def _init(self, show_detail: bool):
        self._table = Table(title=self.name, caption=self.caption)
        self._make_header(show_detail)

    def render(self, data: List[dict], show_detail: bool = False) -> None:
        self._build_table(data, show_detail)
        self._console.print(self._table)

    def get_renderable(self, data: List[dict], show_detail: bool = False) -> Table:
        self._build_table(data, show_detail)
        return self._table

    def _build_table(self, data: List[dict], show_detail: bool):
        self._init(show_detail)

        for d in data:
            info = [ self._substitute(get_value(d, f)) for f in self.fields ]
            if show_detail:
                info.extend([ self._substitute(get_value(d, f)) for f in self.extra_fields ])
            self._table.add_row(*info)

    def _make_header(self, show_detail: bool) -> None:
        for header in self.headers:
            self._table.add_column(header, **self._styling_map.get(header, {}))

        if show_detail:
            for extra_header in self.extra_headers:
                self._table.add_column(extra_header)

    def apply_styling(self, header: str, **kwargs) -> None:
        self._styling_map[header] = kwargs

    def add_substitution_rule(self, before: str, after: str) -> None:
        self._substitution_rule[before] = after

    def _substitute(self, val: str) -> str:
        if val in self._substitution_rule:
            return self._substitution_rule[val]
        return val


@dataclass
class PanelFormatter(ListFormatter):
    subtitle: Optional[str] = None

    def render(self, data: List[dict], show_detail: bool = False) -> None:
        self._build_panel(data, show_detail)
        self._console.print(self._panel)

    def get_renderable(self, data: List[dict], show_detail: bool = False) -> Panel:
        self._build_panel(data, show_detail)
        return self._panel

    def _build_panel(self, data: List[dict], show_detail: bool):
        headers = self.headers + self.extra_headers if show_detail else self.headers
        text = Text()

        for d in data:
            info = [ get_value(d, f) for f in self.fields ]
            if show_detail:
                info.extend([ get_value(d, f) for f in self.extra_fields ])
            text.append("\n".join(f"{k}: {v}" for k, v in zip(headers, info)))
        self._panel = Panel(text, title=self.name, subtitle=self.subtitle)


@dataclass
class Edge:
    name: str
    size: int


def find_and_insert(parent: Tree, edges: List[Edge]) -> None:
    if not edges:
        return

    is_dir = len(edges) > 1

    match = [tree for tree in parent.children if edges[0].name in tree.label]

    if match:
        tree = match[0]
    else:
        if is_dir:
            tree = parent.add(f"[bold magenta]:open_file_folder: {edges[0].name}")
        else:
            text_filename = Text(edges[0].name, "green")
            text_filename.highlight_regex(r"\..*$", "bold red")
            file_size = edges[0].size
            text_filename.append(f" ({decimal(file_size)})", "blue")
            icon = "🐍 " if edges[0].name.endswith == ".py" else "📄 "
            tree = parent.add(Text(icon) + text_filename)

    find_and_insert(tree, edges[1:])


@dataclass
class TreeFormatter(Formatter):

    def __post_init__(self):
        super().__post_init__()

    def render(self, data: List[dict]) -> None:
        self._build_tree(data)
        self._console.print(self._panel)

    def get_renderable(self, data: List[dict]) -> Panel:
        self._build_tree(data)
        return self._panel

    def _build_tree(self, data: List[dict]):
        root = Tree("/")
        paths = [ f"/{d['path']}" for d in data ]
        sizes = [ d['size'] for d in data]
        for path, size in zip(paths, sizes):
            edges = []
            parts = path.split("/")[1:]
            for i, part in enumerate(parts):
                file_size = size if i == len(parts) - 1 else None
                edges.append(Edge(part, file_size))
            find_and_insert(root, edges)
        self._panel = Panel(root, title=self.name)


@dataclass
class JSONFormatter(Formatter):
    def render(self, data: dict) -> None:
        self._build_json(data)
        self._console.print(self._panel)

    def get_renderable(self, data: List[dict]) -> Panel:
        self._build_json(data)
        return self._panel

    def _build_json(self, data: dict):
        self._panel = Panel(JSON(json.dumps(data)), title=self.name)
