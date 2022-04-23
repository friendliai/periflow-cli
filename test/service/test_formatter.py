# Copyright (C) 2021 FriendliAI

"""Test Client Service"""

import pytest
from rich.table import Table

from pfcli.service.formatter import (
    PanelFormatter,
    TableFormatter,
    get_value,
)


@pytest.fixture
def table_formatter() -> TableFormatter:
    return TableFormatter(
        name='Personal Info',
        fields=['required.name', 'email', 'age'],
        headers=['Name', 'Email', 'Age'],
        extra_fields=['job', 'active'],
        extra_headers=['Occupation', 'Active'],
        caption="This table shows user's personal info"
    )


@pytest.fixture
def panel_formatter() -> PanelFormatter:
    return PanelFormatter(
        name='Personal Info',
        fields=['required.name', 'email', 'age'],
        headers=['Name', 'Email', 'Age'],
        extra_fields=['job', 'active'],
        extra_headers=['Occupation', 'Active'],
        subtitle="This table shows user's personal info"
    )


def test_get_value():
    data = {
        'k1': {
            'k2': {
                'k3': 'v1',
                'k4': 'v2'
            },
            'k5': 'v3'
        },
        'k6': 'v4'
    }

    assert get_value(data, 'k1.k2.k3') == 'v1'
    assert get_value(data, 'k1.k2.k4') == 'v2'
    assert get_value(data, 'k1.k5') == 'v3'
    assert get_value(data, 'k6') == 'v4'


def test_table_formatter(table_formatter: TableFormatter, capsys: pytest.CaptureFixture):
    data = [
        {
            'required': {
                'name': 'koo'
            },
            'email': 'koo@friendli.ai',
            'age': 26,
            'job': 'historian',
            'active': True
        },
        {
            'required': {
                'name': 'kim'
            },
            'email': 'kim@friendli.ai',
            'age': 28,
            'job': 'scientist',
            'active': False
        }
    ]
    table = table_formatter.get_renderable(data)
    assert isinstance(table, Table)
    table_formatter.render(data)
    out = capsys.readouterr().out
    assert 'Active' not in out
    assert 'Occupation' not in out

    table_formatter.add_substitution_rule('True', 'Yes')
    table_formatter.add_substitution_rule('False', 'No')
    table_formatter.apply_styling('Active', style='blue')
    table = table_formatter.get_renderable(data, show_detail=True)
    assert isinstance(table, Table)
    table_formatter.render(data, show_detail=True)
    out = capsys.readouterr().out
    assert 'Active' in out
    assert 'Occupation' in out
    assert 'Yes' in out
    assert 'No' in out


def test_panel_formatter():
    ...
