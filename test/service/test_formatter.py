# Copyright (C) 2021 FriendliAI

"""Test Client Service"""

import pytest
from rich.table import Table

from pfcli.service.formatter import (
    TableFormatter,
    get_value,
)


@pytest.fixture
def table_formatter() -> TableFormatter:
    return TableFormatter(
        name='Personal Info',
        fields=['required.name', 'email', 'age'],
        headers=['Name', 'Email', 'Age'],
        extra_fields=['job'],
        extra_headers=['Occupation'],
        caption="This table shows user's personal info"
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


def test_table_formatter(table_formatter: TableFormatter):
    data = [
        {
            'required': {
                'name': 'koo'
            },
            'email': 'koo@friendli.ai',
            'age': 26,
            'job': 'historian'
        },
        {
            'required': {
                'name': 'kim'
            },
            'email': 'kim@friendli.ai',
            'age': 28,
            'job': 'scientist'
        }
    ]
    table = table_formatter.get_renderable(data)
    assert isinstance(table, Table)
    table_formatter.render(data)
