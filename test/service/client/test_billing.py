# Copyright (C) 2021 FriendliAI

from copy import deepcopy
from datetime import datetime, timedelta

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.billing import BillingClientService


@pytest.fixture
def billing_summary_client(user_project_group_context) -> BillingClientService:
    return build_client(ServiceType.PFT_BILLING_SUMMARY)


@pytest.mark.usefixtures("patch_auto_token_refresh")
def test_billing_summary_client_get_summary(
    requests_mock: requests_mock.Mocker, billing_summary_client: BillingClientService
):
    assert isinstance(billing_summary_client, BillingClientService)

    url_template = deepcopy(billing_summary_client.url_template)

    now = datetime.now()
    ten_days_after = now + timedelta(days=10)
    # Success
    requests_mock.get(
        url_template.render(),
        json={
            "results": [
                {
                    "start_time": now.isoformat(),
                    "end_time": ten_days_after.isoformat(),
                    "price_list": [{"agg_unit": "user_id", "id": "some_id", "price": 1.557}],
                }
            ],
        },
    )
    assert billing_summary_client.list_prices(start_date=now.isoformat(),
                                              end_date=ten_days_after.isoformat())

    # Failed due to HTTP error
    requests_mock.get(url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        assert billing_summary_client.list_prices(
            start_date=now.isoformat(),
            end_date=ten_days_after.isoformat()
        )
