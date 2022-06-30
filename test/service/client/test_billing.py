import uuid
from copy import deepcopy

import pytest
import requests_mock
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.billing import BillingSummaryClientService


@pytest.fixture
def billing_summary_client() -> BillingSummaryClientService:
    return build_client(ServiceType.BILLING_SUMMARY)


@pytest.mark.usefixtures('patch_auto_token_refresh')
def test_billing_summary_client_get_summary(requests_mock: requests_mock.Mocker,
                                            billing_summary_client: BillingSummaryClientService):
    assert isinstance(billing_summary_client, BillingSummaryClientService)

    url_template = deepcopy(billing_summary_client.url_template)

    # Success
    requests_mock.get(url_template.render(), json=[
        {
            "instance": {
                "id": "some_id",
                "attributes": {
                    "vm_name": "vm_1"
                }
            },
            "price": 1.557
        }
    ])
    assert billing_summary_client.get_summary(group_id=uuid.uuid4())

    # Failed due to HTTP error
    requests_mock.get(url_template.render(), status_code=404)
    with pytest.raises(typer.Exit):
        assert billing_summary_client.get_summary(group_id=uuid.uuid4())
