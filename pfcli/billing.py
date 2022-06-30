# Copyright (C) 2021 FriendliAI

from collections import defaultdict
from functools import reduce
from typing import Optional
from uuid import UUID

import tabulate
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.billing import BillingClientService
from pfcli.context import get_current_group_id, get_current_project_id
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import secho_error_and_exit


tabulate.PRESERVE_WHITESPACE = True

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)

template_app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False
)
app.add_typer(template_app, name="template", help="Manager job templates.")

table_formatter = TableFormatter(
    name="Billing",
    fields = ['instance.attributes.vm_name', 'price'],
    headers = ['VM Name', 'Amount used in USD']
)

panel_formatter = PanelFormatter(
    name="Billing",
    fields = ['vm_name', 'price'],
    headers = ['VM Name', 'Amount used in USD']
)

@app.command(help="summarize billing information")
def summary(
    year: int = typer.Argument(
        ...
    ),
    month: int = typer.Argument(
        ...
    ),
    day: Optional[int] = typer.Argument(
        None
    ),
    view_organization: bool = typer.Option(
        False,
        '--organization',
        '-o'
    )
):
    "Summarize the billing information for the given time range"
    client: BillingClientService = build_client(ServiceType.BILLING_SUMMARY)
    group_id = None
    project_id = None
    if not view_organization:
        project_id = get_current_project_id()
        if project_id is None:
            secho_error_and_exit("Project is not set!")
    else:
        group_id = get_current_group_id()
        if group_id is None:
            secho_error_and_exit("Organization is not set!")

    prices = client.list_prices(year=year,
                                month=month,
                                day=day,
                                group_id=group_id,
                                project_id=project_id)

    price_by_vmname = defaultdict(float)
    for price_info in prices:
        price_by_vmname[price_info['attributes']['vm_name']] += price_info['price']
    table_formatter.render([{"VM Name": agg_price_info.key, "price": round(agg_price_info.value, 2)} for agg_price_info in price_by_vmname])

    price_sum = reduce((lambda acc, x: acc + x), price_by_vmname.values(), 0.)

    total_price = [{"name": "Total", "price": round(price_sum, 2)}]
    panel_formatter.render(total_price)
