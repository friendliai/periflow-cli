# Copyright (C) 2021 FriendliAI

from datetime import datetime, timedelta
from functools import reduce
from typing import Optional

import tabulate
import typer

from pfcli.service import ServiceType
from pfcli.service.client import build_client
from pfcli.service.client.billing import BillingClientService, TimeGranularity
from pfcli.service.formatter import PanelFormatter, TableFormatter
from pfcli.utils import datetime_to_simple_string, secho_error_and_exit, utc_to_local


tabulate.PRESERVE_WHITESPACE = True

app = typer.Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=False,
)

table_formatter = TableFormatter(
    name="Billing",
    fields=["start_time", "end_time", "price"],
    headers=["Start Time", "End Time", "Amount used in USD"],
)

panel_formatter = PanelFormatter(
    name="Total", fields=["price"], headers=["Total amount used in USD"]
)


@app.command(help="summarize billing information")
def summary(
    year: int = typer.Argument(...),
    month: int = typer.Argument(...),
    day: Optional[int] = typer.Argument(None),
    view_project: bool = typer.Option(
        False, "--project", "-p", help="View project-level cost summary."
    ),
    view_organization: bool = typer.Option(
        False, "--organization", "-o", help="View organization-level cost summary."
    ),
    time_granularity: Optional[TimeGranularity] = typer.Option(
        None, "--time-granularity", "-t", help="View within the given time granularity."
    ),
):
    """Summarize the billing information for the given time range
    """
    client: BillingClientService = build_client(ServiceType.BILLING_SUMMARY)

    agg_by = "user_id"

    if view_project:
        agg_by = "project_id"

    if view_organization:
        agg_by = "organization_id"

    try:
        if day is None:
            start_date = datetime(year, month, 1).astimezone()
            end_date = (datetime(year, (month + 1) % 12, 1) - timedelta(days=1)).astimezone()
        else:
            start_date = datetime(year, month, day).astimezone()
            end_date = datetime(year, month, day).astimezone()
    except ValueError as exc:
        secho_error_and_exit(f"Failed to parse datetime: {exc}")

    prices = client.list_prices(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        agg_by=agg_by,
        time_granularity=time_granularity
    )

    total_price = 0
    for price_info in prices:
        price_info["start_time"] = datetime_to_simple_string(
            utc_to_local(datetime.strptime(price_info["start_time"], "%Y-%m-%dT%H:%M:%SZ"))
        )
        price_info["end_time"] = datetime_to_simple_string(
            utc_to_local(datetime.strptime(price_info["end_time"], "%Y-%m-%dT%H:%M:%SZ"))
        )
        aggregated_price = reduce(lambda acc, x: acc + x["price"], price_info["price_list"], 0.0)
        price_info["price"] = round(aggregated_price, 2)
        total_price += aggregated_price

    table_formatter.render(prices)
    panel_formatter.render([{"price": round(total_price, 2)}])
