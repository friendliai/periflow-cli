from functools import reduce
from typing import Optional
from datetime import datetime

import typer
import tabulate
from pfcli.service import ServiceType
from pfcli.service.client import build_client

from pfcli.service.client.billing import BillingSummaryClientService
from pfcli.service.formatter import PanelFormatter, TableFormatter

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
    organization_id: Optional[str] = typer.Argument(
        None,
        '--organization',
        '-o'
    ),
    project_id: Optional[str] = typer.Argument(
        None,
        '--project',
        '-p'
    ),
    year: Optional[int] = typer.Argument(
        None,
        '--year',
        '-y'
    ),
    month: Optional[int] = typer.Argument(
        None,
        '--month',
        '-m'
    ),
    day: Optional[int] = typer.Argument(
        None,
        '--day',
        '-d'
    )
):
    "Summarize the billing information for the given time range"
    client: BillingSummaryClientService = build_client(ServiceType.BILLING_SUMMARY)
    summaries = client.get_summary(year=year,
                                   month=month,
                                   day=day,
                                   group_id=organization_id,
                                   project_id=project_id)
    table_formatter.render(summaries)

    price_sum = reduce((lambda x, y: x["price"] + y["price"]), summaries)

    total_price = [{"name": "Total", "price": price_sum}]
    panel_formatter.render(total_price)
