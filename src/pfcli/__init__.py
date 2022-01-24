import typer

from pfcli import checkpoint
from pfcli import credential
from pfcli import job
from pfcli import datastore
from pfcli import vm

app = typer.Typer()
app.add_typer(credential.app, name="credential")
app.add_typer(job.app, name="job")
app.add_typer(checkpoint.app, name="checkpoint")
app.add_typer(datastore.app, name="datastore")
app.add_typer(vm.app, name="vm")