import typer
import requests
import yaml
from pathlib import Path

from utils import get_uri
import autoauth


app = typer.Typer()


@auto_token_refresh
def try_list(cred_type: str):
    request_data = {"type": cred_type}
    r = requests.get(get_uri("credential/"),
                     data=request_data,
                     headers=get_auth_header())
    return r


@auto_token_refresh
def try_update(cred_id: str,
               cred_type: str,
               name: str,
               yaml_path: str,
               type_version: int):
    request_data = {
        "type": cred_type,
        "name": name,
        "type_version": type_version,
        "value": yaml.safe_load(yaml_path.open(mode="r"))
    }

    r = requests.patch(get_uri(f"credential/{cred_id}/"), data=request_data)
    return r


@auto_token_refresh
def try_delete(cred_id: str):
    r = requests.delete(get_uri(f"credential/{cred_id}/"))
    return r


@app.command()
def create(cred_type: str = typer.Option(...),
           name: str = typer.Option(...),
           yaml_path: str = typer.Option(...),
           type_version: int = typer.Option(0)):
    request_data = {
        "type": cred_type,
        "name": name,
        "type_version": type_version
    }
    yaml_path = Path(yaml_path)
    value = yaml.safe_load(yaml_path.open(mode="r"))
    request_data.update({"value": value})

    r = autoauth.post(get_uri("credential/"),
                      json=request_data)
    if r.status_code == 200:
        typer.echo(f"Credential registered... ID = {r.json()['id']}")
    else:
        typer.secho(f"Credential register failed... Code = {r.status_code}, Msg = {r.text}",
                    err=True,
                    color=typer.colors.RED)


@app.command()
def list(cred_type: str = typer.Option(...)):
    r = try_list(cred_type)
    if r.status_code == 200:
        typer.echo(r.json())
    else:
        typer.echo(f"Credential listing failed... Code = {r.status_code}, Msg = {r.text}")


@app.command()
def update(cred_id: str = typer.Option(...),
           cred_type: str = typer.Option(...),
           name: str = typer.Option(...),
           yaml_path: str = typer.Option(...),
           type_version: int = typer.Option(0)
           ):
    r = try_update(cred_id, cred_type, name, yaml_path, type_version)
    # TODO: Elaborate
    typer.echo(r.json())


@app.command()
def delete(cred_id: str = typer.Option(...)):
    r = try_delete(cred_id)
    # TODO: Elaborate
    typer.echo(r.json())


if __name__ == '__main__':
    app()
