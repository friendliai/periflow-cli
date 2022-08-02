# Copyright (C) 2022 FriendliAI

"""PeriFlow ServeClient Service"""

from typing import List, Optional
import requests
from requests.models import Response

from pfcli.service.client.base import ClientService, T, safe_request


class ServeClientService(ClientService):
    def get_serve(self, serve_id: T) -> dict:
        response = safe_request(self.retrieve_admin, err_prefix=f"Serve ({serve_id}) is not found. You may enter wrongID.")(
            pk=serve_id
        )
        return response.json()

    def create_serve(self, config = dict) -> dict:
        response = safe_request(self.post_admin, err_prefix="Failed to post new serve.")(
            json=config
        )
        return response.json()

    def list_serves(self) -> List[dict]:
        response = safe_request(self.get_admin, err_prefix="Failed to list serves.")()
        return response.json()

    def delete_serve(self, serve_id: T) -> None:
        safe_request(self.delete_admin, err_prefix="Failed to delete serve.")(
            pk=serve_id
        )

    def post_admin(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.post(
            self.url_template.render(path=path, **self.url_kwargs),
            headers={"Authorization": "Bearer admin"},
            **kwargs
        )

    def get_admin(self, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
            self.url_template.render(path=path, **self.url_kwargs),
            headers={"Authorization": "Bearer admin"},
            **kwargs
        )
    
    def retrieve_admin(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.get(
        self.url_template.render(pk=pk, path=path, **self.url_kwargs),
        headers={"Authorization": "Bearer admin"},
        **kwargs
    )

    def delete_admin(self, pk: T, path: Optional[str] = None, **kwargs) -> Response:
        return requests.delete(
            self.url_template.render(pk=pk, path=path, **self.url_kwargs),
            headers={"Authorization": "Bearer admin"},
            **kwargs
        )