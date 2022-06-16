# Copyright (C) 2022 FriendliAI

"""PeriFlow ServeClient Service"""

from typing import List

from pfcli.service.client.base import ClientService, T, safe_request


class ServeClientService(ClientService):
    def get_serve(self, serve_id: T) -> dict:
        response = safe_request(self.retrieve, err_prefix=f"Serve ({serve_id}) is not found. You may enter wrongID.")(
            pk=serve_id
        )
        return response.json()

    def create_serve(self, config = dict) -> dict:
        response = safe_request(self.post, err_prefix="Failed to post new serve.")(
            json=config
        )
        return response.json()

    def list_serves(self) -> List[dict]:
        response = safe_request(self.list, err_prefix="Failed to list serves.")()
        return response.json()

    def delete_serve(self, serve_id: T) -> None:
        safe_request(self.delete, err_prefix="Failed to delete serve.")(
            pk=serve_id
        )
