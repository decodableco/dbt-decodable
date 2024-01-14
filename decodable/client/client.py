#
#  Copyright 2023 decodable Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

import requests
from typing_extensions import override

from decodable.client.api import Connector, ConnectionType, StartPosition
from decodable.client.types import FieldType
from decodable.config.client_config import (
    DecodableControlPlaneClientConfig,
    DecodableDataPlaneClientConfig,
)


@dataclass
class ApiResponse:
    items: List[Any]
    next_page_token: Optional[str]


@dataclass
class PreviewResponse:
    id: str
    output_stream_type: str
    results: List[Dict[str, Any]]
    next_token: str

    @classmethod
    def from_dict(cls, response: Dict[str, Any]) -> PreviewResponse:
        return cls(
            id=response["id"],
            output_stream_type=response["output_stream_type"],
            results=response["results"],
            next_token=response["next_token"],
        )


@dataclass
class AccountInfoResponse:
    subscription: SubscriptionResponse
    deployment_type: str
    data_plane_hostname: str
    uses_new_secrets: bool

    @classmethod
    def from_dict(cls, response: Dict[str, Any]) -> AccountInfoResponse:
        return cls(
            subscription=SubscriptionResponse.from_dict(response["subscription"]),
            deployment_type=response["deployment_type"],
            data_plane_hostname=response["data_plane_hostname"],
            uses_new_secrets=response["uses_new_secrets"],
        )


@dataclass
class SubscriptionResponse:
    plan_id: str
    capabilities: List[str]

    @classmethod
    def from_dict(cls, response: Dict[str, Any]) -> SubscriptionResponse:
        return cls(
            plan_id=response["plan_id"],
            capabilities=response["capabilities"],
        )


@dataclass
class PreviewTokensResponse:
    data_plane_request: str
    post_token: str
    get_token: str

    @classmethod
    def from_dict(cls, response: Dict[str, Any]) -> PreviewTokensResponse:
        return cls(
            data_plane_request=response["data_plane_request"],
            post_token=response["post_token"],
            get_token=response["get_token"],
        )


@dataclass
class DataPlaneTokenResponse:
    data_plane_request: str
    token: str

    @classmethod
    def from_dict(cls, response: Dict[str, Any]) -> DataPlaneTokenResponse:
        return cls(
            # Some requests don't include a body, so check whether it's present
            data_plane_request=response["data_plane_request"]
            if "data_plane_request" in response
            else None,
            token=response["token"],
        )


class DecodableAPIException(Exception):
    @classmethod
    def category(cls) -> str:
        return "DecodableAPIException"

    @override
    def __str__(self) -> str:
        return f"Decodable: {self.category()}: {self.args[0]}"


class InvalidRequest(DecodableAPIException):
    @classmethod
    def category(cls) -> str:
        return "InvalidRequest"


class ResourceAlreadyExists(DecodableAPIException):
    @classmethod
    def category(cls) -> str:
        return "ResourceAlreadyExists"


class ResourceNotFound(DecodableAPIException):
    @classmethod
    def category(cls) -> str:
        return "ResourceNotFound"


def raise_api_exception(code: int, reason: str):
    if code == 400:
        raise InvalidRequest(reason)
    elif code == 404:
        raise ResourceNotFound(reason)
    elif code == 409:
        raise ResourceAlreadyExists(reason)
    else:
        raise DecodableAPIException(reason)


@dataclass
class SchemaField:
    name: str
    type: FieldType

    def __hash__(self) -> int:
        res = hash(self.name)
        res ^= hash(self.type)
        return res


class DecodableDataPlaneApiClient:
    config: DecodableDataPlaneClientConfig

    def __init__(self, config: DecodableDataPlaneClientConfig):
        self.config = config

    def start_preview(self, token: str, data_plane_request: str) -> PreviewResponse:
        response = self._post_api_request(
            data=data_plane_request,
            bearer_token=token,
            endpoint_url=f"{self.config.api_url}/sql/preview",
        )
        return PreviewResponse.from_dict(response.json())

    def get_preview(self, auth_token: str, next_token: str) -> PreviewResponse:
        response = self._get_api_request(
            bearer_token=auth_token,
            endpoint_url=f"{self.config.api_url}/sql/preview",
            additional_headers={"decodable-token": next_token},
        )
        return PreviewResponse.from_dict(response.json())

    def clear_stream(self, stream_id: str, token: str) -> None:
        self._post_api_request(
            bearer_token=token, endpoint_url=f"{self.config.api_url}/streams/{stream_id}/clear"
        )

    def _get_api_request(
        self,
        bearer_token: str,
        endpoint_url: str,
        additional_headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        }
        if additional_headers is not None:
            headers.update(additional_headers)

        response = requests.get(
            url=endpoint_url,
            headers=headers,
        )

        if response.ok:
            return response
        else:
            raise_api_exception(response.status_code, response.json())

    def _post_api_request(
        self, bearer_token: str, endpoint_url: str, payload: Any = None, data: Any = None
    ) -> requests.Response:
        response = requests.post(
            url=endpoint_url,
            json=payload,
            data=data,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": f"Bearer {bearer_token}",
            },
        )

        if response.ok:
            return response
        else:
            raise_api_exception(response.status_code, response.json())


class DecodableControlPlaneApiClient:
    config: DecodableControlPlaneClientConfig

    def __init__(self, config: DecodableControlPlaneClientConfig):
        self.config = config

    def test_connection(self) -> requests.Response:
        response = requests.get(
            url=f"{self.config.decodable_api_url()}/streams",
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )
        return response

    def list_streams(self) -> ApiResponse:
        response = self._get_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/streams",
        )
        return self._parse_response(response.json())

    def get_stream_id(self, name: str) -> Optional[str]:
        streams = self.list_streams().items
        stream_id = None

        for stream in streams:
            if stream["name"] == name:
                stream_id = stream["id"]

        return stream_id

    def get_stream_information(self, stream_id: str) -> Dict[str, Any]:
        endpoint_url = f"{self.config.decodable_api_url()}/streams/{stream_id}"
        response = requests.get(
            url=endpoint_url,
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if response.ok:
            return response.json()
        else:
            raise_api_exception(response.status_code, response.json())

    def get_stream_from_sql(self, sql: str) -> Dict[str, Any]:
        payload = {"sql": sql}

        return self._post_api_request(
            payload=payload,
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/outputStream",
        ).json()

    def create_stream(
        self, name: str, schema_fields: List[SchemaField], watermark: Optional[str] = None
    ) -> ApiResponse:
        payload = {
            "schema": [{"name": field.name, "type": repr(field.type)} for field in schema_fields],
            "name": name,
        }

        if watermark:
            payload["watermark"] = watermark

        return self._post_api_request(
            payload=payload, endpoint_url=f"{self.config.decodable_api_url()}/streams"
        ).json()

    def update_stream(self, stream_id: str, props: Dict[str, Any]) -> ApiResponse:
        endpoint_url = f"{self.config.decodable_api_url()}/streams/{stream_id}"
        return self._patch_api_request(payload=props, endpoint_url=endpoint_url).json()

    def delete_stream(self, stream_id: str) -> None:
        return self._delete_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/streams/{stream_id}"
        )

    def get_clear_stream_token(self, stream_id: str) -> DataPlaneTokenResponse:
        response = self._post_api_request(
            payload={},
            endpoint_url=f"{self.config.decodable_api_url()}/streams/{stream_id}/clear/token",
        )
        return DataPlaneTokenResponse.from_dict(response.json())

    def list_pipelines(self) -> ApiResponse:
        response = self._get_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines",
        )
        return self._parse_response(response.json())

    def get_pipeline_id(self, name: str) -> Optional[str]:
        pipelines = self.list_pipelines().items
        pipeline_id = None

        for pipeline in pipelines:
            if pipeline["name"] == name:
                pipeline_id = pipeline["id"]

        return pipeline_id

    def get_pipeline_information(self, pipeline_id: str) -> Dict[str, Any]:
        endpoint_url = f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}"
        response = requests.get(
            url=endpoint_url,
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if response.ok:
            return response.json()
        else:
            raise_api_exception(response.status_code, response.json())

    def get_associated_streams(self, pipeline_id: str) -> ApiResponse:
        response = self._get_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}/streams"
        )
        return self._parse_response(response.json())

    def create_pipeline(self, sql: str, name: str, description: str) -> Dict[str, Any]:
        payload = {
            "sql": sql,
            "name": name,
            "description": description,
        }

        return self._post_api_request(
            payload=payload, endpoint_url=f"{self.config.decodable_api_url()}/pipelines"
        ).json()

    def update_pipeline(self, pipeline_id: str, props: Dict[str, Any]) -> Any:
        return self._patch_api_request(
            payload=props,
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}",
        ).json()

    def activate_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        return self._post_api_request(
            payload={},
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}/activate",
        ).json()

    def deactivate_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        return self._post_api_request(
            payload={},
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}/deactivate",
        ).json()

    def delete_pipeline(self, pipeline_id: str) -> None:
        return self._delete_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/pipelines/{pipeline_id}"
        )

    def get_preview_tokens(
        self,
        sql: str,
        preview_start: StartPosition = StartPosition.LATEST,
        input_streams: Optional[List[str]] = None,
    ) -> PreviewTokensResponse:
        if input_streams is None:
            input_streams = []
        payload = {
            "sql": sql,
            "start_positions": {
                stream: {"type": "TAG", "value": preview_start.value} for stream in input_streams
            },
        }
        response = self._post_api_request(
            payload=payload, endpoint_url=f"{self.config.decodable_api_url()}/preview/tokens"
        )
        return PreviewTokensResponse.from_dict(response.json())

    def get_preview_dependencies(self, sql: str) -> Dict[str, Any]:
        payload = {"sql": sql}

        return self._post_api_request(
            payload=payload, endpoint_url=f"{self.config.decodable_api_url()}/preview/dependencies"
        ).json()

    def list_connections(self) -> ApiResponse:
        response = self._get_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/connections",
        )
        return self._parse_response(response.json())

    def get_connection_id(self, name: str) -> Optional[str]:
        connections = self.list_connections().items
        conn_id = None

        for conn in connections:
            if conn["name"] == name:
                conn_id = conn["id"]
                break

        return conn_id

    def create_connection(
        self,
        name: str,
        schema: List[SchemaField],
        stream: Optional[str] = None,
        connector: Connector = Connector.REST,
        connection_type: ConnectionType = ConnectionType.SOURCE,
    ) -> Dict[str, Any]:
        if not stream:
            stream = name

        payload = {
            "name": name,
            "connector": connector.value,
            "type": connection_type.value,
            "schema": [{"name": field.name, "type": repr(field.type)} for field in schema],
        }

        return self._post_api_request(
            payload=payload,
            endpoint_url=f"{self.config.decodable_api_url()}/connections?stream_name={stream}",
        ).json()

    def activate_connection(self, conn_id: str) -> Dict[str, Any]:
        return self._post_api_request(
            payload={},
            endpoint_url=f"{self.config.decodable_api_url()}/connections/{conn_id}/activate",
        ).json()

    def deactivate_connection(self, conn_id: str) -> Dict[str, Any]:
        return self._post_api_request(
            payload={},
            endpoint_url=f"{self.config.decodable_api_url()}/connections/{conn_id}/deactivate",
        ).json()

    def delete_connection(self, conn_id: str):
        self._delete_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/connections/{conn_id}"
        )

    def send_events(self, id: str, events: List[Dict[str, Any]]) -> int:
        payload = {"events": events}

        response = self._post_api_request(
            payload=payload,
            endpoint_url=f"{self.config.decodable_api_url()}/connections/{id}/events",
        ).json()

        return response["count"]

    def get_account_info(self, account_name: str) -> AccountInfoResponse:
        response = self._get_api_request(
            endpoint_url=f"{self.config.decodable_api_url()}/accounts/{account_name}"
        )

        return AccountInfoResponse.from_dict(response.json())

    def _parse_response(self, result: Any) -> ApiResponse:
        return ApiResponse(items=result["items"], next_page_token=result["next_page_token"])

    def _post_api_request(self, payload: Any, endpoint_url: str) -> requests.Response:
        response = requests.post(
            url=endpoint_url,
            json=payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if response.ok:
            return response
        else:
            raise_api_exception(response.status_code, response.json())

    def _patch_api_request(self, payload: Any, endpoint_url: str) -> requests.Response:
        response = requests.patch(
            url=endpoint_url,
            json=payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if response.ok:
            return response
        else:
            raise_api_exception(response.status_code, response.json())

    def _get_api_request(self, endpoint_url: str) -> requests.Response:
        response = requests.get(
            url=endpoint_url,
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if response.ok:
            return response
        else:
            raise_api_exception(response.status_code, response.json())

    def _delete_api_request(self, endpoint_url: str) -> None:
        response = requests.delete(
            url=endpoint_url,
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.config.access_token}",
            },
        )

        if not response.ok:
            raise_api_exception(response.status_code, response.json())
