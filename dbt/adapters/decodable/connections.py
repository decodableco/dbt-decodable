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
from typing import Any, Optional, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

from agate.table import Table
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt.contracts.connection import (
    AdapterResponse,
    Connection,
    Credentials,
)
from dbt.events import AdapterLogger
from dbt.exceptions import RuntimeException

from dbt.adapters.decodable.handler import DecodableCursor, DecodableHandler
from decodable.client.client_factory import DecodableClientFactory
from decodable.client.api import StartPosition


@dataclass
class DecodableAdapterCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    profile_name: str
    account_name: str
    materialize_tests: bool = False
    preview_start: StartPosition = StartPosition.EARLIEST
    request_timeout_ms: int = 60000
    local_namespace: Optional[str] = None

    api_url: str = "api.decodable.co/v1alpha2"

    _ALIASES = {
        "profile": "profile_name",
        "account": "account_name",
        "request_timeout": "request_timeout_ms",
        "timeout_ms": "request_timeout_ms",
        "timeout": "request_timeout_ms",
    }

    @property
    def unique_field(self) -> str:
        return f"{self.account_name}.{self.profile_name}"

    @property
    def type(self):
        """Return name of adapter."""
        return "decodable"

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return (
            "profile_name",
            "account_name",
            "materialize_tests",
            "preview_start",
            "request_timeout_ms",
            "local_namespace",
        )


class DecodableAdapterConnectionManager(SQLConnectionManager):
    TYPE: str = "decodable"

    logger = AdapterLogger("Decodable")

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        """Get the status of the cursor."""
        return AdapterResponse("OK")

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield
        except Exception as e:
            self.logger.error("Exception thrown during execution: {}".format(str(e)))
            raise RuntimeException(str(e))

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if not connection.credentials:
            raise RuntimeException("Cannot open a Decodable connection without credentials")

        credentials: DecodableAdapterCredentials = connection.credentials
        client = DecodableClientFactory.create_client(
            api_url=credentials.api_url,
            profile_name=credentials.profile_name,
            decodable_account_name=credentials.account_name,
        )

        decodable_connection_test = client.test_connection()
        if not decodable_connection_test.ok:
            error_message = ""
            if (
                decodable_connection_test.reason is not None
                and len(decodable_connection_test.reason) > 0
            ):
                error_message = f"\nReason: {decodable_connection_test.reason}"
            raise RuntimeException(
                f"Status code: {decodable_connection_test.status_code}. Decodable connection failed. Try running 'decodable login' first{error_message}"
            )

        connection.handle = DecodableHandler(
            client, credentials.preview_start, credentials.request_timeout_ms / 1000
        )
        return connection

    def cancel(self, connection: Connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        pass

    def begin(self) -> Connection:
        return self.get_thread_connection()

    def commit(self) -> Connection:
        return self.get_thread_connection()

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, Table]:
        sql = self._add_query_comment(sql)
        if fetch:
            _, cursor = self.add_query(sql, auto_begin)
            response = self.get_response(cursor)
            table = self.get_result_from_cursor(cursor)
        else:
            response = AdapterResponse("OK")
            cursor = self._dummy_cursor()
            cursor.seed_fake_results()
            table = self.get_result_from_cursor(cursor)
        return response, table

    def _dummy_cursor(self) -> DecodableCursor:
        conn = self.get_thread_connection()
        return (
            conn.handle.cursor()  # pyright: ignore [reportOptionalMemberAccess, reportGeneralTypeIssues]
        )
