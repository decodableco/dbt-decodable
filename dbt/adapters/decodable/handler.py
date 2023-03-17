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
from random import randint
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from dbt.events import AdapterLogger

from decodable.client.client import DecodableApiClient
from decodable.client.api import StartPosition


def exponential_backoff(timeout: float) -> Iterator[float]:
    epsilon = 0.001
    backoff: float = 1.0
    total_time: float = 0
    while True:
        yield total_time
        stagger = randint(0, 1000) / 1000
        time = min(backoff + stagger, timeout - total_time)
        sleep(time)
        total_time += time
        backoff *= 2

        if timeout - total_time < epsilon:
            break


class DecodableCursor:
    logger = AdapterLogger("Decodable")

    def __init__(self, client: DecodableApiClient, preview_start: StartPosition, timeout: float):
        self.logger.debug(
            f"Creating new cursor(preview_start: {preview_start}, timeout: {timeout})"
        )
        self.client = client
        self.preview_start = preview_start
        self.timeout = timeout
        self.last_sql: Optional[str] = None
        self.last_result: Optional[Sequence[Dict[str, Any]]]

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        self.logger.debug(f"Execute(sql): {sql}")
        inputs: List[Dict[str, Any]] = self.client.get_preview_dependencies(sql)["inputs"]
        input_streams: List[str] = [i["resourceName"] for i in inputs]
        response = self.client.create_preview(sql, self.preview_start, input_streams)
        self.logger.debug(f"Create preview response: {response}")

        append_stream = response.output_stream_type == "APPEND"
        self.last_result = []

        for _ in exponential_backoff(self.timeout):
            token = response.next_token
            response = self.client.run_preview(id=response.id, token=token)
            self.logger.debug(f"Run preview response: {response}")

            if append_stream:
                self.last_result.extend(response.results)
            elif response.results:
                last_change = response.results[-1]
                if not last_change["after"]:
                    self.last_result = []
                else:
                    self.last_result = [last_change["after"]]

            if response.next_token is None:
                break

        if not self.last_result:
            self.seed_fake_results()

    def fetchall(self) -> Sequence[Tuple[Any]]:
        results = self.last_result
        self.last_result = None

        tuples: Sequence[Tuple[Any]] = []
        if results:
            l = []
            for result in results:
                for val in result.values():
                    l.append(val)
                tuples.append(tuple(l))

        return tuples

    @property
    def description(self) -> List[Tuple[str]]:
        result: List[Tuple[str]] = []

        if not self.last_result:
            return [("failures",), ("should_warn",), ("should_error",)]

        for name in self.last_result[0].keys():
            result.append((name,))
        return result

    def seed_fake_results(self):
        self.last_result = [{"failures": 0, "should_warn": False, "should_error": False}]


class DecodableHandler:
    def __init__(self, client: DecodableApiClient, preview_start: StartPosition, timeout: float):
        self.client = client
        self.preview_start = preview_start
        self.timeout = timeout

    def cursor(self) -> DecodableCursor:
        return DecodableCursor(self.client, self.preview_start, self.timeout)
