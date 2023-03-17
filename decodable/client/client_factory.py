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
from typing import Optional

from decodable.client.client import DecodableApiClient
from decodable.config.client_config import DecodableClientConfig
from decodable.config.profile_reader import DecodableProfileReader


class DecodableClientFactory:
    @staticmethod
    def create_client(
        api_url: str,
        profile_name: Optional[str] = "default",
        decodable_account_name: Optional[str] = None,
    ) -> DecodableApiClient:
        if decodable_account_name is None:
            raise Exception("Undefined Decodable account name. Update DBT profile")
        profile_access_tokens = DecodableProfileReader.load_profiles()
        if profile_name not in profile_access_tokens.profile_tokens:
            raise Exception(
                f"Undefined '{profile_name} in decodable profile file ~/.decodable/auth"
            )
        access_token = profile_access_tokens.profile_tokens[profile_name]
        return DecodableApiClient(
            config=DecodableClientConfig(
                access_token=access_token, account_name=decodable_account_name, api_url=api_url
            )
        )
