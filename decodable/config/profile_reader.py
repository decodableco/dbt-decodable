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
import os
from pathlib import Path
from yaml import SafeLoader, load
from typing import Optional

from decodable.config.profile import DecodableAccessTokens

DEFAULT_PROFILE_PATH = f"{str(Path.home())}/.decodable/auth"
PROFILE_ENV_VARIABLE_NAME = "DECODABLE_PROFILE"


class DecodableProfileReader:
    @staticmethod
    def load_profiles(default_profile_path: str = DEFAULT_PROFILE_PATH) -> DecodableAccessTokens:
        profiles_path = Path(default_profile_path)
        if profiles_path.is_file() is False:
            raise Exception(
                f"No decodable profile under path: {profiles_path}. Execute 'decodable login' command first"
            )

        with open(profiles_path, "r") as file:
            content = file.read()
            return DecodableProfileReader._load_profile_access_tokens(content)

    @staticmethod
    def get_profile_name(profile_name: Optional[str]) -> Optional[str]:
        if profile_name is not None:
            return profile_name
        else:
            return os.getenv(PROFILE_ENV_VARIABLE_NAME)

    @staticmethod
    def _load_profile_access_tokens(yaml: str) -> DecodableAccessTokens:
        config_data = load(yaml, Loader=SafeLoader)
        access_tokens = {}
        for profile_name in config_data["tokens"]:
            access_tokens[profile_name] = config_data["tokens"][profile_name]["access_token"]
        return DecodableAccessTokens(profile_tokens=access_tokens)
