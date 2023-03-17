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
from unittest import mock
from decodable.config.profile import DecodableAccessTokens
from decodable.config.profile_reader import DecodableProfileReader, PROFILE_ENV_VARIABLE_NAME

TEST_PROFILE_NAME = "default"
TEST_PROFILE_ACCESS_TOKEN = "yyy"


class TestProfileAdapter:
    """Test getting profile name from env variable"""

    @mock.patch.dict(os.environ, {PROFILE_ENV_VARIABLE_NAME: "test"})
    def test_get_profile_name(self):
        assert DecodableProfileReader.get_profile_name(profile_name=None) == "test"
        assert DecodableProfileReader.get_profile_name(profile_name="default") == "default"

    """Test loading default profile"""

    def test_load_default_profile(self):
        test_profile: DecodableAccessTokens = DecodableProfileReader.load_profiles(
            f"{os.path.dirname(__file__)}/test_profile.yml"
        )
        assert test_profile.profile_tokens[TEST_PROFILE_NAME] == TEST_PROFILE_ACCESS_TOKEN
