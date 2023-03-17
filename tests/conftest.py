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

import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")  # pyright: ignore [reportUntypedFunctionDecorator]
def dbt_profile_target():
    return {
        "type": "decodable",
        "database": "",
        "schema": "",
        "account_name": os.getenv("DECODABLE_ACCOUNT_NAME", "test_account"),
        "profile_name": os.getenv("DECODABLE_PROFILE_NAME", "test_profile"),
        "local_namespace": "functional_tests",
    }
