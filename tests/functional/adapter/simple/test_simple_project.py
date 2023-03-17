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

from typing import Any, List
import pytest
from dbt.tests.util import run_dbt  # pyright: ignore [reportMissingImports]

from fixtures import (
    my_seed_csv,
    my_model_sql,
    my_model_yml,
)


class TestSimpleProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "simple", "models": {"+materialized": "table"}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "my_seed.csv": my_seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
        }

    def test_run_seed_test(self, project: Any):
        """
        Seed, then run, then test. Perform cleanup at the end,
        so that nothing persists on Decodbale.
        """

        # seed seeds
        results: List[Any] = run_dbt(["seed"])
        assert len(results) == 1

        # run models
        results = run_dbt(["run"])
        assert len(results) == 1

        # test tests
        results = run_dbt(["test"])
        assert len(results) == 1

        # validate that the results include one pass and one failure
        assert results[0].status == "pass"

        # Decodable cleanup
        run_dbt(["run-operation", "cleanup"])
