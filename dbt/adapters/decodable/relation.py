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
from dataclasses import dataclass

# from typing import Dict

from dbt.adapters.base import BaseRelation
from dbt.adapters.contracts.relation import Policy


@dataclass
class DecodableQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class DecodableIncludePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DecodableRelation(BaseRelation):
    # quote_policy: Policy = DecodableQuotePolicy()
    # include_policy: Policy = DecodableIncludePolicy()
    dbt_created: bool = True
    # default_quote_policy: Policy = DecodableQuotePolicy()

    # @property
    # def quote_character(self) -> str:
    #     return '"'

    # @property
    # def quote_policy(self) -> Dict[str, bool]:
    #     return {
    #         "schema": False,
    #         "database": False,
    #         "identifier": True,
    #     }

    # @property
    # def include_policy(self) -> Dict[str, bool]:
    #     return {
    #         "schema": False,
    #         "database": False,
    #         "identifier": True,
    #     }
