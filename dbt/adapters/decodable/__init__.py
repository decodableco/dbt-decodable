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
from dbt.adapters.decodable.connections import DecodableAdapterCredentials
from dbt.adapters.decodable.impl import DecodableAdapter

from dbt.adapters.base import AdapterPlugin
import dbt.include.decodable as decodable


Plugin = AdapterPlugin(
    adapter=DecodableAdapter,
    credentials=DecodableAdapterCredentials,
    include_path=decodable.PACKAGE_PATH,
)
