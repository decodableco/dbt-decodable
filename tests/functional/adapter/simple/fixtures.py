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

# seeds/my_seed.csv
my_seed_csv = """
name,age
Adam,31
George,27
Lily,59
""".lstrip()

# models/my_model.sql
my_model_sql = """
select CHAR_LENGTH(name) as name_length from {{ ref('my_seed') }}
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: name_length
        tests:
          - not_null
"""
