/*
 *  Copyright 2023 decodable Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

{% macro decodable__create_table_as(temporary, relation, sql) -%}
  {% set watermarks = config.get('watermarks', []) %}
  {% set primary_key = config.get('primary_key', []) %}
  {% do adapter.create_table(sql, temporary, relation, graph.nodes, watermarks, primary_key) %}
{%- endmacro %}
