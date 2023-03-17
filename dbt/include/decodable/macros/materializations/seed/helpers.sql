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

{% macro decodable__create_csv_table(model, agate_table) %}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set table_name = this.render() %}
  {% do adapter.create_seed_table(table_name, agate_table, column_override) %}
{% endmacro %}

{% macro decodable__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {% if full_refresh %}
    {% do adapter.drop_relation(old_relation) %}
    {% do create_csv_table(model, agate_table) %}
  {% else %}
    {% do adapter.truncate_relation(old_relation) %}
    {% do adapter.reactivate_connection(old_relation) %}
  {% endif %}
{% endmacro %}

{% macro decodable__load_csv_rows(model, agate_table) %}
  {% do adapter.send_seed_as_events(model['alias'], agate_table) %}
{% endmacro %}
