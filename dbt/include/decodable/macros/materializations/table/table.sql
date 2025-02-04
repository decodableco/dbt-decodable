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

{% materialization table, adapter='decodable' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set should_create = {'value': false} %}
  {% if existing_relation is not none %}
    {% set output_stream = config.get('output_stream', {}) %}
    {% set pipeline = config.get('pipeline', {}) %}
    {% if adapter.has_changed(sql, target_relation, pipeline, output_stream) or should_full_refresh() %}
      {{ adapter.drop_relation(existing_relation) }}
      {% do should_create.update({'value': true}) %}
    {% endif %}
  {% else %}
    {% do should_create.update({'value': true}) %}
  {% endif %}

  {{ log('Changes detected since the last materialization: ' ~ should_create['value']) }}

  {% call statement('main') -%}
    {% if should_create['value'] %}
      {{ create_table_as(False, target_relation, sql) }}
    {% else %}
      -- noop
    {% endif %}
  {%- endcall %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
