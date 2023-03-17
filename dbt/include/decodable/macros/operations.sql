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

{% macro stop_pipelines(pipelines=none) -%}
  {% do count_resources() %}

  {% for key, value in graph['nodes'].items() %}
    {% set relation = api.Relation.create(database=value['database'], schema=value['schema'], identifier=value['alias']) -%}
    {% set resource_type = value['resource_type'] %}
    {% set is_materialized_test = adapter.should_materialize_tests() and resource_type == 'test' %}
    {% set has_associated_pipe = resource_type == 'model' or is_materialized_test %}
    {% set should_include = pipelines is none or relation.render() in pipelines %}

    {% if has_associated_pipe and should_include %}
      {% set cached_relation = load_cached_relation(relation) %}
      {% if cached_relation is not none %}
        {{ log("Stopping pipeline '" ~ cached_relation.render() ~ "'...", true) }}
        {% do adapter.stop_pipeline(cached_relation) %}
      {% endif %}
    {% endif %}
  {% endfor %}
{%- endmacro %}

{% macro delete_pipelines(pipelines=none) -%}
  {% do count_resources() %}

  {% for key, value in graph['nodes'].items() %}
    {% set relation = api.Relation.create(database=value['database'], schema=value['schema'], identifier=value['alias']) -%}
    {% set resource_type = value['resource_type'] %}
    {% set is_materialized_test = adapter.should_materialize_tests() and resource_type == 'test' %}
    {% set has_associated_pipe = resource_type == 'model' or is_materialized_test %}
    {% set should_include = pipelines is none or relation.render() in pipelines %}

    {% if has_associated_pipe and should_include %}
      {% set cached_relation = load_cached_relation(relation) %}
      {% if cached_relation is not none %}
        {{ log("Deleting pipeline '" ~ cached_relation.render() ~ "'...", true) }}
        {% do adapter.delete_pipeline(cached_relation) %}
      {% endif %}
    {% endif %}
  {% endfor %}
{%- endmacro %}

{% macro delete_streams(streams=none, skip_errors=true) -%}
  {% do count_resources() %}

  {% for key, value in graph['nodes'].items() %}
    {% set relation = api.Relation.create(database=value['database'], schema=value['schema'], identifier=value['alias']) -%}
    {% set resource_type = value['resource_type'] %}
    {% set is_materialized_test = adapter.should_materialize_tests() and resource_type == 'test' %}
    {% set has_associated_stream = resource_type == 'model' or resource_type == 'seed' or is_materialized_test %}
    {% set should_include = streams is none or relation.render() in streams %}

    {% if has_associated_stream and should_include %}
      {% set cached_relation = load_cached_relation(relation) %}
      {% if cached_relation is not none %}
        {{ log("Deleting stream '" ~ cached_relation.render() ~ "'...", true) }}
        {% do adapter.delete_stream(cached_relation, skip_errors) %}
      {% endif %}
    {% endif %}
  {% endfor %}
{%- endmacro %}

{% macro cleanup(list=none, seeds=true, models=true, tests=true) -%}
  {% do count_resources() %}
  {% set removed = {'models': 0, 'tests': 0, 'seeds': 0} %}

  {% for key, value in graph['nodes'].items() %}
    {% set relation = api.Relation.create(database=value['database'], schema=value['schema'], identifier=value['alias']) -%}
    {% set resource_type = value['resource_type'] %}
    {% set is_materialized_test = adapter.should_materialize_tests() and resource_type == 'test' %}
    {% set passes_filter = list is none or relation.render() in list %}

    {% if passes_filter %}
      {% set cached_relation = load_cached_relation(relation) %}
      {% if cached_relation is not none %}
        {% if resource_type == 'model' and models == true %}
          {{ log("Cleaning up model '" ~ cached_relation.render() ~ "'...", true) }}
          {% do adapter.drop_relation(cached_relation) %}
          {% do removed.update({'models': removed['models'] + 1}) %}
        {% elif resource_type == 'seed' and seeds == true %}
          {{ log("Cleaning up seed '" ~ cached_relation.render() ~ "'...", true) }}
          {% do adapter.delete_connection(cached_relation) %}
          {% do adapter.delete_stream(cached_relation) %}
          {% do removed.update({'seeds': removed['seeds'] + 1}) %}
        {% elif is_materialized_test and tests == true %}
          {{ log("Cleaning up materialized test '" ~ cached_relation.render() ~ "'...", true) }}
          {% do adapter.delete_pipeline(cached_relation) %}
          {% do adapter.delete_stream(cached_relation) %}
          {% do removed.update({'tests': removed['tests'] + 1}) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% set total = removed['models'] + removed['seeds'] + removed['tests'] %}
  {{ log("Removed " ~ total ~ " resources: " ~ removed['models'] ~ " models, " ~ removed['tests'] ~ " tests, " ~ removed['seeds'] ~ " seeds", true) }}
{%- endmacro %}

{% macro count_resources() %}
  {% set resources = {'models': 0, 'tests': 0, 'seeds': 0} %}
  {% for key, value in graph['nodes'].items() %}
    {% set resource_type = value['resource_type'] %}
    {% if resource_type == 'model' %}
      {% do resources.update({'models': resources['models'] + 1}) %}
    {% elif resource_type == 'seed' %}
      {% do resources.update({'seeds': resources['seeds'] + 1}) %}
    {% elif resource_type == 'test' %}
      {% do resources.update({'tests': resources['tests'] + 1}) %}
    {% endif %}
  {% endfor %}
  {% set total = resources['models'] + resources['seeds'] + resources['tests'] %}
  {{ log("Found a total of " ~ total ~ " resources: " ~ resources['models'] ~ " models, " ~ resources['tests'] ~ " tests, " ~ resources['seeds'] ~ " seeds", true) }}
{% endmacro %}
