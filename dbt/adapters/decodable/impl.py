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

from dataclasses import dataclass, field as dataclass_field
from typing import Any, ContextManager, Dict, Hashable, List, Optional, Set, Type, Sequence, Union

from agate.table import Table as AgateTable
from dbt.adapters.base import BaseAdapter, BaseRelation, Column
from dbt.adapters.base.meta import available
from dbt.adapters.protocol import AdapterConfig
from dbt.contracts.connection import Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedNode
from dbt.contracts.relation import RelationType
from dbt.events import AdapterLogger
from dbt.exceptions import (
    NotImplementedException,
    raise_compiler_error,
    raise_database_error,
)

from dbt.adapters.decodable.connections import (
    DecodableAdapterConnectionManager,
    DecodableAdapterCredentials,
)
from dbt.adapters.decodable.handler import DecodableHandler
from dbt.adapters.decodable.relation import DecodableRelation
from decodable.client.client import (
    DecodableControlPlaneApiClient,
    DecodableDataPlaneApiClient,
)
from decodable.client.types import (
    FieldType,
    String,
    Boolean,
    TimestampLocal,
    Date,
    Time,
    Decimal,
)
from decodable.client.api import (
    StreamStartPositions
)

from decodable.client.schema import SchemaV2, SchemaField, PhysicalSchemaField, Constraints


@dataclass
class DecodableConfig(AdapterConfig):
    output_stream: Dict[str, Any] = dataclass_field(default_factory=dict)
    pipeline: Dict[str, Any] = dataclass_field(default_factory=dict)


class DecodableAdapter(BaseAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    Relation: Type[DecodableRelation] = DecodableRelation
    AdapterSpecificConfigs: Type[AdapterConfig] = DecodableConfig
    ConnectionManager = DecodableAdapterConnectionManager

    connections: DecodableAdapterConnectionManager
    logger = AdapterLogger("Decodable")

    # AdapterProtocol impl

    def set_query_header(self, manifest: Manifest) -> None:
        raise NotImplementedError()

    @staticmethod
    def get_thread_identifier() -> Hashable:
        raise NotImplementedError()

    def get_thread_connection(self) -> Connection:
        return self.connections.get_thread_connection()

    def set_thread_connection(self, conn: Connection) -> None:
        self.set_thread_connection(conn)

    def get_if_exists(self) -> Optional[Connection]:
        raise NotImplementedError()

    def clear_thread_connection(self) -> None:
        self.connections.clear_thread_connection()

    def exception_handler(self, sql: str) -> ContextManager[Any]:
        raise NotImplementedError()

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        return self.connections.set_connection_name(name)

    def cancel_open(self) -> Optional[List[str]]:
        raise NotImplementedError()

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        raise NotImplementedError()

    def release(self) -> None:
        raise NotImplementedError()

    def cleanup_all(self) -> None:
        raise NotImplementedError()

    def begin(self) -> None:
        raise NotImplementedError()

    def commit(self) -> None:
        raise NotImplementedError()

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        raise NotImplementedError()

    # AdapterProtocol impl end

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def convert_text_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(String())

    @classmethod
    def convert_number_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(Decimal())

    @classmethod
    def convert_boolean_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(Boolean())

    @classmethod
    def convert_datetime_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(TimestampLocal(3))

    @classmethod
    def convert_date_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(Date())

    @classmethod
    def convert_time_type(cls, agate_table: AgateTable, col_idx: int) -> str:
        return repr(Time(3))

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def list_schemas(self, database: str) -> List[str]:
        return []

    @available.parse_none
    def create_schema(self, relation: BaseRelation):
        pass

    @available.parse_none
    def drop_schema(self, relation: BaseRelation):
        """Drop the given schema (and everything in it) if it exists."""
        pass
        # raise NotImplementedException("`drop_schema` is not implemented for this adapter!")

    @available
    @classmethod
    def quote(cls, identifier: str) -> str:
        """Quote the given identifier, as appropriate for the database."""
        return f"`{identifier}`"

    @available.parse_none
    def drop_relation(self, relation: BaseRelation) -> None:
        """Drop the given relation.

        *Implementors must call self.cache.drop() to preserve cache state!*
        """
        self.cache_dropped(relation)

        if relation.type is None:
            raise_compiler_error(f"Tried to drop relation {relation}, but its type is null.")

        if relation.identifier is None:
            return

        client = self._control_plane_client()

        self.logger.debug(f"Dropping pipeline '{relation}'...")

        pipeline_id = client.get_pipeline_id(relation.render())
        if pipeline_id:
            pipe_info = client.get_pipeline_information(pipeline_id)
            if pipe_info["actual_state"] == "RUNNING" or pipe_info["target_state"] == "RUNNING":
                client.deactivate_pipeline(pipeline_id)
            client.delete_pipeline(pipeline_id)
            self.logger.debug(f"Pipeline '{relation}' deleted successfully")

        self.logger.debug(f"Dropping stream '{relation}'...")

        stream_id = client.get_stream_id(relation.render())

        if not stream_id:
            return

        # We need to first delete any pipelines that rely on this stream as their source
        pipelines = client.list_pipelines().items
        for pipeline in pipelines:
            pipe_id = pipeline["id"]

            streams = client.get_associated_streams(pipe_id).items
            should_delete = False
            for stream in streams:
                if stream["is_source"] and stream["stream_id"] == stream_id:
                    should_delete = True
                    break

            if not should_delete:
                continue

            pipe_info = client.get_pipeline_information(pipe_id)
            # TODO: Reference cache
            self.drop_relation(
                self.Relation.create(
                    database=relation.database,
                    schema=relation.schema,
                    identifier=pipe_info["name"],
                    type=RelationType.Table,
                )
            )

        client.delete_stream(stream_id)
        self.logger.debug(f"Stream '{relation}' deleted successfully")

    @available.parse_none
    def truncate_relation(self, relation: BaseRelation) -> None:
        """Truncate the given relation."""
        if not relation.identifier:
            raise_compiler_error("Cannot truncate an unnamed relation")

        control_plane_client = self._control_plane_client()
        data_plane_client = self._data_plane_client()
        stream_id = control_plane_client.get_stream_id(relation.render())

        if not stream_id:
            raise_database_error(
                f"Error clearing stream `{relation.render()}`: stream doesn't exist"
            )

        clear_token_response = control_plane_client.get_clear_stream_token(stream_id)
        data_plane_client.clear_stream(stream_id, clear_token_response.token)

    @available.parse_none
    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        """Rename the relation from from_relation to to_relation.

        Implementors must call self.cache.rename() to preserve cache state.
        """
        client = self._control_plane_client()
        self.cache_renamed(from_relation, to_relation)

        if not from_relation.identifier:
            raise_compiler_error("Cannot rename an unnamed relation")

        stream_id = client.get_stream_id(from_relation.render())

        if not stream_id:
            raise_database_error(f"Cannot rename '{from_relation}': stream does not exist")

        if not to_relation.identifier:
            raise_compiler_error(f"Cannot rename relation {from_relation} to nothing")

        client.update_stream(stream_id=stream_id, props={"name": to_relation.render()})
        self.logger.debug(f"Renamed stream '{from_relation}' to '{to_relation}'")

        pipeline_id = client.get_pipeline_id(from_relation.render())

        if not pipeline_id:
            raise_database_error(
                f"Cannot rename '{from_relation.render()}': pipeline does not exist"
            )

        pipe_info = client.get_pipeline_information(pipeline_id)
        if not pipe_info["sql"]:
            raise_database_error(
                f"Cannot rename relation '{from_relation}': pipeline returned no sql"
            )
        sql: str = pipe_info["sql"]

        client.update_pipeline(
            pipeline_id=pipeline_id,
            props={
                "name": to_relation.render(),
                "sql": self._replace_sink(from_relation, to_relation, sql),
                "description": self._pipeline_description(to_relation),
            },
        )
        self.logger.debug(f"Renamed pipeline '{from_relation}' to '{to_relation}'")

        # Update the sql for any pipelines that had `from_relation` as an inbound stream
        renamed_sources: int = 0
        pipelines = client.list_pipelines().items
        for pipeline in pipelines:
            pipe_id = pipeline["id"]

            streams = client.get_associated_streams(pipe_id).items
            should_update = False
            for stream in streams:
                if stream["is_source"] and stream["stream_id"] == stream_id:
                    should_update = True
                    break

            if not should_update:
                continue

            pipe_info = client.get_pipeline_information(pipe_id)
            if pipe_info["sql"]:
                client.update_pipeline(
                    pipeline_id=pipe_id,
                    props={
                        "sql": self._replace_source(from_relation, to_relation, pipe_info["sql"])
                    },
                )
                renamed_sources += 1

        self.logger.debug(
            f"Renamed sources from '{from_relation}' to '{to_relation}' in {renamed_sources} pipelines"
        )

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        """Expand the current table's types to match the goal table. (passable)

        :param self.Relation goal: A relation that currently exists in the
            database with columns of the desired types.
        :param self.Relation current: A relation that currently exists in the
            database with columns of unspecified types.
        """
        raise NotImplementedException(
            "`expand_target_column_types` is not implemented for this adapter!"
        )

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        relations: List[BaseRelation] = []

        stream_list: List[Dict[str, Any]] = self._control_plane_client().list_streams().items
        for stream in stream_list:
            relations.append(
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=stream["name"],
                    type=RelationType.Table,
                )
            )

        return relations

    @available.parse_list
    def get_columns_in_relation(
        self,
        relation: BaseRelation,
    ) -> List[Column]:
        columns: List[Column] = []
        if not relation.identifier:
            return []

        stream_info = self._control_plane_client().get_stream_information(
            stream_id=relation.render()
        )

        for schema_column in stream_info["schema_v2"]["fields"]:
            columns.append(
                Column.create(name=schema_column["name"], label_or_dtype=schema_column.get("type"))
            )

        return columns

    @available
    def has_changed(
        self,
        sql: str,
        relation: BaseRelation,
        pipeline: Dict[str, Any],
        output_stream: Dict[str, Any],
    ) -> bool:
        client = self._control_plane_client()
        for resource_result in client.apply(self.generate_declarative_yaml(sql, relation, pipeline, output_stream)):
            if resource_result.get('result', 'unknown') != 'unchanged':
                return True
        return False


    @available
    def create_table(
        self,
        sql: str,
        temporary: bool,
        relation: BaseRelation,
        nodes: Dict[str, Any],
        pipeline: Dict[str, Any],
        output_stream: Dict[str, Any],
    ) -> None:
        if not relation.identifier:
            raise_compiler_error("Cannot create an unnamed relation")

        self.logger.debug(f"Creating table {relation}")

        name: str = relation.identifier.split("__")[0]  # strip any suffixes added by dbt
        model: Optional[ParsedNode] = None
        for node, info in nodes.items():
            if info["alias"] == name:
                model = ParsedNode.from_dict(nodes[node])

        if not model:
            self.logger.debug(f"Model {relation.render()} not found in dbt graph")

        client = self._control_plane_client()

        client.apply(self.generate_declarative_yaml(sql, relation, pipeline, output_stream))

        self.logger.debug(f"Pipeline '{relation}' successfully created!")

    @available
    def generate_declarative_yaml(
        self,
        sql: str,
        relation: BaseRelation,
        pipeline: Dict[str, Any],
        output_stream: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        self.populate_output_stream_spec(relation, sql, output_stream)

        pipeline['sql'] = self._wrap_as_pipeline(relation.render(), sql)
        pipeline.setdefault('execution', {})
        pipeline['execution'].setdefault('active', True)

        return [
            {
                'kind': 'stream',
                'spec_version': 'v1',
                'metadata': {
                    'name': relation.render(),
                },
                'spec': output_stream
            },
            {
                'kind': 'pipeline',
                'spec_version': 'v2',
                'metadata': {
                    'name': relation.render(),
                    'description': self._pipeline_description(relation)
                },
                'spec': pipeline
            }
        ]

    def populate_output_stream_spec(self, relation: BaseRelation, sql: str, stream_spec: Dict[str, Any]):
        client = self._control_plane_client()
        stream_spec.setdefault('schema_v2', {})
        stream_spec['schema_v2'].setdefault('fields', [])
        stream_spec['schema_v2'].setdefault('constraints', {})
        stream_spec['schema_v2'].setdefault('watermarks', [])
        if len(stream_spec['schema_v2']['fields']) == 0:
            fields: List[Dict[str, str]] = client.get_stream_from_sql(
                self._wrap_as_pipeline(relation.render(), sql)
            )["schema_v2"]["fields"]

            if not fields:
                raise_database_error(
                    f"Error creating the {relation} stream: empty schema returned for sql:\n{sql}"
                )

            stream_spec['schema_v2']['fields'] = fields

    @available
    def create_seed_table(
        self, table_name: str, agate_table: AgateTable, column_override: Dict[str, str]
    ):
        schema_fields: List[PhysicalSchemaField] = []

        column_names: tuple[str] = agate_table.column_names
        for ix, col_name in enumerate(column_names):
            type: Optional[str] = self.convert_type(agate_table, ix)
            if not type:
                raise_compiler_error(
                    f"Couldn't infer type for column `{col_name}` in seed `{table_name}`"
                )

            field_type = FieldType.from_str(type)
            override = column_override.get(col_name, "")

            if override:
                override_field_type = FieldType.from_str(override)
                if not override_field_type:
                    self.logger.warning(
                        f"Type override `{override}` for column `{col_name}` in seed `{table_name}` doesn't match any of Decodable's known types. Falling back to inferred type."
                    )
                else:
                    field_type = override_field_type

            if not field_type:
                raise_compiler_error(
                    f"Inferred type `{type}` for column `{col_name}` doesn't match any of Decodable's known types"
                )

            schema_fields.append(PhysicalSchemaField(name=col_name, type=field_type))

        client = self._control_plane_client()

        self.logger.debug(f"Creating connection and stream for seed `{table_name}`...")
        response = client.create_connection(
            name=table_name, schema=SchemaV2(schema_fields, [], Constraints(primary_key=[]))
        )
        self.logger.debug(f"Connection and stream `{table_name}` successfully created!")

        self.logger.debug(f"Activating connection `{table_name}`...")
        client.activate_connection(conn_id=response["id"])
        self.logger.debug(f"Connection `{table_name}` activated!")

    @available
    def send_seed_as_events(self, seed_name: str, data: AgateTable):
        self.logger.debug(f"Sending data to connection `{seed_name}`")
        client = self._control_plane_client()

        conn_id = client.get_connection_id(seed_name)
        if not conn_id:
            raise_database_error(
                f"Trying to send seed events to a non-existing connection `{seed_name}`"
            )

        events: List[Dict[str, Any]] = []
        for row in data.rows:
            event: Dict[str, Any] = {
                col_name: str(row[col_name])  # pyright: ignore [reportUnknownArgumentType]
                for col_name in data.column_names
            }
            events.append(event)

        events_received = client.send_events(conn_id, events)
        if len(events) != events_received:
            self.logger.warning(
                f"While seeding data for `{seed_name}`: sent {len(events)} but connection reported only {events_received} events received."
            )

        client.deactivate_connection(conn_id)

    @available
    def reactivate_connection(self, connection: Relation):
        client = self._control_plane_client()

        conn_id = client.get_connection_id(connection.render())
        if not conn_id:
            raise_database_error(f"Unable to reactivate connection: '{connection}' does not exist")

        client.activate_connection(conn_id)

    @available
    def stop_pipeline(self, pipe: Relation):
        client = self._control_plane_client()

        pipe_id = client.get_pipeline_id(pipe.render())
        if not pipe_id:
            raise_database_error(f"Unable to deactivate pipeline: '{pipe}' does not exist")

        client.deactivate_pipeline(pipe_id)

    @available
    def delete_pipeline(self, pipe: Relation):
        client = self._control_plane_client()

        pipeline_id = client.get_pipeline_id(pipe.render())
        if pipeline_id:
            pipe_info = client.get_pipeline_information(pipeline_id)
            if pipe_info["actual_state"] == "RUNNING" or pipe_info["target_state"] == "RUNNING":
                client.deactivate_pipeline(pipeline_id)
            client.delete_pipeline(pipeline_id)

    @available
    def delete_stream(self, stream: Relation, skip_errors: bool = False):
        client = self._control_plane_client()

        stream_id = client.get_stream_id(stream.render())
        if not stream_id:
            raise_database_error(f"Unable to delete stream: `{stream}` does not exist")

        try:
            client.delete_stream(stream_id)
        except Exception as e:
            if skip_errors:
                self.logger.warning(f"Deleting stream `{stream}` failed: {e}")
            else:
                raise_database_error(f"Deleting stream `{stream}` failed: {e}")

    @available
    def delete_connection(self, conn: Relation):
        client = self._control_plane_client()

        conn_id = client.get_connection_id(conn.render())
        if not conn_id:
            raise_database_error(f"Unable to delete connection: `{conn}` does not exist")

        client.deactivate_connection(conn_id)
        client.delete_connection(conn_id)

    @available
    def replace_disallowed_operations(self, sql: str) -> str:
        return sql.replace("!=", "<>")

    @available
    def should_materialize_tests(self) -> bool:
        credentials: Optional[
            DecodableAdapterCredentials
        ] = self.get_thread_connection().credentials
        if not credentials:
            return False
        return credentials.materialize_tests

    def _control_plane_client(self) -> DecodableControlPlaneApiClient:
        handle: DecodableHandler = (
            self.get_thread_connection().handle
        )  # pyright: ignore [reportGeneralTypeIssues]
        return handle.control_plane_client

    def _data_plane_client(self) -> DecodableDataPlaneApiClient:
        handle: DecodableHandler = (
            self.get_thread_connection().handle
        )  # pyright: ignore [reportGeneralTypeIssues]
        return handle.data_plane_client

    @classmethod
    def _get_model_schema_hints(cls, model: ParsedNode) -> Set[PhysicalSchemaField]:
        return {
            PhysicalSchemaField.get(column.name, column.data_type)
            for column in model.columns.values()
            if column.data_type
        }

    @staticmethod
    def _pretty_schema(
        schema: Sequence[SchemaField], indent: int = 0, name: Optional[str] = None
    ) -> str:
        fields = ""
        for field_ in sorted(schema, key=lambda sf: sf.name):
            i = "\t" * (indent + 1)
            fields += f"{i}{field_},\n"

        i = "\t" * indent
        prefix = f"{i}{{"
        if name:
            prefix = f"{i}{name} = {{"

        suffix = f"{i}}}"
        if not fields:
            suffix = "}"

        return f"{prefix}\n{fields}{suffix}"

    @classmethod
    def _wrap_as_pipeline(cls, sink: str, sql: str) -> str:
        return f"INSERT INTO {sink} {sql}"

    @classmethod
    def _replace_sink(cls, old_sink: BaseRelation, new_sink: BaseRelation, sql: str) -> str:
        return sql.replace(f"INSERT INTO {old_sink}", f"INSERT INTO {new_sink}", 1)

    @classmethod
    def _replace_source(cls, old_source: BaseRelation, new_source: BaseRelation, sql: str) -> str:
        sql = sql.replace(f"from {old_source}", f"from {new_source}")
        return sql.replace(f"FROM {old_source}", f"FROM {new_source}")

    @classmethod
    def _pipeline_description(cls, relation: BaseRelation) -> str:
        return f"Pipeline for the '{relation}' dbt model"
