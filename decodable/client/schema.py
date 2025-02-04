#
#  Copyright 2025 decodable Inc.
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


import json

from dataclasses import dataclass, asdict, field as dataclass_field, fields as dataclass_fields
from decodable.client.types import FieldType
from typing import Any, Sequence, List, Dict, Union, Literal

from dbt.exceptions import raise_compiler_error


class FieldKind:
    physical = "physical"
    metadata = "metadata"
    computed = "computed"


@dataclass(frozen=True, init=False)
class SchemaField:
    name: str
    kind: str

    @classmethod
    def get_field_type(cls, type_: str) -> FieldType:
        field_type = FieldType.from_str(type_)
        if field_type is None:
            raise_compiler_error(f"Type '{type_}' not recognized")
        return field_type

    def to_dict(self) -> Dict[str, str]:
        res = {}
        for field in dataclass_fields(self):
            field_value = getattr(self, field.name)
            res[field.name] = repr(field_value) if field.type == FieldType else field_value
        return res

    def __str__(self) -> str:
        return " | ".join(f"{k}: '{v}'" for k, v in self.to_dict().items())


@dataclass(frozen=True)
class PhysicalSchemaField(SchemaField):
    name: str
    type: FieldType
    kind: str = dataclass_field(init=False, default=FieldKind.physical)

    @classmethod
    def get(cls, name: str, type: str) -> "PhysicalSchemaField":
        return cls(name, cls.get_field_type(type))


@dataclass(frozen=True)
class MetadataSchemaField(SchemaField):
    name: str
    key: str
    type: FieldType
    kind: str = dataclass_field(init=False, default=FieldKind.metadata)

    @classmethod
    def get(cls, name: str, key: str, type: str) -> "MetadataSchemaField":
        return cls(name, key, cls.get_field_type(type))


@dataclass(frozen=True)
class ComputedSchemaField(SchemaField):
    name: str
    expression: str
    kind: str = dataclass_field(init=False, default=FieldKind.computed)

    @classmethod
    def get(cls, name: str, expression: str) -> "ComputedSchemaField":
        return cls(name, expression)


def schema_field_factory(field: Dict[str, str]) -> SchemaField:
    kind = field["kind"]
    if kind == FieldKind.physical:
        return PhysicalSchemaField.get(field["name"], field["type"])
    elif kind == FieldKind.metadata:
        return MetadataSchemaField.get(field["name"], field["key"], field["type"])
    elif kind == FieldKind.computed:
        return ComputedSchemaField.get(field["name"], field["expression"])
    else:
        raise ValueError(f"Unknown field kind: {kind}")


@dataclass(frozen=True)
class Constraints:
    primary_key: List[str]


@dataclass(frozen=True)
class Watermark:
    name: str
    expression: str


@dataclass(frozen=True)
class SchemaV2:
    fields: Sequence[SchemaField]
    watermarks: List[Watermark]
    constraints: Constraints

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> "SchemaV2":
        return SchemaV2(
            fields=[schema_field_factory(field) for field in json.get('fields', [])],
            watermarks=[
                Watermark(name=w_json["name"], expression=w_json["expression"])
                for w_json in json.get('watermarks', [])
            ],
            constraints=Constraints(primary_key=json.get('constraints', {}).get('primary_key')))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fields": [field.to_dict() for field in self.fields],
            "watermarks": [asdict(w) for w in self.watermarks],
            "constraints": asdict(self.constraints),
        }

    def __hash__(self) -> int:
        return hash(json.dumps(self.to_dict()))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and hash(self) == hash(other)
