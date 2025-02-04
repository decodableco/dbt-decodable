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

import unittest
from decodable.client.schema import (
    SchemaField,
    PhysicalSchemaField,
    FieldKind,
    Constraints,
    SchemaV2,
    Watermark,
    schema_field_factory,
    MetadataSchemaField,
    ComputedSchemaField,
)
from decodable.client.types import String, Int
import json


class TestSchemaField(unittest.TestCase):
    def test_get_field_type_valid(self):
        field_type = SchemaField.get_field_type("STRING")
        self.assertEqual(field_type, String())

    def test_get_field_type_invalid_lowercase(self):
        with self.assertRaises(Exception) as context:
            SchemaField.get_field_type("string")
        self.assertTrue("Type 'string' not recognized" in str(context.exception))


class TestPhysicalSchemaField(unittest.TestCase):
    def test_to_dict(self):
        field = PhysicalSchemaField(name="field1", type=String())
        expected_dict = {"name": "field1", "type": "STRING", "kind": "physical"}
        self.assertEqual(field.to_dict(), expected_dict)

    def test_eq(self):
        field1 = PhysicalSchemaField(name="field1", type=String())
        field2 = PhysicalSchemaField(name="field1", type=String())
        self.assertEqual(field1, field2)

    def test_str(self):
        field = PhysicalSchemaField(name="field1", type=String())
        self.assertEqual(str(field), "name: 'field1' | kind: 'physical' | type: 'STRING'")

    def test_hash(self):
        field1 = PhysicalSchemaField(name="field1", type=String())
        field2 = PhysicalSchemaField(name="field1", type=String())
        self.assertEqual(hash(field1), hash(field2))

    def test_get(self):
        field = PhysicalSchemaField.get(name="field1", type="INT")
        self.assertEqual(field.name, "field1")
        self.assertEqual(field.type, Int())
        self.assertEqual(field.kind, FieldKind.physical)


class TestMetadataSchemaField(unittest.TestCase):
    def test_eq(self):
        field1 = MetadataSchemaField(name="field1", key="key1", type=String())
        field2 = MetadataSchemaField(name="field1", key="key1", type=String())
        self.assertEqual(field1, field2)


class TestComputedSchemaField(unittest.TestCase):
    def test_eq(self):
        field1 = ComputedSchemaField(name="field1", expression="expr1")
        field2 = ComputedSchemaField(name="field1", expression="expr1")
        self.assertEqual(field1, field2)


class TestSchemaV2(unittest.TestCase):
    def test_from_json(self):
        json_data = {
            "fields": [{"name": "field1", "kind": "physical", "type": "STRING"}],
            "watermarks": [{"name": "wm1", "expression": "expr1"}],
            "constraints": {"primary_key": ["field1"]},
        }
        schema = SchemaV2.from_json(json_data)
        self.assertEqual(len(schema.fields), 1)
        self.assertEqual(schema.fields[0].name, "field1")
        self.assertEqual(len(schema.watermarks), 1)
        self.assertEqual(schema.watermarks[0].name, "wm1")
        self.assertEqual(schema.constraints.primary_key, ["field1"])

    def test_to_dict(self):
        fields = [PhysicalSchemaField(name="field1", type=String())]
        watermarks = [Watermark(name="wm1", expression="expr1")]
        constraints = Constraints(primary_key=["field1"])
        schema = SchemaV2(fields=fields, watermarks=watermarks, constraints=constraints)
        expected_dict = {
            "fields": [{"name": "field1", "type": "STRING", "kind": "physical"}],
            "watermarks": [{"name": "wm1", "expression": "expr1"}],
            "constraints": {"primary_key": ["field1"]},
        }
        self.assertEqual(schema.to_dict(), expected_dict)

    def test_eq(self):
        fields = [PhysicalSchemaField(name="field1", type=String())]
        watermarks = [Watermark(name="wm1", expression="expr1")]
        constraints = Constraints(primary_key=["field1"])
        schema1 = SchemaV2(fields=fields, watermarks=watermarks, constraints=constraints)
        schema2 = SchemaV2(fields=fields, watermarks=watermarks, constraints=constraints)
        self.assertEqual(schema1, schema2)

    def test_hash(self):
        fields = [PhysicalSchemaField(name="field1", type=String())]
        watermarks = [Watermark(name="wm1", expression="expr1")]
        constraints = Constraints(primary_key=["field1"])
        schema = SchemaV2(fields=fields, watermarks=watermarks, constraints=constraints)
        expected_hash = hash(
            json.dumps(
                {
                    "fields": [{"name": "field1", "kind": "physical", "type": "STRING"}],
                    "watermarks": [{"name": "wm1", "expression": "expr1"}],
                    "constraints": {"primary_key": ["field1"]},
                }
            )
        )
        self.assertEqual(hash(schema), expected_hash)


class TestSchemaFieldFactory(unittest.TestCase):
    def test_schema_field_factory_physical(self):
        field_dict = {"name": "field1", "kind": "physical", "type": "STRING"}
        field = schema_field_factory(field_dict)
        assert type(field) is PhysicalSchemaField
        self.assertEqual(field.name, "field1")
        self.assertEqual(field.type, String())
        self.assertEqual(field.kind, FieldKind.physical)

    def test_schema_field_factory_metadata(self):
        field_dict = {"name": "field1", "kind": "metadata", "key": "key1", "type": "STRING"}
        field = schema_field_factory(field_dict)
        assert type(field) is MetadataSchemaField
        self.assertEqual(field.name, "field1")
        self.assertEqual(field.key, "key1")
        self.assertEqual(field.type, String())
        self.assertEqual(field.kind, FieldKind.metadata)

    def test_schema_field_factory_computed(self):
        field_dict = {"name": "field1", "kind": "computed", "expression": "expr1"}
        field = schema_field_factory(field_dict)
        assert type(field) is ComputedSchemaField
        self.assertEqual(field.name, "field1")
        self.assertEqual(field.expression, "expr1")
        self.assertEqual(field.kind, FieldKind.computed)


if __name__ == "__main__":
    unittest.main()
