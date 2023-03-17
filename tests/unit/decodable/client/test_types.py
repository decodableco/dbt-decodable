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

from decodable.client import types


class TestTypes:
    def test_concrete_type_from_str(self):
        t = types.Char.from_str("CHAR(15)")
        assert t == types.Char(15)

        t = types.Char.from_str("CHAR()")
        assert t is None

        t = types.Char.from_str("Char(10)")
        assert t is None

        t = types.Timestamp.from_str("TIMESTAMP(15) WITH TIME ZONE")
        assert t == types.Timestamp(precision=15, timezone=True)

    def test_from_str_dispatch(self):
        str_types = ["DECIMAL", "STRING", "ARRAY<CHAR(1)>", "VARBINAR", "TIMESTAMP(3)"]

        expected = [
            types.Decimal(),
            types.String(),
            types.Array(types.Char(1)),
            None,
            types.Timestamp(precision=3, timezone=False),
        ]

        for a, b in zip(str_types, expected):
            assert types.FieldType.from_str(a) == b

    def test_from_str_defaults(self):
        a = types.FieldType.from_str("DECIMAL")
        b = types.FieldType.from_str("DECIMAL(10)")
        c = types.FieldType.from_str("DECIMAL(10, 0)")

        assert a == b
        assert b == c
        assert c == a

    def test_synonyms_equality(self):
        assert types.Decimal() == types.Dec()
        assert types.Numeric(15, 3) == types.Decimal(15, 3)
        assert types.Decimal(5, 1) != types.Numeric(3, 1)

        assert types.Varbinary(types.Bytes.length) == types.Bytes()
        assert types.Varbinary(100) != types.Bytes()

        assert types.Array(types.Decimal()) == types.TArray(types.Decimal())
        assert types.Array(types.Decimal()) != types.TArray(types.String())
        assert types.Array(types.Decimal()) == types.Array(types.Numeric())
        assert types.Array(types.Decimal()) == types.TArray(types.Numeric())

        assert types.NotNull(types.Decimal()) == types.NotNull(types.Numeric())
        assert types.NotNull(types.Array(types.Dec())) == types.NotNull(
            types.TArray(types.Decimal())
        )
        assert types.NotNull(types.Array(types.String())) != types.NotNull(
            types.Array(types.Boolean())
        )
        assert types.NotNull(types.Array(types.Bytes())) != types.Bytes()
