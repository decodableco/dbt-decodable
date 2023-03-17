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
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Type
import re


@dataclass(frozen=True)
class FieldType:
    synonyms: List[FieldType] = field(init=False, repr=False, default_factory=lambda: [])

    def __eq__(self, __o: object) -> bool:
        if not issubclass(__o.__class__, FieldType):
            return False

        if __o in self.synonyms:
            return True

        return self.__repr__() == __o.__repr__()

    def __hash__(self) -> int:
        return hash(repr(self))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [
            NotNull,
            StringType,
            BinaryType,
            NumericType,
            DateTimeType,
            CompoundType,
            Boolean,
            Interval,
            Multiset,
        ]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class NotNull(FieldType):
    inner_type: FieldType

    def __repr__(self) -> str:
        return f"{repr(self.inner_type)} NOT NULL"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, NotNull):
            return False
        return self.inner_type == __o.inner_type

    def __hash__(self) -> int:
        return super().__hash__()

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"(?P<inner>.*) NOT NULL", type)

        if not found:
            return None

        inner_type = FieldType.from_str(found["inner"])

        if not inner_type:
            return None

        return cls(inner_type)


@dataclass(frozen=True, eq=False)
class StringType(FieldType):
    length: int
    is_synonym: bool = False

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Char, Varchar, String]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Char(StringType):
    def __repr__(self) -> str:
        return f"CHAR({self.length})"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"CHAR\((?P<length>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["length"]))


@dataclass(frozen=True, eq=False)
class Varchar(StringType):
    def __repr__(self) -> str:
        return f"VARCHAR({self.length})"

    def __post_init__(self):
        if not self.is_synonym and self.length == String.length:
            self.synonyms.append(String(is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"VARCHAR\((?P<length>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["length"]))


@dataclass(frozen=True, eq=False)
class String(StringType):
    length: int = 2147483647

    def __repr__(self) -> str:
        return "STRING"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Varchar(self.length, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"STRING", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class BinaryType(FieldType):
    length: int
    is_synonym: bool = False

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Binary, Varbinary, Bytes]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Binary(BinaryType):
    def __repr__(self) -> str:
        return f"BINARY({self.length})"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"BINARY\((?P<length>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["length"]))


@dataclass(frozen=True, eq=False)
class Varbinary(BinaryType):
    def __repr__(self) -> str:
        return f"VARBINARY({self.length})"

    def __post_init__(self):
        if not self.is_synonym and self.length == Bytes.length:
            self.synonyms.append(Bytes(is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"VARBINARY\((?P<length>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["length"]))


@dataclass(frozen=True, eq=False)
class Bytes(BinaryType):
    length: int = 2147483647

    def __repr__(self) -> str:
        return "BYTES"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Varbinary(self.length, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"BYTES", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class NumericType(FieldType):
    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [
            ExactNumericType,
            TinyInt,
            SmallInt,
            Int,
            BigInt,
            Float,
            Double,
        ]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class ExactNumericType(NumericType):
    precision: int
    scale: int
    is_synonym: bool = False

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Decimal, Dec, Numeric]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Decimal(ExactNumericType):
    precision: int = 10
    scale: int = 0

    def __repr__(self) -> str:
        return f"DECIMAL({self.precision}, {self.scale})"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Dec(self.precision, self.scale, is_synonym=True))
            self.synonyms.append(Numeric(self.precision, self.scale, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"DECIMAL(\((?P<precision>\d+)(, (?P<scale>\d+))?\))?", type)

        if not found:
            return None

        if not found["precision"]:
            return cls()
        elif not found["scale"]:
            return cls(precision=int(found["precision"]))
        else:
            return cls(precision=int(found["precision"]), scale=int(found["scale"]))


@dataclass(frozen=True, eq=False)
class Dec(ExactNumericType):
    precision: int = 10
    scale: int = 0

    def __repr__(self) -> str:
        return f"DEC({self.precision}, {self.scale})"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Decimal(self.precision, self.scale, is_synonym=True))
            self.synonyms.append(Numeric(self.precision, self.scale, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"DEC(\((?P<precision>\d+)(, (?P<scale>\d+))?\))?", type)

        if not found:
            return None

        if not found["precision"]:
            return cls()
        elif not found["scale"]:
            return cls(precision=int(found["precision"]))
        else:
            return cls(precision=int(found["precision"]), scale=int(found["scale"]))


@dataclass(frozen=True, eq=False)
class Numeric(ExactNumericType):
    precision: int = 10
    scale: int = 0

    def __repr__(self) -> str:
        return f"NUMERIC({self.precision}, {self.scale})"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Dec(self.precision, self.scale, is_synonym=True))
            self.synonyms.append(Decimal(self.precision, self.scale, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"NUMERIC(\((?P<precision>\d+)(, (?P<scale>\d+))?\))?", type)

        if not found:
            return None

        if not found["precision"]:
            return cls()
        elif not found["scale"]:
            return cls(precision=int(found["precision"]))
        else:
            return cls(precision=int(found["precision"]), scale=int(found["scale"]))


@dataclass(frozen=True, eq=False)
class TinyInt(NumericType):
    def __repr__(self) -> str:
        return "TINYINT"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"TINYINT", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class SmallInt(NumericType):
    def __repr__(self) -> str:
        return "SMALLINT"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"SMALLINT", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Int(NumericType):
    def __repr__(self) -> str:
        return "INT"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"INT", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class BigInt(NumericType):
    def __repr__(self) -> str:
        return "BIGINT"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"BIGINT", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Float(NumericType):
    is_synonym: bool = False

    def __repr__(self) -> str:
        return "FLOAT"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Double(is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"FLOAT", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Double(NumericType):
    is_synonym: bool = False

    def __repr__(self) -> str:
        return "DOUBLE"

    def __post_init__(self):
        if not self.is_synonym:
            self.synonyms.append(Float(is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"DOUBLE", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class DateTimeType(FieldType):
    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Date, Time, TimestampType]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Date(DateTimeType):
    def __repr__(self) -> str:
        return "DATE"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"DATE", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Time(DateTimeType):
    precision: int

    def __repr__(self) -> str:
        return f"TIME({self.precision})"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"TIME\((?P<precision>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["precision"]))


@dataclass(frozen=True, eq=False)
class TimestampType(DateTimeType):
    precision: int
    timezone: bool
    is_synonym: bool = False

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Timestamp, TimestampLocal]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Timestamp(TimestampType):
    timezone: bool = False

    def __repr__(self) -> str:
        if self.timezone:
            w = "WITH"
        else:
            w = "WITHOUT"

        return f"TIMESTAMP({self.precision}) {w} TIME ZONE"

    def __post_init__(self):
        if not self.is_synonym and self.timezone:
            self.synonyms.append(TimestampLocal(self.precision, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(
            r"TIMESTAMP\((?P<precision>\d+)\)(?P<timezone_clause> (?P<with_clause>WITH|WITHOUT) TIME ZONE)?",
            type,
        )

        if not found:
            return None

        timezone: bool

        if not found["timezone_clause"]:
            timezone = False
        else:
            if found["with_clause"] == "WITH":
                timezone = True
            else:
                timezone = False

        return cls(precision=int(found["precision"]), timezone=timezone)


@dataclass(frozen=True, eq=False)
class TimestampLocal(TimestampType):
    timezone: bool = True

    def __repr__(self) -> str:
        return f"TIMESTAMP_LTZ({self.precision})"

    def __post_init__(self):
        if not self.is_synonym and self.timezone:
            self.synonyms.append(Timestamp(self.precision, timezone=True, is_synonym=True))

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"TIMESTAMP_LTZ\((?P<precision>\d+)\)", type)

        if not found:
            return None

        return cls(int(found["precision"]))


@dataclass(frozen=True)
class CompoundType(FieldType):
    class ContainerType(Enum):
        ARRAY = 0
        MAP = 1
        ROW = 2

    container_type: ContainerType = field(init=False, repr=False)
    internal_types: List[FieldType] = field(init=False, repr=False, default_factory=lambda: [])

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [ArrayType, Map, Row]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found

    def __eq__(self, __o: object) -> bool:
        if not issubclass(__o.__class__, CompoundType):
            return False

        return (
            self.container_type
            == __o.container_type  # pyright: ignore [reportGeneralTypeIssues], we know __o is a CompoundType
            and self.internal_types
            == __o.internal_types  # pyright: ignore [reportGeneralTypeIssues], we know __o is a CompoundType
        )

    def __hash__(self) -> int:
        return super().__hash__()


@dataclass(frozen=True, eq=False)
class ArrayType(CompoundType):
    container_type: CompoundType.ContainerType = field(
        init=False, repr=False, default=CompoundType.ContainerType.ARRAY
    )
    type: FieldType

    def __post_init__(self):
        self.internal_types.append(self.type)

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        candidates: List[Type[FieldType]] = [Array, TArray]

        found: Optional[FieldType] = None
        for candidate in candidates:
            found = candidate.from_str(type)
            if found:
                break

        return found


@dataclass(frozen=True, eq=False)
class Array(ArrayType):
    def __repr__(self) -> str:
        return f"ARRAY<{self.type}>"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"ARRAY<(?P<inner>.*)>", type)

        if not found:
            return None

        inner_type = FieldType.from_str(found["inner"])

        if not inner_type:
            return None

        return cls(inner_type)


@dataclass(frozen=True, eq=False)
class TArray(ArrayType):
    def __repr__(self) -> str:
        return f"{self.type} ARRAY"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"(?P<inner>.*) ARRAY", type)

        if not found:
            return None

        inner_type = FieldType.from_str(found["inner"])

        if not inner_type:
            return None

        return cls(inner_type)


@dataclass(frozen=True, eq=False)
class Map(CompoundType):
    container_type: CompoundType.ContainerType = field(
        init=False, repr=False, default=CompoundType.ContainerType.MAP
    )
    key: FieldType
    value: FieldType

    def __post_init__(self):
        self.internal_types.append(self.key)
        self.internal_types.append(self.value)

    def __repr__(self) -> str:
        return f"MAP<{self.key}, {self.value}>"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"MAP<(?P<key>.*), (?P<value>.*)>", type)

        if not found:
            return None

        key_type = FieldType.from_str(found["key"])
        value_type = FieldType.from_str(found["value"])

        if not key_type or not value_type:
            return None

        return cls(key_type, value_type)


@dataclass(frozen=True, eq=False)
class Row(CompoundType):
    # TODO: Handle ROW types
    container_type: CompoundType.ContainerType = field(
        init=False, repr=False, default=CompoundType.ContainerType.ROW
    )

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        return None


@dataclass(frozen=True, eq=False)
class PrimaryKey(FieldType):
    inner_type: FieldType

    def __repr__(self) -> str:
        return f"{self.inner_type} PRIMARY KEY"


@dataclass(frozen=True, eq=False)
class Boolean(FieldType):
    def __repr__(self) -> str:
        return "BOOLEAN"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"BOOLEAN", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Interval(FieldType):
    def __repr__(self) -> str:
        return "INTERVAL"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"INTERVAL", type)

        if not found:
            return None

        return cls()


@dataclass(frozen=True, eq=False)
class Multiset(FieldType):
    def __repr__(self) -> str:
        return "MULTISET"

    @classmethod
    def from_str(cls, type: str) -> Optional[FieldType]:
        found = re.fullmatch(r"MULTISET", type)

        if not found:
            return None

        return cls()
