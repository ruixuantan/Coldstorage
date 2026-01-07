from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Tuple

from cffi import FFI

header_file = open("colderstorage.h").read()
cffi = FFI()
cffi.cdef(header_file)
lib = cffi.dlopen("../zig-out/lib/libcolderstorage.dylib")


class Type(Enum):
    null = 1
    integer = 2
    real = 3
    text = 4
    blob = 5


@dataclass
class Column:
    name: str
    type: Type


@dataclass
class Schema:
    columns: List[Column]

    @classmethod
    def from_tuple(cls, tuple: Tuple[Any]) -> Schema:
        return cls(columns=[Column(name=t[0], type=Type(t[1])) for t in tuple])

    def to_tuples(self) -> List[Tuple[str, str]]:
        return [(col.name, col.type.name) for col in self.columns]


class ColderstorageError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class Cursor:
    def __init__(self):
        self._open = False

    def fetch(self) -> Optional[Tuple[Any]]:
        self._open = True
        res = lib.fetch()
        buffer_len = lib.get_buffer_len()
        buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
        t = ptcl.parse_array(buffer_msg)
        if not res:
            self.close()
            return None
        return t

    def schema(self) -> Schema:
        lib.cursor_write_schema()
        buffer_len = lib.get_buffer_len()
        buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
        t = ptcl.parse_array(buffer_msg)
        return Schema.from_tuple(t)

    def commit(self) -> Tuple[Any]:
        res = lib.fetch()
        buffer_len = lib.get_buffer_len()
        buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
        if not res:
            err = ptcl.parse_error(buffer_msg)
            self.close()
            raise ColderstorageError(f"Error executing sql statement: {err}")
        t = ptcl.parse_array(buffer_msg)
        self.close()
        return t

    def open(self):
        self._open = True

    def close(self):
        self._open = False
        lib.close_cursor()


class Colderstorage:
    def __init__(self, path: str = "colderstorage.db"):
        self.path = path
        self.cursor = Cursor()

    def __enter__(self):
        lib.open(self.path.encode("ascii"))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.cursor._open:
            self.cursor.close()
        lib.close()
        if lib.detect_leaks():
            raise RuntimeError("Leaks detected")

    def execute(self, sql: str) -> Cursor:
        if self.cursor._open:
            self.cursor.close()
        if not lib.execute(sql.encode("ascii")):
            buffer_len = lib.get_buffer_len()
            buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
            err = ptcl.parse_error(buffer_msg)
            raise ColderstorageError(f"Error executing sql statement: {err}")
        self.cursor.open()
        return self.cursor

    def list_tables(self) -> List[Tuple[str]]:
        lib.list_tables()
        buffer_len = lib.get_buffer_len()
        buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
        return ptcl.parse_array(buffer_msg)

    def display_table(self, table_name: str) -> Optional[Schema]:
        if not lib.display_table(table_name.encode("ascii")):
            return None
        buffer_len = lib.get_buffer_len()
        buffer_msg = cffi.string(lib.get_buffer())[0:buffer_len]
        t = ptcl.parse_array(buffer_msg)
        return Schema.from_tuple(t)
