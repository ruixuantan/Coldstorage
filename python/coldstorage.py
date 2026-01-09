from __future__ import annotations

from typing import Optional

from cffi import FFI

header_file = open("coldstorage.h").read()
cffi = FFI()
cffi.cdef(header_file)
lib = cffi.dlopen("../zig-out/lib/libcoldstorage.dylib")


class Coldstorage:
    def __init__(self, path: str = "coldstorage.db"):
        self.path = path

    def __enter__(self):
        lib.cs_open(self.path.encode("ascii"))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        lib.cs_close()
        if lib.cs_detect_leaks():
            raise RuntimeError("Leaks detected")

    def put(self, key: bytes, val: bytes):
        lib.cs_put(key, val)

    def get(self, key: bytes) -> Optional[bytes]:
        result = b" " * 1024
        length = lib.cs_get(key, result)
        return result[:length] if length > 0 else None

    def delete(self, key: bytes):
        lib.cs_remove(key)

    def scan(self, lower: bytes, upper: bytes) -> Cursor:
        iterator = lib.cs_scan(lower, upper)
        return Cursor(iterator)

    def detect_leaks(self) -> bool:
        return lib.cs_detect_leaks()


class Cursor:
    def __init__(self, iterator: cffi.CDLL.cs_scan):
        self.iterator = iterator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        lib.cs_iterator_deinit(self.iterator)

    def key(self) -> Optional[bytes]:
        result = b" " * 1024
        length = lib.cs_iterator_key(self.iterator, result)
        return result[:length] if length > 0 else None

    def val(self) -> Optional[bytes]:
        result = b" " * 1024
        length = lib.cs_iterator_val(self.iterator, result)
        return result[:length] if length > 0 else None

    def is_valid(self) -> bool:
        return lib.cs_iterator_is_valid(self.iterator)

    def next(self):
        lib.cs_iterator_next(self.iterator)
