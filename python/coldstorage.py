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
        lib.open(self.path.encode("ascii"))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        lib.close()
        if lib.detect_leaks():
            raise RuntimeError("Leaks detected")

    def put(self, key: bytes, val: bytes):
        lib.put(key, val)

    def get(self, key: bytes) -> Optional[bytes]:
        result = b" " * 1024
        length = lib.get(key, result)
        return result[:length] if length > 0 else None

    def delete(self, key: bytes):
        lib.remove(key)

    def detect_leaks(self) -> bool:
        return lib.detect_leaks()
