# Coldstorage

Toy LSM storage. Single threaded for simplicity. Referenced and followed: <https://skyzh.github.io/mini-lsm/>

## Features

* WAL
* SSTable block cache optimization
* Bloom filter optimization
* C FFI

Example python script:

```python
from coldstorage import ColdStorage

cs = ColdStorage("/path/to/db")
with cs:
    cs.put(b"key1", b"value1")
    cs.put(b"key2", b"value2")
    print(cs.get(b"key1"))  # b"value1"
    cs.delete(b"key2")
    print(cs.get(b"key2"))  # None
```

## Setting up

This project uses:

* uv (0.6.16)
* zig (0.15.2)

Running the python example:

```sh
zig build
cd python
uv sync
uv run main.py
```

Testing:

```sh
zig build test
```

Linting:

```sh
zig fmt src/
cd python
uv run ruff check
uv run ruff format
```

## TODOs

* Storage compression
* Improve compaction
