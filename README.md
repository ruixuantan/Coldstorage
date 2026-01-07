# Coldstorage

Toy LSM storage. Single threaded for simplicity. Referenced and followed: <https://skyzh.github.io/mini-lsm/>

## Setting up

This project uses:

* uv (0.6.16)
* zig (0.15.2)

Running the python example:

```sh
zig build
cd python
uv run main.py -p <path to where db folder is>
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

* [x] Bloom filter optimization
* [x] WAL
* [x] C FFI
* [ ] Storage compression
* [ ] Improve compaction (consider tigerbeetle)
* [ ] Implement SSTable Block cache
