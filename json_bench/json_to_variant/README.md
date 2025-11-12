# json-to-variant

A tiny CLI that converts JSON documents into a single-column Parquet file encoded with
Parquet's experimental Variant type. It is intended for quick benchmarking and for
sharing Variant-encoded datasets with Arrow tooling.

## Build

```bash
cargo build --release
```

## Usage

```
json-to-variant [OPTIONS] <INPUT> <OUTPUT>
```

**Arguments**

- `INPUT` – Path to a JSON file or a directory containing JSON/NDJSON files.
- `OUTPUT` – Destination `.parquet` file.

**Key options**

- `--format <auto|ndjson|single>` – Force how each file is interpreted (default: auto, which
  treats `.jsonl` / `.ndjson` extensions as line-delimited JSON and everything else as a single
  JSON document).
- `--recursive` – When `INPUT` is a directory, walk all subdirectories.
- `--batch-bytes <N>` – Flush roughly `N` bytes of JSON at a time (default 100 MB). Lower this
  if you hit memory limits; raise it for larger batches.

## Examples

Convert one NDJSON file:

```bash
cargo run --release -- \
  ./data/events.ndjson \
  ./out/events.parquet \
  --format ndjson
```

Process every JSON/NDJSON file under a directory, recursing into subdirectories and flushing
once every 256 MB of accumulated JSON text:

```bash
cargo run --release -- \
  ./fixtures \
  ./out/fixtures.parquet \
  --recursive \
  --batch-bytes $((256*1024*1024))
```

The resulting Parquet file contains a single Variant column named `data`, ready for use with
`parquet::variant` utilities or downstream Arrow consumers.
