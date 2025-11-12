# JSONBench Dataset

The JSONBench dataset from [ClickHouse/JSONBench](https://github.com/ClickHouse/JSONBench) - a benchmark for data analytics on JSON.

## Setup

To download the dataset and convert it to Parquet format, run:

```bash
uv run --with requests python json_bench/setup.py --size <size>
```

Unfortunately, this setup requires Rust and Cargo to be installed, because as of Nov 2025, only Rust's parquet implementation supports the Variant type.

So you want to make sure you installed Rust:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


### Options

- `--size <size>`: Dataset size. Options: `1m` (1M rows), `10m` (10M rows), `100m` (100M rows), `1000m` (1B rows). Default: `1m`
- `--output-dir <dir>`: Output directory for Parquet files (default: `data-bluesky-<size>`)
- `--keep-json`: Keep JSON files after conversion (default: remove them)
- `--skip-download`: Skip download step and use existing JSON files

### Examples

```bash
# Download and convert 1M row dataset (default)
uv run --with requests python json_bench/setup.py

# Download and convert 10M row dataset
uv run --with requests python json_bench/setup.py --size 10m

# Use existing JSON files (skip download)
uv run --with requests python json_bench/setup.py --skip-download

# Keep JSON files after conversion
uv run --with requests python json_bench/setup.py --keep-json
```

## What the script does

1. **Downloads** JSON files (compressed as `.json.gz`) from S3: `clickhouse-public-datasets.s3.amazonaws.com/bluesky`
2. **Decompresses** the `.gz` compressed files to `.json` files
3. **Builds** the `json_to_variant` Rust tool (if not already built)
4. **Converts** all JSON files to a single Parquet file using the Variant type
5. **Cleans up** temporary files (downloads, compressed files, and JSON files unless `--keep-json` is used)

## Output

The script produces a single Parquet file: `data-bluesky-<size>/bluesky.parquet`

The Parquet file contains a single Variant column named `data`, which stores the JSON documents in Parquet's experimental Variant type format.

## Notes

- The script requires Rust and Cargo to be installed (for building `json_to_variant`)
- The first run will take longer as it needs to build the Rust tool
- Large datasets (100m, 1000m) may take significant time to download and convert
- The script automatically creates a `.gitignore` file in the output directory to exclude Parquet files from git

