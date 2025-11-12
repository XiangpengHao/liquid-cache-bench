# StackOverflow dataset

To download the dataset and convert it to Parquet format, run:
```bash
uv run --with duckdb --with requests --with py7zr --with pandas python stackoverflow/setup.py --site dba 
```

The script will:
1. Download the Stack Exchange data dump for the specified site from archive.org
2. Extract the 7z archive
3. Convert all XML files (Posts, Users, Comments, etc.) to Parquet format with proper type conversion
4. Clean up temporary files (archive and extracted folders) after conversion

## Options

- `--site <site>`: Required. Stack Exchange site name (e.g., "stackoverflow", "pets", "dba")
- `--output-dir <dir>`: Optional. Output directory for Parquet files (default: `data-<site>`)
- `--keep-archive`: Optional. Keep the downloaded archive file after extraction (extracted folder is still removed)

## Example

```bash
# Download and convert Stack Overflow dataset (outputs to data-stackoverflow/)
uv run --with duckdb --with requests --with py7zr --with pandas python stackoverflow/setup.py --site stackoverflow

# Download and convert with custom output directory
uv run --with duckdb --with requests --with py7zr --with pandas python stackoverflow/setup.py --site stackoverflow --output-dir my_data

# Keep the archive file after extraction (extracted folder is still removed)
uv run --with duckdb --with requests --with py7zr --with pandas python stackoverflow/setup.py --site stackoverflow --keep-archive
```

## Queries

There's no official queries for the dataset, we included a few sample queries in the `query` directory.

## Expected results

We currently have expected results from three sites: `dba`, `math`, and `pets`.

More results are welcome!

