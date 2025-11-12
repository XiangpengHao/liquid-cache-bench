# Dev tools for the repo

## Generate expected results

Generate expected query results from Parquet data using DataFusion. This ensures queries are compatible with DataFusion SQL syntax.

```bash
uv run --with datafusion --with pyarrow python _dev_/generate_results.py --data-dir <data_dir> --sql-dir <sql_dir> --output-dir <output_dir>
```

**Arguments:**
- `--data-dir`: Directory containing Parquet data files (e.g., `stackoverflow/data-dba/`)
- `--sql-dir`: Directory containing SQL query files (e.g., `stackoverflow/query/`)
- `--output-dir`: Output directory for query results (Parquet format)

**Example:**
```bash
uv run --with datafusion --with pyarrow python _dev_/generate_results.py \
  --data-dir stackoverflow/data-dba \
  --sql-dir stackoverflow/query \
  --output-dir stackoverflow/results-dba
```

The script will:
1. Register all Parquet files in the data directory as tables
2. Execute each SQL query file against the registered tables
3. Write the results as Parquet files (one per query) to the output directory
4. Report success/failure for each query
