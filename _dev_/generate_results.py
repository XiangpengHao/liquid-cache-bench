#!/usr/bin/env python3
"""
Generate expected query results from Parquet data using DataFusion.

Usage:
    uv run --with datafusion python _dev_/generate_results.py --data-dir <data_dir> --sql-dir <sql_dir> --output-dir <output_dir>
"""

import argparse
import sys
from pathlib import Path

try:
    from datafusion import SessionContext
except ImportError:
    print("Error: datafusion is required. Install with: uv pip install datafusion")
    sys.exit(1)

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
except ImportError:
    print("Error: pyarrow is required. Install with: uv pip install pyarrow")
    sys.exit(1)


def register_parquet_tables(ctx, data_dir: Path):
    """Register all Parquet files in the data directory as tables."""
    parquet_files = list(data_dir.glob("*.parquet"))
    
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {data_dir}")
    
    registered_tables = []
    for parquet_file in parquet_files:
        # Table name is the filename without extension
        # Parquet files are named with uppercase table names (e.g., Posts.parquet)
        table_name = parquet_file.stem
        
        # Register the Parquet file as a table
        ctx.register_parquet(f'"{table_name}"', str(parquet_file))
        registered_tables.append(table_name)
        print(f"Registered table '{table_name}' from {parquet_file.name}")
    
    return registered_tables


def execute_query(ctx, sql: str, output_path: Path) -> None:
    """Execute a SQL query and write the result to a Parquet file."""
    print(f"Executing query and writing result to {output_path.name}...")
    
    try:
        # Execute the query
        df = ctx.sql(sql)
        
        # Collect results
        batches = df.collect()
        
        # Write to Parquet
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert batches to PyArrow table and write to Parquet
        if batches:
            # Combine all batches into a single table
            table = pa.Table.from_batches(batches)
        else:
            # Empty result - create empty table
            # We'll create a minimal empty table (this is a fallback)
            # In practice, most queries should return at least one row
            table = pa.Table.from_pylist([])
        
        pq.write_table(table, output_path)
        
        # Get row count for confirmation
        row_count = table.num_rows
        print(f"  ✓ Query executed successfully. Result has {row_count} rows.")
        
    except Exception as e:
        print(f"  ✗ Error executing query: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Generate expected query results from Parquet data using DataFusion"
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        required=True,
        help='Directory containing Parquet data files'
    )
    parser.add_argument(
        '--sql-dir',
        type=str,
        required=True,
        help='Directory containing SQL query files'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory for query results (Parquet format)'
    )
    
    args = parser.parse_args()
    
    # Convert to Path objects
    data_dir = Path(args.data_dir)
    sql_dir = Path(args.sql_dir)
    output_dir = Path(args.output_dir)
    
    # Validate directories
    if not data_dir.exists() or not data_dir.is_dir():
        print(f"Error: Data directory does not exist or is not a directory: {data_dir}")
        sys.exit(1)
    
    if not sql_dir.exists() or not sql_dir.is_dir():
        print(f"Error: SQL directory does not exist or is not a directory: {sql_dir}")
        sys.exit(1)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create DataFusion context
    print("Initializing DataFusion context...")
    ctx = SessionContext()
    
    # Register all Parquet files as tables
    print(f"\nRegistering Parquet files from {data_dir}...")
    registered_tables = register_parquet_tables(ctx, data_dir)
    print(f"Registered {len(registered_tables)} tables: {', '.join(registered_tables)}\n")
    
    # Find all SQL files
    sql_files = sorted(sql_dir.glob("*.sql"))
    
    if not sql_files:
        print(f"Warning: No SQL files found in {sql_dir}")
        sys.exit(1)
    
    print(f"Found {len(sql_files)} SQL query files\n")
    
    # Execute each query
    successful = 0
    failed = 0
    
    for sql_file in sql_files:
        print(f"Processing {sql_file.name}...")
        
        # Read SQL query
        try:
            sql = sql_file.read_text()
        except Exception as e:
            print(f"  ✗ Error reading SQL file: {e}")
            failed += 1
            continue
        
        # Output filename is the SQL filename with .parquet extension
        output_filename = sql_file.stem + ".parquet"
        output_path = output_dir / output_filename
        
        # Execute query and write result
        try:
            execute_query(ctx, sql, output_path)
            successful += 1
        except Exception as e:
            print(f"  ✗ Failed to execute query: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
        
        print()  # Empty line between queries
    
    # Summary
    print("=" * 60)
    print(f"Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(sql_files)}")
    print(f"\nResults written to: {output_dir}")
    
    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()

