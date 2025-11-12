#!/usr/bin/env python3
"""
Download and convert Stack Exchange dataset to Parquet format.

Usage:
    uv run --with duckdb --with requests --with py7zr --with pandas python stackoverflow/setup.py --site <site>
"""

import argparse
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: requests is required. Install with: uv pip install requests")
    sys.exit(1)

try:
    import py7zr
except ImportError:
    print("Error: py7zr is required. Install with: uv pip install py7zr")
    sys.exit(1)

try:
    import duckdb
except ImportError:
    print("Error: duckdb is required. Install with: uv pip install duckdb")
    sys.exit(1)


def download_file(url: str, output_path: Path, chunk_size: int = 8192) -> None:
    """Download a file from URL with progress indication."""
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\rProgress: {percent:.1f}%", end='', flush=True)
    print()  # New line after progress


def extract_7z(archive_path: Path, extract_to: Path) -> None:
    """Extract a 7z archive."""
    print(f"Extracting {archive_path} to {extract_to}...")
    extract_to.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode='r') as archive:
        archive.extractall(path=extract_to)
    print("Extraction complete.")


def parse_xml_to_dicts(xml_file: Path) -> list[dict]:
    """Parse XML file and return list of dictionaries with row attributes."""
    print(f"Parsing {xml_file}...")
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    data = []
    for row in root.findall('row'):
        data.append(row.attrib)
    
    print(f"Parsed {len(data)} rows.")
    return data


def convert_types_for_schema(df, table_name: str):
    """Convert DataFrame columns to appropriate types based on Stack Exchange schema."""
    import pandas as pd
    
    # Common integer fields across tables
    int_fields = ['Id', 'PostTypeId', 'AcceptedAnswerId', 'ParentId', 'Score', 
                  'ViewCount', 'OwnerUserId', 'LastEditorUserId', 'AnswerCount',
                  'CommentCount', 'FavoriteCount', 'UserId', 'PostId', 'VoteTypeId',
                  'CommentId', 'BadgeId', 'TagId']
    
    # Date fields
    date_fields = ['CreationDate', 'LastEditDate', 'LastActivityDate', 'ClosedDate',
                   'CommunityOwnedDate', 'DeletionDate', 'Date']
    
    # Convert integer fields
    for field in int_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce').astype('Int64')  # Nullable integer
    
    # Convert date fields
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')
    
    return df


def convert_xml_to_parquet(xml_file: Path, parquet_file: Path, table_name: str) -> None:
    """Convert XML file to Parquet using DuckDB."""
    print(f"Converting {xml_file.name} to Parquet...")
    
    # Parse XML to list of dicts
    data = parse_xml_to_dicts(xml_file)
    
    if not data:
        print(f"Warning: No data found in {xml_file}")
        return
    
    # Convert to pandas DataFrame for easier handling
    import pandas as pd
    df = pd.DataFrame(data)
    
    # Convert data types according to Stack Exchange schema
    df = convert_types_for_schema(df, table_name)
    
    # Create DuckDB connection
    con = duckdb.connect()
    
    # Register DataFrame with DuckDB
    con.register('temp_df', df)
    
    # Write to Parquet using DuckDB
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    # Use absolute path for DuckDB (escape single quotes in path)
    abs_parquet_path = str(parquet_file.resolve()).replace("'", "''")
    con.execute(f"COPY (SELECT * FROM temp_df) TO '{abs_parquet_path}' (FORMAT PARQUET)")
    
    print(f"Converted {len(data)} rows to {parquet_file}")
    con.close()


def get_stackexchange_url(site: str) -> str:
    """Get the download URL for a Stack Exchange site."""
    return f"https://archive.org/download/stackexchange/{site}.stackexchange.com.7z"


def main():
    parser = argparse.ArgumentParser(
        description="Download and convert Stack Exchange dataset to Parquet format"
    )
    parser.add_argument(
        '--site',
        type=str,
        required=True,
        help='Stack Exchange site name (e.g., "stackoverflow", "math", "serverfault")'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for Parquet files (default: data-<site>)'
    )
    parser.add_argument(
        '--keep-archive',
        action='store_true',
        help='Keep the downloaded archive file after extraction'
    )
    
    args = parser.parse_args()
    
    # Setup paths
    script_dir = Path(__file__).parent
    site = args.site
    
    # Use data-<site> as default output directory
    if args.output_dir is None:
        output_dir = script_dir / f"data-{site}"
    else:
        output_dir = script_dir / args.output_dir
    
    archive_dir = script_dir / 'archives'
    extract_dir = script_dir / 'extracted'
    
    archive_dir.mkdir(exist_ok=True)
    extract_dir.mkdir(exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create .gitignore in output directory to ignore Parquet files
    gitignore_path = output_dir / '.gitignore'
    if not gitignore_path.exists():
        gitignore_path.write_text('*.parquet\n')
    
    archive_name = f"{site}.stackexchange.com.7z"
    archive_path = archive_dir / archive_name
    site_extract_dir = extract_dir / site
    
    # Step 1: Download the dataset
    if not archive_path.exists():
        url = get_stackexchange_url(site)
        try:
            download_file(url, archive_path)
        except Exception as e:
            print(f"Error downloading dataset: {e}")
            sys.exit(1)
    else:
        print(f"Archive already exists: {archive_path}")
    
    # Step 2: Extract the archive
    if not site_extract_dir.exists() or not any(site_extract_dir.iterdir()):
        try:
            extract_7z(archive_path, site_extract_dir)
        except Exception as e:
            print(f"Error extracting archive: {e}")
            sys.exit(1)
    else:
        print(f"Archive already extracted to: {site_extract_dir}")
    
    # Step 3: Convert XML files to Parquet
    # Use uppercase table names to match SQL queries
    xml_files = {
        'Posts.xml': 'Posts',
        'Users.xml': 'Users',
        'Comments.xml': 'Comments',
        'PostHistory.xml': 'PostHistory',
        'PostLinks.xml': 'PostLinks',
        'Tags.xml': 'Tags',
        'Votes.xml': 'Votes',
        'Badges.xml': 'Badges',
    }
    
    converted_count = 0
    for xml_filename, table_name in xml_files.items():
        xml_path = site_extract_dir / xml_filename
        if xml_path.exists():
            parquet_path = output_dir / f"{table_name}.parquet"
            try:
                convert_xml_to_parquet(xml_path, parquet_path, table_name)
                converted_count += 1
            except Exception as e:
                print(f"Error converting {xml_filename}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"Warning: {xml_filename} not found in extracted data")
    
    print(f"\nConversion complete! Converted {converted_count} files to Parquet format in {output_dir}")
    
    # Cleanup: Remove archive and extracted folders
    print("\nCleaning up temporary files...")
    
    # Remove archive file
    if not args.keep_archive:
        if archive_path.exists():
            print(f"Removing archive file: {archive_path}")
            archive_path.unlink()
    
    # Remove extracted directory
    if site_extract_dir.exists():
        print(f"Removing extracted directory: {site_extract_dir}")
        shutil.rmtree(site_extract_dir)
    
    # Remove parent directories if empty
    try:
        if archive_dir.exists() and not any(archive_dir.iterdir()):
            print(f"Removing empty archive directory: {archive_dir}")
            archive_dir.rmdir()
    except OSError:
        pass  # Directory not empty or other error, skip
    
    try:
        if extract_dir.exists() and not any(extract_dir.iterdir()):
            print(f"Removing empty extract directory: {extract_dir}")
            extract_dir.rmdir()
    except OSError:
        pass  # Directory not empty or other error, skip
    
    print("Cleanup complete!")


if __name__ == '__main__':
    main()

