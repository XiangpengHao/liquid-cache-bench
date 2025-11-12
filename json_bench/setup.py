#!/usr/bin/env python3
"""
Download and convert JSONBench dataset to Parquet format.

Usage:
    uv run --with requests python json_bench/setup.py --size <size>
"""

import argparse
import gzip
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: requests is required. Install with: uv pip install requests")
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


def decompress_gz_file(gz_path: Path, output_path: Path) -> None:
    """Decompress a .gz file."""
    print(f"Decompressing {gz_path.name}...")
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_jsonbench_urls(size: str) -> tuple:
    """Get download URLs for JSONBench dataset based on size.
    
    JSONBench dataset is hosted on S3 at clickhouse-public-datasets.s3.amazonaws.com
    Files are compressed as .json.gz and named file_XXXX.json.gz
    """
    # Base URL for JSONBench files on S3
    base_url = "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky"
    
    # Map size to file patterns
    # Files are compressed as .json.gz
    size_configs = {
        '1m': {'files': 1, 'description': '1 million rows'},
        '10m': {'files': 10, 'description': '10 million rows'},
        '100m': {'files': 100, 'description': '100 million rows'},
        '1000m': {'files': 1000, 'description': '1 billion rows'},
    }
    
    if size not in size_configs:
        raise ValueError(f"Invalid size: {size}. Must be one of: {', '.join(size_configs.keys())}")
    
    config = size_configs[size]
    urls = []
    
    # Generate URLs for each file
    # Files are numbered starting from 0001 and are .json.gz compressed
    for i in range(1, config['files'] + 1):
        filename = f"file_{i:04d}.json.gz"
        url = f"{base_url}/{filename}"
        urls.append((url, filename))
    
    return urls, config['description']


def build_json_to_variant_tool(tool_dir: Path) -> Path:
    """Build the json_to_variant Rust tool."""
    print("Building json_to_variant tool...")
    result = subprocess.run(
        ['cargo', 'build', '--release'],
        cwd=tool_dir,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error building json_to_variant tool:")
        print(result.stderr)
        sys.exit(1)
    
    # Find the binary
    binary_path = tool_dir / 'target' / 'release' / 'json_to_variant'
    if not binary_path.exists():
        # Try alternative name
        binary_path = tool_dir / 'target' / 'release' / 'json-to-variant'
    
    if not binary_path.exists():
        raise FileNotFoundError(f"Could not find built binary in {tool_dir / 'target' / 'release'}")
    
    print(f"Built tool: {binary_path}")
    return binary_path


def main():
    parser = argparse.ArgumentParser(
        description="Download and convert JSONBench dataset to Parquet format"
    )
    parser.add_argument(
        '--size',
        type=str,
        default='1m',
        choices=['1m', '10m', '100m', '1000m'],
        help='Dataset size: 1m (1M rows), 10m (10M rows), 100m (100M rows), 1000m (1B rows). Default: 1m'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for Parquet files (default: data-bluesky-<size>)'
    )
    parser.add_argument(
        '--keep-json',
        action='store_true',
        help='Keep JSON files after conversion (default: remove them)'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download step and use existing JSON files'
    )
    
    args = parser.parse_args()
    
    # Setup paths
    script_dir = Path(__file__).parent
    size = args.size
    
    # Use data-bluesky-<size> as default output directory
    if args.output_dir is None:
        output_dir = script_dir / f"data-bluesky-{size}"
    else:
        output_dir = script_dir / args.output_dir
    
    download_dir = script_dir / 'downloads'
    json_dir = script_dir / f'json-{size}'
    tool_dir = script_dir / 'json_to_variant'
    
    download_dir.mkdir(exist_ok=True)
    json_dir.mkdir(exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create .gitignore in output directory
    gitignore_path = output_dir / '.gitignore'
    if not gitignore_path.exists():
        gitignore_path.write_text('*.parquet\n')
    
    # Get download URLs
    try:
        urls, description = get_jsonbench_urls(size)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"Dataset size: {description} ({len(urls)} files)")
    
    # Step 1: Download JSON files (unless skipped)
    if not args.skip_download:
        print(f"\nStep 1: Downloading {len(urls)} JSON files (compressed as .gz)...")
        downloaded_files = []
        for url, filename in urls:
            download_path = download_dir / filename
            gz_path = json_dir / filename  # Files are downloaded as .gz
            
            # Skip if already downloaded
            if gz_path.exists():
                print(f"  Skipping {filename} (already exists)")
                downloaded_files.append(gz_path)
                continue
            
            # Download
            try:
                download_file(url, download_path)
                # Move to json_dir
                download_path.rename(gz_path)
                downloaded_files.append(gz_path)
            except Exception as e:
                print(f"  Error downloading {filename}: {e}")
                # Try to continue with other files
                continue
        
        print(f"Downloaded {len(downloaded_files)} files")
    else:
        print(f"\nStep 1: Skipping download (using existing files)")
    
    # Step 2: Decompress .gz files
    print(f"\nStep 2: Decompressing .gz files...")
    json_files = []
    
    # Find all .gz files that need decompression
    gz_files = sorted(json_dir.glob("*.json.gz"))
    decompressed_count = 0
    
    for gz_path in gz_files:
        output_json = json_dir / gz_path.stem  # Remove .gz extension to get .json
        if output_json.exists():
            print(f"  Skipping {gz_path.name} (already decompressed)")
            json_files.append(output_json)
        else:
            try:
                decompress_gz_file(gz_path, output_json)
                json_files.append(output_json)
                decompressed_count += 1
                # Remove .gz file after successful decompression (unless --keep-json is set)
                # Actually, --keep-json means keep JSON, so we remove .gz to save space
                if not args.keep_json:
                    gz_path.unlink()
            except Exception as e:
                print(f"  Error decompressing {gz_path.name}: {e}")
                continue
    
    # Also check for any existing .json files (not from .gz)
    for json_path in json_dir.glob("*.json"):
        if json_path not in json_files and not json_path.name.endswith('.json.gz'):
            json_files.append(json_path)
    
    print(f"Decompressed {decompressed_count} files, found {len(json_files)} JSON files total")
    
    if not json_files:
        print("Error: No JSON files found after download/decompression")
        sys.exit(1)
    
    print(f"Found {len(json_files)} JSON files ready for conversion")
    
    # Step 3: Build json_to_variant tool
    print(f"\nStep 3: Building json_to_variant conversion tool...")
    try:
        tool_binary = build_json_to_variant_tool(tool_dir)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Step 4: Convert JSON to Parquet
    print(f"\nStep 4: Converting JSON files to Parquet...")
    output_parquet = output_dir / 'bluesky.parquet'
    
    try:
        # Use json_to_variant tool to convert all JSON files to a single Parquet file
        # The tool can process a directory of JSON files
        cmd = [
            str(tool_binary),
            str(json_dir),
            str(output_parquet),
            '--recursive',
            '--format', 'ndjson',  # JSONBench files are NDJSON
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error converting to Parquet:")
            print(result.stderr)
            sys.exit(1)
        
        print(result.stdout)
        print(f"\n✓ Conversion complete! Parquet file: {output_parquet}")
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Step 5: Cleanup
    print(f"\nStep 5: Cleaning up...")
    
    # Remove download directory
    if download_dir.exists():
        print(f"Removing download directory: {download_dir}")
        shutil.rmtree(download_dir)
    
    # Remove JSON files unless --keep-json is specified
    if not args.keep_json:
        if json_dir.exists():
            print(f"Removing JSON directory: {json_dir}")
            shutil.rmtree(json_dir)
    else:
        print(f"Keeping JSON files in: {json_dir}")
    
    print("\n" + "=" * 60)
    print(f"Setup complete!")
    print(f"  Dataset size: {description}")
    print(f"  Parquet file: {output_parquet}")
    print(f"  Output directory: {output_dir}")


if __name__ == '__main__':
    main()

