use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::variant::{VariantArray, json_to_variant};
use walkdir::WalkDir;

const DEFAULT_BATCH_BYTES: usize = 100 * 1024 * 1024;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let sources = collect_sources(&cli.input, cli.recursive)?;
    if sources.is_empty() {
        bail!(
            "no JSON files found under {} (use --recursive if needed)",
            cli.input.display()
        );
    }

    let mut writer = VariantBatchWriter::new(cli.output.clone(), cli.batch_bytes);
    for source in sources {
        process_source(&source, cli.format, &mut writer)?;
    }

    let written = writer.finish()?;
    if written == 0 {
        bail!("no JSON records were found in {}", cli.input.display());
    }

    println!("Wrote {written} rows to {}", cli.output.display());
    Ok(())
}

#[derive(Parser, Debug)]
#[command(
    name = "json-to-variant",
    version,
    about = "Convert JSON documents into a parquet Variant column"
)]
struct Cli {
    /// Path to a JSON file or directory that holds JSON files
    #[arg(value_name = "INPUT")]
    input: PathBuf,
    /// Path to the parquet file that will be produced
    #[arg(value_name = "OUTPUT")]
    output: PathBuf,
    /// How to interpret JSON files (auto-detect, NDJSON lines, or single JSON blob)
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    format: InputFormat,
    /// Recurse into nested folders when INPUT is a directory
    #[arg(long)]
    recursive: bool,
    /// Target batch size in bytes before flushing to parquet
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_BATCH_BYTES)]
    batch_bytes: usize,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum InputFormat {
    Auto,
    Ndjson,
    Single,
}

enum FileFormat {
    Ndjson,
    Single,
}

fn collect_sources(input: &Path, recursive: bool) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }
    if !input.is_dir() {
        bail!("{} is not a file or directory", input.display());
    }

    let mut files = Vec::new();
    if recursive {
        for entry in WalkDir::new(input) {
            let entry = entry?;
            if entry.file_type().is_file() && looks_like_json(entry.path()) {
                files.push(entry.path().to_path_buf());
            }
        }
    } else {
        for entry in fs::read_dir(input)? {
            let entry = entry?;
            if entry.file_type()?.is_file() && looks_like_json(&entry.path()) {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    Ok(files)
}

fn process_source(
    path: &Path,
    requested: InputFormat,
    writer: &mut VariantBatchWriter,
) -> Result<()> {
    let format = match requested {
        InputFormat::Auto => infer_file_format(path),
        InputFormat::Ndjson => FileFormat::Ndjson,
        InputFormat::Single => FileFormat::Single,
    };

    match format {
        FileFormat::Single => process_single_json(path, writer),
        FileFormat::Ndjson => process_ndjson_file(path, writer),
    }
}

fn process_single_json(path: &Path, writer: &mut VariantBatchWriter) -> Result<()> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    writer.push_row(trimmed.to_string())
}

fn process_ndjson_file(path: &Path, writer: &mut VariantBatchWriter) -> Result<()> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut saw_row = false;
    for line in reader.lines() {
        let line = line.with_context(|| format!("failed to read line from {}", path.display()))?;
        let entry = line.trim();
        if entry.is_empty() {
            continue;
        }
        saw_row = true;
        writer.push_row(entry.to_string())?;
    }
    if !saw_row {
        bail!("no NDJSON rows found in {}", path.display());
    }
    Ok(())
}

fn infer_file_format(path: &Path) -> FileFormat {
    if has_ndjson_hint(path) {
        FileFormat::Ndjson
    } else {
        FileFormat::Single
    }
}

fn has_ndjson_hint(path: &Path) -> bool {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => matches!(
            ext.to_ascii_lowercase().as_str(),
            "jsonl" | "ndjson" | "jsonlines"
        ),
        None => false,
    }
}

fn looks_like_json(path: &Path) -> bool {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => matches!(
            ext.to_ascii_lowercase().as_str(),
            "json" | "jsonl" | "ndjson" | "jsons" | "jsonlines"
        ),
        None => true,
    }
}

struct VariantBatchWriter {
    output: PathBuf,
    target_bytes: usize,
    pending_rows: Vec<String>,
    pending_bytes: usize,
    writer: Option<ArrowWriter<File>>,
    total_rows: usize,
    writer_props: WriterProperties,
}

impl VariantBatchWriter {
    fn new(output: PathBuf, target_bytes: usize) -> Self {
        let target = target_bytes.max(1);
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        Self {
            output,
            target_bytes: target,
            pending_rows: Vec::new(),
            pending_bytes: 0,
            writer: None,
            total_rows: 0,
            writer_props,
        }
    }

    fn push_row(&mut self, row: String) -> Result<()> {
        self.pending_bytes += row.len();
        self.pending_rows.push(row);
        if self.pending_bytes >= self.target_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }

        let rows = std::mem::take(&mut self.pending_rows);
        let batch = build_batch(rows)?;
        self.pending_bytes = 0;

        if self.writer.is_none() {
            let file = File::create(&self.output)
                .with_context(|| format!("unable to create {}", self.output.display()))?;
            let schema = batch.schema();
            let writer =
                ArrowWriter::try_new(file, schema.clone(), Some(self.writer_props.clone()))
                    .context("failed to initialize parquet writer")?;
            self.writer = Some(writer);
        }

        if let Some(writer) = self.writer.as_mut() {
            writer
                .write(&batch)
                .context("unable to write record batch")?;
        }

        self.total_rows += batch.num_rows();
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush()?;
        if let Some(writer) = self.writer.take() {
            writer
                .close()
                .context("failed to finalize parquet writer")?;
        }
        Ok(self.total_rows)
    }
}

fn build_batch(rows: Vec<String>) -> Result<RecordBatch> {
    let values: Vec<Option<String>> = rows.into_iter().map(Some).collect();
    let json_array: ArrayRef = Arc::new(StringArray::from(values));

    let variant_array: VariantArray =
        json_to_variant(&json_array).context("unable to convert JSON rows into Variant data")?;
    let data_field = variant_array.field("data").clone();
    let schema = Arc::new(Schema::new(vec![data_field.as_ref().clone()]));

    let column: ArrayRef = variant_array.into();
    RecordBatch::try_new(schema, vec![column]).context("failed to build Arrow record batch")
}
