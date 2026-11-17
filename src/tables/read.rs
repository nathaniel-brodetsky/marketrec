//! Reading the tables back: for `verify`, for tests, and as the reference for readers in
//! other languages.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use super::TablesError;
use super::manifest::DayManifest;
use super::schema::Table;

/// One finished part: its rows and its key-value metadata.
#[derive(Debug, Clone)]
pub struct PartData {
    pub path: PathBuf,
    pub batch: RecordBatch,
    pub metadata: HashMap<String, String>,
}

/// Finished parts of `table` in a day directory, in name order. Unfinished `.tmp` parts
/// are not listed.
pub fn parts(day: &Path, table: Table) -> Result<Vec<PathBuf>, TablesError> {
    let dir = day.join(table.name());
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// One part, checked against the schema of `table`.
pub fn read_part(path: &Path, table: Table) -> Result<PartData, TablesError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let metadata = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();
    let expected = table.schema();
    let fields = |s: &arrow_schema::Schema| s.fields().iter().cloned().collect::<Vec<_>>();
    if fields(builder.schema()) != fields(&expected) {
        return Err(TablesError::Manifest(format!(
            "{}: columns differ from the {} schema",
            path.display(),
            table.name()
        )));
    }
    let batches = builder
        .with_batch_size(64 * 1024)
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    let batch = concat_batches(&expected, &batches)?;
    Ok(PartData {
        path: path.to_owned(),
        batch,
        metadata,
    })
}

/// All rows of `table` in a day directory, parts in name order.
pub fn read_table(day: &Path, table: Table) -> Result<RecordBatch, TablesError> {
    let mut batches = Vec::new();
    for path in parts(day, table)? {
        batches.push(read_part(&path, table)?.batch);
    }
    Ok(concat_batches(&table.schema(), &batches)?)
}

pub fn read_manifest(day: &Path) -> Result<DayManifest, TablesError> {
    let path = day.join("manifest.json");
    let text = fs::read_to_string(&path)?;
    serde_json::from_str(&text)
        .map_err(|e| TablesError::Manifest(format!("{}: {e}", path.display())))
}

/// Day directories under a symbol directory (`data/binance/BTCUSDT`), in date order.
pub fn days(symbol_root: &Path) -> Result<Vec<PathBuf>, TablesError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(symbol_root)? {
        let path = entry?.path();
        if path.is_dir() && path.join("manifest.json").exists() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// The rows of a table batch by batch, parts in the given order, optionally only some
/// columns: memory stays bounded for tables of any size.
pub struct TableStream {
    table: Table,
    parts: std::collections::VecDeque<PathBuf>,
    columns: Option<Vec<usize>>,
    current: Option<ParquetRecordBatchReader>,
}

impl TableStream {
    /// `columns`: indices into `table.schema()`, or `None` for all.
    pub fn new(table: Table, parts: Vec<PathBuf>, columns: Option<Vec<usize>>) -> Self {
        Self {
            table,
            parts: parts.into(),
            columns,
            current: None,
        }
    }

    fn open(&self, path: &Path) -> Result<ParquetRecordBatchReader, TablesError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        let fields = |s: &arrow_schema::Schema| s.fields().iter().cloned().collect::<Vec<_>>();
        if fields(builder.schema()) != fields(&self.table.schema()) {
            return Err(TablesError::Manifest(format!(
                "{}: columns differ from the {} schema",
                path.display(),
                self.table.name()
            )));
        }
        let builder = match &self.columns {
            Some(c) => {
                let mask = ProjectionMask::roots(builder.parquet_schema(), c.iter().copied());
                builder.with_projection(mask)
            }
            None => builder,
        };
        Ok(builder.with_batch_size(64 * 1024).build()?)
    }
}

impl Iterator for TableStream {
    type Item = Result<RecordBatch, TablesError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => return Some(Ok(batch)),
                    Some(Err(e)) => return Some(Err(e.into())),
                    None => self.current = None,
                }
            }
            let path = self.parts.pop_front()?;
            match self.open(&path) {
                Ok(reader) => self.current = Some(reader),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Compare two row streams of the same table. Returns the first differing row number, or
/// `None` if they hold the same rows; a shorter stream differs at its end.
pub fn first_difference(
    a: impl Iterator<Item = Result<RecordBatch, TablesError>>,
    b: impl Iterator<Item = Result<RecordBatch, TablesError>>,
) -> Result<Option<u64>, TablesError> {
    let (mut a, mut b) = (a, b);
    let (mut ba, mut bb): (Option<RecordBatch>, Option<RecordBatch>) = (None, None);
    let (mut oa, mut ob, mut row) = (0usize, 0usize, 0u64);
    loop {
        if ba.as_ref().is_none_or(|x| oa == x.num_rows()) {
            ba = a.next().transpose()?;
            oa = 0;
        }
        if bb.as_ref().is_none_or(|x| ob == x.num_rows()) {
            bb = b.next().transpose()?;
            ob = 0;
        }
        match (&ba, &bb) {
            (None, None) => return Ok(None),
            (Some(x), Some(y)) => {
                let n = (x.num_rows() - oa).min(y.num_rows() - ob);
                if n == 0 {
                    continue; // an empty batch
                }
                let (sx, sy) = (x.slice(oa, n), y.slice(ob, n));
                if sx != sy {
                    let i = (0..n)
                        .find(|&i| sx.slice(i, 1) != sy.slice(i, 1))
                        .unwrap_or(0);
                    return Ok(Some(row + i as u64));
                }
                oa += n;
                ob += n;
                row += n as u64;
            }
            (Some(x), None) if x.num_rows() == oa => continue,
            (None, Some(y)) if y.num_rows() == ob => continue,
            _ => return Ok(Some(row)),
        }
    }
}
