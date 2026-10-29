//! The declared shape of a dataset: one table, its columns and the time axis
//! that the hot store cools along.
//!
//! A dataset *is* one two-dimensional table: it is declared once, at dataset
//! creation, and never changes. Cold files are immutable, so the schema has to
//! be stable for every file already written. The table declares one primary key
//! (what "the same row" means) and one timestamp column (when the information
//! was produced, supplied by the writer); that timestamp is what partitions the
//! cold layout.

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::types::Value as SqlValue;
use serde::{Deserialize, Serialize};

/// Longest accepted identifier.
pub const MAX_IDENTIFIER: usize = 64;
/// The single `SQLite` table inside every hot database.
pub const HOT_TABLE: &str = "data";
/// Columns one table may declare.
pub const MAX_COLUMNS: usize = 64;

/// The column types the storage layer understands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    /// 64-bit signed integer.
    Integer,
    /// 64-bit float.
    Real,
    /// UTF-8 text.
    Text,
    /// `0` / `1`.
    Boolean,
    /// RFC 3339 instant, normalised to UTC microseconds.
    Timestamp,
}

impl ColumnType {
    /// The `SQLite` storage class backing this type.
    pub fn storage_class(self) -> &'static str {
        match self {
            Self::Integer | Self::Boolean => "INTEGER",
            Self::Real => "REAL",
            Self::Text | Self::Timestamp => "TEXT",
        }
    }
}

/// The time granularity a table is partitioned by in cold storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    /// One partition per hour.
    Hour,
    /// One partition per day.
    Day,
    /// One partition per month.
    Month,
}

impl Granularity {
    /// The partition key used in object paths (`hour=2026-10-01T13`).
    pub fn key(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }
}

/// One declared column.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnSpec {
    /// Column name; lower snake case.
    pub name: String,
    /// Declared type.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Whether writers may omit the column.
    #[serde(default)]
    pub nullable: bool,
}

/// The one table a dataset declares.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableSpec {
    /// Columns that identify a row. Writers upsert against this key.
    pub primary_key: Vec<String>,
    /// The timestamp column the hot window cools along.
    pub time_column: String,
    /// Cold partition granularity of [`TableSpec::time_column`].
    pub granularity: Granularity,
    /// Declared columns, in cold-file order.
    pub columns: Vec<ColumnSpec>,
}

impl TableSpec {
    /// The declared columns of the primary key, in declaration order.
    ///
    /// # Errors
    ///
    /// Returns an error when a primary key column is not declared.
    pub fn key_columns(&self) -> Result<Vec<&ColumnSpec>, String> {
        self.primary_key
            .iter()
            .map(|name| {
                self.column(name)
                    .ok_or_else(|| format!("unknown primary key column {name}"))
            })
            .collect()
    }

    /// Looks up one declared column.
    pub fn column(&self, name: &str) -> Option<&ColumnSpec> {
        self.columns.iter().find(|column| column.name == name)
    }
}

/// A value in one cell, already coerced to its declared type.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Absent value.
    Null,
    /// Integer cell.
    Integer(i64),
    /// Float cell.
    Real(f64),
    /// Text cell.
    Text(String),
    /// Boolean cell.
    Boolean(bool),
    /// Timestamp cell, normalised to UTC microseconds.
    Timestamp(String),
}

impl Value {
    /// Maps the value onto `SQLite`.
    pub fn to_sql(&self) -> SqlValue {
        match self {
            Self::Null => SqlValue::Null,
            Self::Integer(value) => SqlValue::Integer(*value),
            Self::Real(value) => SqlValue::Real(*value),
            Self::Text(value) | Self::Timestamp(value) => SqlValue::Text(value.clone()),
            Self::Boolean(value) => SqlValue::Integer(i64::from(*value)),
        }
    }

    /// Maps the value onto JSON for API responses.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Integer(value) => serde_json::Value::from(*value),
            Self::Real(value) => serde_json::Value::from(*value),
            Self::Text(value) | Self::Timestamp(value) => serde_json::Value::from(value.clone()),
            Self::Boolean(value) => serde_json::Value::from(*value),
        }
    }

    /// Rebuilds a value from its `SQLite` representation.
    ///
    /// # Errors
    ///
    /// Returns an error when a stored value does not match the declared type.
    pub fn from_sql(column: &ColumnSpec, stored: SqlValue) -> Result<Self, String> {
        let value = match (column.column_type, stored) {
            (_, SqlValue::Null) => Self::Null,
            (ColumnType::Integer, SqlValue::Integer(value)) => Self::Integer(value),
            (ColumnType::Boolean, SqlValue::Integer(value)) => Self::Boolean(value != 0),
            (ColumnType::Real, SqlValue::Real(value)) => Self::Real(value),
            (ColumnType::Real, SqlValue::Integer(value)) => Self::Real(value as f64),
            (ColumnType::Text, SqlValue::Text(value)) => Self::Text(value),
            (ColumnType::Timestamp, SqlValue::Text(value)) => Self::Timestamp(value),
            (declared, stored) => {
                return Err(format!(
                    "column {} holds {stored:?}, which is not a {declared:?}",
                    column.name
                ));
            }
        };
        Ok(value)
    }
}

/// Coerces one JSON cell into the declared column type.
///
/// # Errors
///
/// Returns an error when the value does not match the declared type or when a
/// required column is missing.
pub fn coerce(column: &ColumnSpec, cell: Option<&serde_json::Value>) -> Result<Value, String> {
    let Some(cell) = cell else {
        return if column.nullable {
            Ok(Value::Null)
        } else {
            Err(format!("column {} is required", column.name))
        };
    };
    if cell.is_null() {
        return if column.nullable {
            Ok(Value::Null)
        } else {
            Err(format!("column {} is required", column.name))
        };
    }
    let value = match column.column_type {
        ColumnType::Integer => cell
            .as_i64()
            .map(Value::Integer)
            .ok_or_else(|| format!("column {} expects an integer", column.name))?,
        ColumnType::Real => cell
            .as_f64()
            .map(Value::Real)
            .ok_or_else(|| format!("column {} expects a number", column.name))?,
        ColumnType::Text => cell
            .as_str()
            .map(|text| Value::Text(text.to_owned()))
            .ok_or_else(|| format!("column {} expects a string", column.name))?,
        ColumnType::Boolean => cell
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| format!("column {} expects a boolean", column.name))?,
        ColumnType::Timestamp => cell
            .as_str()
            .and_then(normalize_timestamp)
            .map(Value::Timestamp)
            .ok_or_else(|| format!("column {} expects an RFC 3339 timestamp", column.name))?,
    };
    Ok(value)
}

/// Normalises an RFC 3339 instant to fixed-width UTC microseconds.
///
/// Fixed width keeps the textual form sortable, which is what makes range
/// scans and the cooling boundary plain string comparisons.
pub fn normalize_timestamp(raw: &str) -> Option<String> {
    parse_timestamp(raw).map(|moment| moment.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// Parses an RFC 3339 instant into UTC.
pub(crate) fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|moment| moment.with_timezone(&Utc))
}

/// Checks one identifier.
pub fn valid_identifier(name: &str) -> bool {
    (1..=MAX_IDENTIFIER).contains(&name.len())
        && name.starts_with(|first: char| first.is_ascii_lowercase())
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

/// Validates the table schema of a dataset.
///
/// # Errors
///
/// Returns a human readable message naming the first problem found.
pub fn validate(table: &TableSpec) -> Result<(), String> {
    if table.columns.is_empty() || table.columns.len() > MAX_COLUMNS {
        return Err(format!(
            "a table declares between 1 and {MAX_COLUMNS} columns"
        ));
    }
    let mut columns = std::collections::HashSet::new();
    for column in &table.columns {
        if !valid_identifier(&column.name) {
            return Err(format!("invalid column name {}", column.name));
        }
        if !columns.insert(column.name.as_str()) {
            return Err(format!("duplicate column {}", column.name));
        }
    }
    if table.primary_key.is_empty() {
        return Err("a table declares a primary key".to_owned());
    }
    for key in &table.primary_key {
        if table.column(key).is_none() {
            return Err(format!("the primary key names unknown column {key}"));
        }
    }
    let Some(time) = table.column(&table.time_column) else {
        return Err(format!("unknown time column {}", table.time_column));
    };
    if time.column_type != ColumnType::Timestamp {
        return Err(format!(
            "time column {} must be declared as timestamp",
            table.time_column
        ));
    }
    if time.nullable {
        return Err(format!(
            "time column {} cannot be nullable",
            table.time_column
        ));
    }
    if !table.primary_key.contains(&table.time_column) {
        return Err(format!(
            "the primary key must include {}",
            table.time_column
        ));
    }
    Ok(())
}

/// Statements that create the hot table of one dataset.
///
/// Every dataset keeps its rows in a single `data` table: one dataset is one
/// two-dimensional table, so there is nothing to name and nothing to join.
pub fn create_statements(table: &TableSpec) -> Vec<String> {
    let mut definitions = vec!["\"_flush\" INTEGER".to_owned()];
    for column in &table.columns {
        definitions.push(format!(
            "\"{}\" {} {}",
            column.name,
            column.column_type.storage_class(),
            if column.nullable { "NULL" } else { "NOT NULL" }
        ));
    }
    let key = table
        .primary_key
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS \"{HOT_TABLE}\" ({})",
            definitions.join(", ")
        ),
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"data_primary_key\" ON \"{HOT_TABLE}\" ({key})"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS \"data_time\" ON \"{HOT_TABLE}\" (\"{}\")",
            table.time_column
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnSpec, ColumnType, Granularity, TableSpec, coerce, normalize_timestamp, validate,
    };

    fn column(name: &str, column_type: ColumnType) -> ColumnSpec {
        ColumnSpec {
            name: name.to_owned(),
            column_type,
            nullable: false,
        }
    }

    fn klines() -> TableSpec {
        TableSpec {
            primary_key: vec!["symbol".to_owned(), "ts".to_owned()],
            time_column: "ts".to_owned(),
            granularity: Granularity::Day,
            columns: vec![
                column("symbol", ColumnType::Text),
                column("ts", ColumnType::Timestamp),
                column("close", ColumnType::Real),
            ],
        }
    }

    #[test]
    fn timestamps_normalise_to_fixed_width_utc() {
        assert_eq!(
            normalize_timestamp("2026-10-01T13:45:12+08:00").as_deref(),
            Some("2026-10-01T05:45:12.000000Z")
        );
        assert_eq!(normalize_timestamp("not a timestamp"), None);
        let earlier = normalize_timestamp("2026-10-01T05:45:12.000001Z").expect("parses");
        let later = normalize_timestamp("2026-10-01T05:45:12.000002Z").expect("parses");
        assert!(
            earlier < later,
            "fixed width keeps text order chronological"
        );
    }

    #[test]
    fn validation_rejects_declarations_that_would_break_the_cold_layout() {
        assert!(validate(&klines()).is_ok());

        let mut missing_key = klines();
        missing_key.primary_key = vec!["symbol".to_owned()];
        assert!(validate(&missing_key).unwrap_err().contains("primary key"));

        let mut time_not_timestamp = klines();
        time_not_timestamp.columns[1] = column("ts", ColumnType::Text);
        assert!(validate(&time_not_timestamp).is_err());

        let mut nullable_time = klines();
        nullable_time.columns[1].nullable = true;
        assert!(validate(&nullable_time).is_err());
    }

    #[test]
    fn coercion_follows_the_declared_types() {
        let symbol = column("symbol", ColumnType::Text);
        assert_eq!(
            coerce(&symbol, Some(&serde_json::json!("BTCUSDT"))).expect("text"),
            super::Value::Text("BTCUSDT".to_owned())
        );
        assert!(coerce(&symbol, Some(&serde_json::json!(1))).is_err());

        let ts = column("ts", ColumnType::Timestamp);
        assert_eq!(
            coerce(&ts, Some(&serde_json::json!("2026-10-01T13:45:12+08:00"))).expect("timestamp"),
            super::Value::Timestamp("2026-10-01T05:45:12.000000Z".to_owned())
        );
        assert!(coerce(&ts, Some(&serde_json::json!("nope"))).is_err());
        assert!(coerce(&ts, None).is_err());
    }
}
