//! Rows a statement writes: each table row it changes plus each index row that change rewrites.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
};

use anyhow::Result;
use rusqlite::{Connection, hooks::AuthAction};

const INDEX_COLUMNS: &str = "SELECT il.name, ii.name
    FROM pragma_index_list(?1) AS il, pragma_index_info(il.name) AS ii
    WHERE il.origin <> 'pk' OR NOT (SELECT wr FROM pragma_table_list(?1))";

/// The table one statement writes and, for an UPDATE or upsert, the columns it sets.
#[derive(Default)]
pub(super) struct WriteTarget {
    table: Option<String>,
    columns: BTreeSet<String>,
}

impl WriteTarget {
    pub(super) fn record(&mut self, action: &AuthAction<'_>) {
        match *action {
            AuthAction::Insert { table_name } | AuthAction::Delete { table_name } => {
                self.write(table_name)
            }
            AuthAction::Update {
                table_name,
                column_name,
            } => {
                self.write(table_name);
                self.columns.insert(column_name.to_owned());
            }
            _ => {}
        }
    }

    fn write(&mut self, table: &str) {
        if !table.starts_with("sqlite_") {
            self.table.get_or_insert_with(|| table.to_owned());
        }
    }
}

/// Each table's indexes as the columns they cover, read once per schema version. A rollback restores the
/// version, so indexes it removed are read again.
#[derive(Default)]
pub(super) struct Indexes {
    version: Cell<Option<i64>>,
    tables: RefCell<BTreeMap<String, Vec<BTreeSet<String>>>>,
}

impl Indexes {
    /// Index rows the statement that just finished rewrote: every index for an insert or delete, and only the
    /// indexes covering a column it sets for an update. An upsert counts as an update, which most are.
    pub(super) fn written(&self, connection: &Connection, target: &WriteTarget) -> Result<u64> {
        let Some(table) = &target.table else {
            return Ok(0);
        };
        let changes = connection.changes();
        if changes == 0 {
            return Ok(0);
        }
        Ok(changes * self.touched(connection, table, &target.columns)?)
    }

    fn touched(&self, connection: &Connection, table: &str, set: &BTreeSet<String>) -> Result<u64> {
        let version = connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        if self.version.replace(Some(version)) != Some(version) {
            self.tables.borrow_mut().clear();
        }
        if !self.tables.borrow().contains_key(table) {
            let indexes = read(connection, table)?;
            self.tables.borrow_mut().insert(table.to_owned(), indexes);
        }
        let touched = self.tables.borrow()[table]
            .iter()
            .filter(|columns| set.is_empty() || !columns.is_disjoint(set))
            .count();
        Ok(touched as u64)
    }
}

fn read(connection: &Connection, table: &str) -> Result<Vec<BTreeSet<String>>> {
    let mut indexes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut statement = connection.prepare(INDEX_COLUMNS)?;
    let mut rows = statement.query([table])?;
    while let Some(row) = rows.next()? {
        let columns = indexes.entry(row.get(0)?).or_default();
        columns.extend(row.get::<_, Option<String>>(1)?);
    }
    Ok(indexes.into_values().collect())
}
