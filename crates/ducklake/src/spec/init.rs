use sea_query::Table;

use super::entities::*;
use super::{LATEST_VERSION, metadata};
use crate::db::sea_query_ext::CreateTable;
use crate::{DucklakeResult, db, io};

macro_rules! create_table {
    ($tx:ident, $entity:ident) => {
        let query = Table::create_entity::<$entity>($tx.dialect());
        $tx.execute(&query).await?;
    };
}

#[derive(Default)]
pub struct Config {
    pub data_path: String,
}

/// Initialize a new catalog database with the most up-to-date DuckLake schema.
pub async fn init_catalog(pool: &db::Pool, config: Config) -> DucklakeResult<()> {
    let mut tx = pool.begin().await?;

    // Create all tables
    create_table!(tx, DucklakeColumn);
    create_table!(tx, DucklakeColumnMapping);
    create_table!(tx, DucklakeColumnTag);
    create_table!(tx, DucklakeDataFile);
    create_table!(tx, DucklakeDeleteFile);
    create_table!(tx, DucklakeFileColumnStats);
    create_table!(tx, DucklakeFilePartitionValue);
    create_table!(tx, DucklakeFilesScheduledForDeletion);
    create_table!(tx, DucklakeInlinedDataTables);
    create_table!(tx, DucklakeMetadata);
    create_table!(tx, DucklakeNameMapping);
    create_table!(tx, DucklakePartitionColumn);
    create_table!(tx, DucklakePartitionInfo);
    create_table!(tx, DucklakeSchema);
    create_table!(tx, DucklakeSchemaVersions);
    create_table!(tx, DucklakeSnapshot);
    create_table!(tx, DucklakeSnapshotChanges);
    create_table!(tx, DucklakeTable);
    create_table!(tx, DucklakeTableColumnStats);
    create_table!(tx, DucklakeTableStats);
    create_table!(tx, DucklakeTag);
    create_table!(tx, DucklakeView);
    create_table!(tx, DucklakeMacro);
    create_table!(tx, DucklakeMacroImpl);
    create_table!(tx, DucklakeMacroParameters);
    create_table!(tx, DucklakeSortInfo);
    create_table!(tx, DucklakeSortExpression);
    create_table!(tx, DucklakeFileVariantStats);

    // Populate the table
    populate(&mut tx, &config.data_path).await?;

    // Commit all changes
    tx.commit().await?;
    Ok(())
}

async fn populate(tx: &mut db::Transaction, data_path: &str) -> DucklakeResult<()> {
    // Basic metadata
    tx.insert_entities([
        DucklakeMetadata {
            key: metadata::VERSION.to_string(),
            value: LATEST_VERSION.to_string(),
            scope: None,
            scope_id: None,
        },
        DucklakeMetadata {
            key: metadata::CREATED_BY.to_string(),
            value: format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            scope: None,
            scope_id: None,
        },
        DucklakeMetadata {
            key: metadata::DATA_PATH.to_string(),
            value: data_path
                .parse::<io::DucklakePath>()?
                .ensure_directory()
                .to_string(),
            scope: None,
            scope_id: None,
        },
        DucklakeMetadata {
            key: metadata::ENCRYPTED.to_string(),
            value: "false".to_string(),
            scope: None,
            scope_id: None,
        },
    ])
    .await?;

    // Initial schema
    tx.insert_entity(DucklakeSchema {
        schema_id: 0,
        schema_uuid: Some(db::UuidText::now_v7()),
        schema_name: "main".to_string(),
        begin_snapshot: 0,
        end_snapshot: None,
        path: "main/".to_string(),
        path_is_relative: true,
    })
    .await?;

    // Initial snapshot
    tx.insert_entity(DucklakeSnapshot {
        snapshot_id: 0,
        snapshot_time: db::UtcDateTime::now(),
        schema_version: 0,
        next_catalog_id: 1,
        next_file_id: 0,
    })
    .await?;

    // Snapshot changes
    tx.insert_entity(DucklakeSnapshotChanges {
        snapshot_id: 0,
        changes_made: "created_schema:\"main\"".to_string(),
        author: None,
        commit_message: None,
        commit_extra_info: None,
    })
    .await?;

    Ok(())
}
