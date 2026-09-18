use std::collections::HashMap;

use indexmap::IndexMap;
use rstest::{fixture, rstest};
use sea_query::{ExprTrait, Query};

use super::*;
use crate::{
    BucketFilter,
    Column,
    CreateOptions,
    DataFileStatistics,
    DataType,
    Ducklake,
    IfExistsStrategy,
    PartitionColumn,
    PartitionTransform,
    Table,
    Value,
    WriteDataFile,
};

#[fixture]
async fn bucket_table() -> Table {
    let lake = Ducklake::create(CreateOptions::new(
        "sqlite://:memory:",
        std::env::current_dir().unwrap().to_str().unwrap(),
    ))
    .await
    .unwrap();
    let table = lake
        .create_table(
            "bucket_test",
            vec![
                Column::new("region".into(), DataType::Varchar),
                Column::new("value".into(), DataType::Float32),
            ],
            Some(vec![
                PartitionColumn {
                    column: "region".into(),
                    transform: PartitionTransform::Identity,
                },
                PartitionColumn {
                    column: "value".into(),
                    transform: PartitionTransform::Bucket(8),
                },
            ]),
            None,
            None,
            IfExistsStrategy::Fail,
        )
        .await
        .unwrap();
    write_file(&table, "first.parquet", 2).await;
    write_file(&table, "second.parquet", 3).await;
    table
}

async fn write_file(table: &Table, path: &str, bucket: i64) {
    table
        .write_data_files(vec![WriteDataFile {
            path: path.into(),
            statistics: Some(DataFileStatistics {
                num_rows: 10,
                file_size_bytes: None,
                footer_size_bytes: None,
                column_stats: HashMap::new(),
            }),
            partition_values: Some(IndexMap::from([
                ("region".into(), Some(Value::Varchar("east".into()))),
                ("value".into(), Some(Value::Int64(bucket))),
            ])),
        }])
        .await
        .unwrap();
}

async fn fresh_scan(table: &Table) -> crate::ScanResult {
    let cache = SnapshotCache::new(table.conn.pool().clone(), None)
        .await
        .unwrap();
    scan_table(
        table.conn.pool(),
        table.id,
        cache.get_current(),
        &cache,
        &io::DucklakePath::Relative("data/".into()),
    )
    .await
    .unwrap()
}

#[rstest]
#[tokio::test]
async fn scan_exposes_buckets_and_native_filters_prune_files(#[future] bucket_table: Table) {
    // Arrange
    let table = bucket_table.await;
    let filters = [BucketFilter {
        field_id: 2,
        num_buckets: 8,
        values: vec![2],
    }];

    // Act
    let scan = table.scan().await.unwrap();
    let pruned = table.scan_with_bucket_filters(&filters).await.unwrap();

    // Assert
    assert_eq!(scan.data_files.len(), 2);
    for file in scan.data_files {
        let bucket = if file.path.ends_with("first.parquet") {
            2
        } else {
            3
        };
        assert_eq!(file.bucket_values, HashMap::from([(2, (8, bucket))]));
    }
    assert_eq!(pruned.data_files.len(), 1);
    assert!(pruned.data_files[0].path.ends_with("first.parquet"));
}

#[rstest]
#[tokio::test]
async fn column_type_evolution_omits_old_file_buckets(#[future] bucket_table: Table) {
    // Arrange
    let table = bucket_table.await;
    let before = table.scan().await.unwrap();
    let old_snapshot = table.conn.snapshot_cache().get_current();
    let old_catalog = old_snapshot.catalog().await.unwrap();

    // Act
    table
        .update_column_dtype("value", DataType::Float64)
        .await
        .unwrap();
    write_file(&table, "new.parquet", 2).await;
    let scan = table
        .scan_with_bucket_filters(&[BucketFilter {
            field_id: 2,
            num_buckets: 8,
            values: vec![3],
        }])
        .await
        .unwrap();

    // Assert
    assert!(
        before
            .data_files
            .iter()
            .all(|file| file.bucket_values.contains_key(&2))
    );
    assert_eq!(
        old_catalog.table(table.id).unwrap().column_data_types()[&2],
        DataType::Float32
    );
    assert_eq!(
        table
            .columns()
            .await
            .unwrap()
            .find(|column| column.name == "value")
            .unwrap()
            .dtype,
        DataType::Float64
    );
    assert_eq!(scan.data_files.len(), 2);
    assert!(
        scan.data_files
            .iter()
            .all(|file| file.bucket_values.is_empty())
    );
    assert!(
        scan.data_files
            .iter()
            .all(|file| !file.path.ends_with("new.parquet"))
    );
}

#[rstest]
#[case(None)]
#[case(Some(PartitionTransform::Identity))]
#[tokio::test]
async fn historical_nonbucket_files_are_kept_after_adding_buckets(
    #[future] bucket_table: Table,
    #[case] transform: Option<PartitionTransform>,
) {
    // Arrange
    let table = bucket_table.await;
    table
        .update_partitioning(transform.map(|transform| {
            vec![PartitionColumn {
                column: "value".into(),
                transform,
            }]
        }))
        .await
        .unwrap();
    write_file(&table, "nonbucket.parquet", 2).await;

    // Act
    table
        .update_partitioning(Some(vec![PartitionColumn {
            column: "value".into(),
            transform: PartitionTransform::Bucket(8),
        }]))
        .await
        .unwrap();
    write_file(&table, "new.parquet", 2).await;
    let scan = table
        .scan_with_bucket_filters(&[BucketFilter {
            field_id: 2,
            num_buckets: 8,
            values: vec![],
        }])
        .await
        .unwrap();

    // Assert
    assert_eq!(scan.data_files.len(), 3);
    assert!(
        scan.data_files
            .iter()
            .all(|file| file.bucket_values.is_empty())
    );
    assert!(
        scan.data_files
            .iter()
            .any(|file| file.path.ends_with("nonbucket.parquet"))
    );
    assert!(
        scan.data_files
            .iter()
            .all(|file| !file.path.ends_with("new.parquet"))
    );
}

#[rstest]
#[tokio::test]
async fn partition_evolution_omits_old_file_buckets(#[future] bucket_table: Table) {
    // Arrange
    let table = bucket_table.await;
    table
        .update_partitioning(Some(vec![
            PartitionColumn {
                column: "region".into(),
                transform: PartitionTransform::Identity,
            },
            PartitionColumn {
                column: "value".into(),
                transform: PartitionTransform::Bucket(16),
            },
        ]))
        .await
        .unwrap();
    write_file(&table, "new.parquet", 9).await;

    // Act
    let scan = table.scan().await.unwrap();

    // Assert
    for file in scan.data_files {
        if file.path.ends_with("new.parquet") {
            assert_eq!(file.bucket_values, HashMap::from([(2, (16, 9))]));
        } else {
            assert!(file.bucket_values.is_empty());
        }
    }
}

#[rstest]
#[case(None)]
#[case(Some(PartitionTransform::Identity))]
#[case(Some(PartitionTransform::Bucket(0)))]
#[tokio::test]
async fn nonbucket_scans_do_not_fetch_partition_values(
    #[future] bucket_table: Table,
    #[case] transform: Option<PartitionTransform>,
) {
    // Arrange
    let table = bucket_table.await;
    table
        .update_partitioning(transform.map(|transform| {
            vec![PartitionColumn {
                column: "value".into(),
                transform,
            }]
        }))
        .await
        .unwrap();
    let mut tx = table.conn.pool().begin().await.unwrap();
    tx.execute(
        &sea_query::Table::drop()
            .table(ducklake_file_partition_value::Table)
            .to_owned(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Act
    let scan = table.scan().await.unwrap();

    // Assert
    assert_eq!(scan.data_files.len(), 2);
    assert!(
        scan.data_files
            .iter()
            .all(|file| file.bucket_values.is_empty())
    );
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn ambiguous_partition_definitions_are_not_exposed(
    #[future] bucket_table: Table,
    #[case] duplicate_field: bool,
) {
    // Arrange
    let table = bucket_table.await;
    let mut tx = table.conn.pool().begin().await.unwrap();
    let mut columns: Vec<DucklakePartitionColumn> = tx
        .fetch_all(
            &Query::select()
                .column(sea_query::Asterisk)
                .from(ducklake_partition_column::Table)
                .and_where(
                    ducklake_partition_column::Column::TableId
                        .col()
                        .eq(table.id),
                )
                .and_where(
                    ducklake_partition_column::Column::PartitionKeyIndex
                        .col()
                        .eq(1),
                )
                .to_owned(),
        )
        .await
        .unwrap();
    let mut duplicate = columns.pop().unwrap();
    if duplicate_field {
        duplicate.partition_key_index = 9;
    } else {
        duplicate.column_id = 1;
        duplicate.transform = "identity".into();
    }
    tx.insert_entity(duplicate).await.unwrap();
    tx.commit().await.unwrap();

    // Act
    let scan = fresh_scan(&table).await;

    // Assert
    assert_eq!(scan.data_files.len(), 2);
    assert!(
        scan.data_files
            .iter()
            .all(|file| file.bucket_values.is_empty())
    );
}

#[rstest]
#[tokio::test]
async fn transfer_keeps_raw_partition_values_after_type_evolution(#[future] bucket_table: Table) {
    // Arrange
    let table = bucket_table.await;
    table
        .update_column_dtype("value", DataType::Float64)
        .await
        .unwrap();
    let cache = table.conn.snapshot_cache();
    let snapshot = cache.get_current();

    // Act
    let transfer = scan_table_for_transfer(
        table.conn.pool(),
        table.id,
        snapshot,
        cache,
        &io::DucklakePath::Relative("data/".into()),
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(transfer.partition_values.len(), 2);
    for (file, values) in transfer
        .result
        .data_files
        .iter()
        .zip(transfer.partition_values)
    {
        let bucket = if file.path.ends_with("first.parquet") {
            "2"
        } else {
            "3"
        };
        assert_eq!(values, Some(vec![Some("east".into()), Some(bucket.into())]));
    }
}

#[rstest]
#[tokio::test]
async fn transferred_files_with_unknown_original_types_are_not_pruned(
    #[future] bucket_table: Table,
) {
    // Arrange
    let table = bucket_table.await;
    table
        .update_column_dtype("value", DataType::Float64)
        .await
        .unwrap();
    let source = Ducklake {
        conn: table.conn.clone(),
    };
    let target = Ducklake::create(CreateOptions::new(
        "sqlite://:memory:",
        std::env::current_dir().unwrap().to_str().unwrap(),
    ))
    .await
    .unwrap();

    // Act
    let transferred = source
        .move_tables([&table], &target)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let scan = transferred
        .scan_with_bucket_filters(&[BucketFilter {
            field_id: 2,
            num_buckets: 8,
            values: vec![],
        }])
        .await
        .unwrap();
    write_file(&transferred, "new.parquet", 2).await;
    let latest_scan = transferred.scan().await.unwrap();

    // Assert
    assert_eq!(scan.data_files.len(), 2);
    assert!(
        scan.data_files
            .iter()
            .all(|file| file.bucket_values.is_empty())
    );
    let new_file = latest_scan
        .data_files
        .iter()
        .find(|file| file.path.ends_with("new.parquet"))
        .unwrap();
    assert_eq!(new_file.bucket_values, HashMap::from([(2, (8, 2))]));
}
