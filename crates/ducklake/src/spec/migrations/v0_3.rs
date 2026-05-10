use sea_query::{ColumnDef, Expr, ExprTrait, Query, Table};

use crate::{DucklakeResult, db};

pub async fn migrate(tx: &mut db::Transaction) -> DucklakeResult<()> {
    update_schema(tx).await?;
    migrate_data(tx).await?;
    Ok(())
}

async fn update_schema(tx: &mut db::Transaction) -> DucklakeResult<()> {
    let dialect = tx.dialect();

    add_table_columns!(
        tx,
        "ducklake_name_mapping",
        ColumnDef::new_with_type("is_partition", dialect.column_type_bool()).default(false),
    );
    add_table_columns!(
        tx,
        "ducklake_snapshot_changes",
        ColumnDef::new_with_type("author", dialect.column_type_string()),
        ColumnDef::new_with_type("commit_message", dialect.column_type_string()),
        ColumnDef::new_with_type("commit_extra_info", dialect.column_type_string()),
    );
    create_table!(
        tx,
        "ducklake_schema_versions",
        ColumnDef::new_with_type("begin_snapshot", dialect.column_type_i64()),
        ColumnDef::new_with_type("schema_version", dialect.column_type_i64()),
    );
    execute_migration!(
        tx,
        Table::rename().table(
            "ducklake_file_column_statistics",
            "ducklake_file_column_stats"
        )
    );
    add_table_columns!(
        tx,
        "ducklake_file_column_stats",
        ColumnDef::new_with_type("extra_stats", dialect.column_type_string()),
    );
    add_table_columns!(
        tx,
        "ducklake_table_column_stats",
        ColumnDef::new_with_type("extra_stats", dialect.column_type_string()),
    );
    Ok(())
}

async fn migrate_data(tx: &mut db::Transaction) -> DucklakeResult<()> {
    execute_migration!(
        tx,
        Query::insert()
            .into_table("ducklake_schema_versions")
            .columns(["begin_snapshot", "schema_version"])
            .select_from(
                Query::select()
                    .exprs([Expr::col("snapshot_id").min(), Expr::col("schema_version")])
                    .from("ducklake_snapshot")
                    .group_by_col("schema_version")
                    .to_owned(),
            )
            .unwrap()
    );
    Ok(())
}
