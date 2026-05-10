use sea_query::{Alias, ColumnDef, Expr, ExprTrait, JoinType, Query, Table};

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
        "ducklake_column",
        ColumnDef::new_with_type("default_value_type", dialect.column_type_string())
            .default(dialect.default_value_string("literal")),
    );
    add_table_columns!(
        tx,
        "ducklake_column",
        ColumnDef::new_with_type("default_value_dialect", dialect.column_type_string()),
    );
    add_table_columns!(
        tx,
        "ducklake_data_file",
        ColumnDef::new_with_type("partial_max", dialect.column_type_i64()),
    );
    add_table_columns!(
        tx,
        "ducklake_delete_file",
        ColumnDef::new_with_type("partial_max", dialect.column_type_i64()),
    );
    add_table_columns!(
        tx,
        "ducklake_schema_versions",
        ColumnDef::new_with_type("table_id", dialect.column_type_i64()),
    );
    create_table!(
        tx,
        "ducklake_macro",
        ColumnDef::new_with_type("schema_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("macro_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("macro_name", dialect.column_type_string()),
        ColumnDef::new_with_type("begin_snapshot", dialect.column_type_i64()),
        ColumnDef::new_with_type("end_snapshot", dialect.column_type_i64()),
    );
    create_table!(
        tx,
        "ducklake_macro_impl",
        ColumnDef::new_with_type("macro_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("impl_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("dialect", dialect.column_type_string()),
        ColumnDef::new_with_type("sql", dialect.column_type_string()),
        ColumnDef::new_with_type("type", dialect.column_type_string()),
    );
    create_table!(
        tx,
        "ducklake_macro_parameters",
        ColumnDef::new_with_type("macro_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("impl_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("column_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("parameter_name", dialect.column_type_string()),
        ColumnDef::new_with_type("parameter_type", dialect.column_type_string()),
        ColumnDef::new_with_type("default_value", dialect.column_type_string()),
        ColumnDef::new_with_type("default_value_type", dialect.column_type_string()),
    );
    create_table!(
        tx,
        "ducklake_sort_info",
        ColumnDef::new_with_type("sort_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("table_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("begin_snapshot", dialect.column_type_i64()),
        ColumnDef::new_with_type("end_snapshot", dialect.column_type_i64()),
    );
    create_table!(
        tx,
        "ducklake_sort_expression",
        ColumnDef::new_with_type("sort_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("table_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("sort_key_index", dialect.column_type_i64()),
        ColumnDef::new_with_type("expression", dialect.column_type_string()),
        ColumnDef::new_with_type("dialect", dialect.column_type_string()),
        ColumnDef::new_with_type("sort_direction", dialect.column_type_string()),
        ColumnDef::new_with_type("null_order", dialect.column_type_string()),
    );
    create_table!(
        tx,
        "ducklake_file_variant_stats",
        ColumnDef::new_with_type("data_file_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("table_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("column_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("variant_path", dialect.column_type_string()),
        ColumnDef::new_with_type("shredded_type", dialect.column_type_string()),
        ColumnDef::new_with_type("column_size_bytes", dialect.column_type_i64()),
        ColumnDef::new_with_type("value_count", dialect.column_type_i64()),
        ColumnDef::new_with_type("null_count", dialect.column_type_i64()),
        ColumnDef::new_with_type("min_value", dialect.column_type_string()),
        ColumnDef::new_with_type("max_value", dialect.column_type_string()),
        ColumnDef::new_with_type("contains_nan", dialect.column_type_bool()),
        ColumnDef::new_with_type("extra_stats", dialect.column_type_string()),
    );
    Ok(())
}

async fn migrate_data(tx: &mut db::Transaction) -> DucklakeResult<()> {
    let dialect = tx.dialect();
    let int_cast = match dialect {
        #[cfg(feature = "mysql")]
        db::Dialect::MySql => "SIGNED",
        _ => "BIGINT",
    };

    // Migrate partial_file_info data to partial_max before dropping the column
    execute_migration!(
        tx,
        Query::update()
            .table("ducklake_data_file")
            .value(
                "partial_max",
                Expr::cust("REPLACE(partial_file_info, 'partial_max:', '')")
                    .cast_as(Alias::new(int_cast)),
            )
            .and_where(Expr::col("partial_file_info").is_not_null())
            .and_where(Expr::col("partial_file_info").like("%partial_max:%"))
    );
    execute_migration!(
        tx,
        Table::alter()
            .table("ducklake_data_file")
            .drop_column("partial_file_info")
    );
    execute_migration!(
        tx,
        Query::insert()
            .into_table("ducklake_schema_versions")
            .columns(["table_id", "begin_snapshot", "schema_version"])
            .select_from(
                Query::select()
                    .expr(Expr::col(("t", "table_id")))
                    .expr(Expr::col(("t", "begin_snapshot")))
                    .expr(Expr::col(("sv", "schema_version")))
                    .from_as("ducklake_schema_versions", "sv")
                    .join_as(
                        JoinType::InnerJoin,
                        "ducklake_table",
                        "t",
                        Expr::col(("sv", "begin_snapshot")).between(
                            Expr::col(("t", "begin_snapshot")),
                            Expr::col(("t", "begin_snapshot")),
                        ),
                    )
                    .and_where(Expr::col(("sv", "table_id")).is_null())
                    .to_owned(),
            )
            .unwrap()
    );
    execute_migration!(
        tx,
        Query::delete()
            .from_table("ducklake_schema_versions")
            .and_where(Expr::col("table_id").is_null())
    );
    Ok(())
}
