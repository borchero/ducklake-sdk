use sea_query::{
    ColumnDef,
    CommonTableExpression,
    Expr,
    ExprTrait,
    Func,
    Order,
    Query,
    WindowStatement,
};

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
        "ducklake_schema",
        ColumnDef::new_with_type("path", dialect.column_type_string())
            .default(dialect.default_value_string("")),
        ColumnDef::new_with_type("path_is_relative", dialect.column_type_bool()).default(true),
    );
    add_table_columns!(
        tx,
        "ducklake_table",
        ColumnDef::new_with_type("path", dialect.column_type_string())
            .default(dialect.default_value_string("")),
        ColumnDef::new_with_type("path_is_relative", dialect.column_type_bool()).default(true),
    );
    add_table_columns!(
        tx,
        "ducklake_metadata",
        ColumnDef::new_with_type("scope", dialect.column_type_string()),
        ColumnDef::new_with_type("scope_id", dialect.column_type_i64()),
    );
    add_table_columns!(
        tx,
        "ducklake_data_file",
        ColumnDef::new_with_type("mapping_id", dialect.column_type_i64()),
    );
    create_table!(
        tx,
        "ducklake_column_mapping",
        ColumnDef::new_with_type("mapping_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("table_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("type", dialect.column_type_string()),
    );
    create_table!(
        tx,
        "ducklake_name_mapping",
        ColumnDef::new_with_type("mapping_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("column_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("source_name", dialect.column_type_string()),
        ColumnDef::new_with_type("target_field_id", dialect.column_type_i64()),
        ColumnDef::new_with_type("parent_column", dialect.column_type_i64()),
    );
    Ok(())
}

async fn migrate_data(tx: &mut db::Transaction) -> DucklakeResult<()> {
    let cte = Query::select()
        .column("table_id")
        .column("column_id")
        .expr_window_as(
            Func::cust("ROW_NUMBER"),
            WindowStatement::partition_by("table_id")
                .order_by("column_order", Order::Asc)
                .to_owned(),
            "column_rank",
        )
        .from("ducklake_column")
        .and_where(Expr::col("parent_column").is_null())
        .and_where(Expr::col("end_snapshot").is_null())
        .to_owned();
    let cte = CommonTableExpression::new()
        .columns(["table_id", "column_id", "column_rank"])
        .query(cte)
        .table_name("column_ranks")
        .to_owned();
    execute_migration!(
        tx,
        Query::update()
            .table("ducklake_partition_column")
            .value("column_id", Expr::col(("column_ranks", "column_id")))
            .from("column_ranks")
            .and_where(
                Expr::col(("ducklake_partition_column", "table_id"))
                    .eq(Expr::col(("column_ranks", "table_id"))),
            )
            .and_where(
                Expr::col(("ducklake_partition_column", "column_id"))
                    .add(1)
                    .eq(Expr::col(("column_ranks", "column_rank"))),
            )
            .with_cte(cte)
    );
    Ok(())
}
