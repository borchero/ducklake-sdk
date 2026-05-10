use sea_query::ExprTrait;

pub trait SnapshotFilter {
    fn filter_for_snapshot(
        &mut self,
        begin_snapshot: sea_query::Expr,
        end_snapshot: sea_query::Expr,
        snapshot_id: i64,
    ) -> &mut Self;
}

impl SnapshotFilter for sea_query::SelectStatement {
    fn filter_for_snapshot(
        &mut self,
        begin_snapshot: sea_query::Expr,
        end_snapshot: sea_query::Expr,
        snapshot_id: i64,
    ) -> &mut Self {
        self.and_where(begin_snapshot.lte(snapshot_id)).and_where(
            end_snapshot
                .clone()
                .gt(snapshot_id)
                .or(end_snapshot.is_null()),
        )
    }
}
