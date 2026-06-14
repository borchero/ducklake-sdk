/* --------------------------------------------------------------------------------------------- */
/*                                          QUERY VALUES                                         */
/* --------------------------------------------------------------------------------------------- */

pub(super) fn adapt_values(values: sea_query_sqlx::SqlxValues) -> sea_query_sqlx::SqlxValues {
    use sea_query::Value;
    let values = values
        .0
        .into_iter()
        .map(|v| match v {
            Value::Uuid(Some(uuid)) => Value::String(Some(uuid.to_string())),
            Value::Uuid(None) => Value::String(None),
            other => other,
        })
        .collect();
    sea_query_sqlx::SqlxValues(sea_query::Values(values))
}
