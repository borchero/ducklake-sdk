use std::fmt::Write;

use sea_query::Value;
use sqlx::PgConnection;

use crate::DucklakeResult;

/// COPY supports the scalar types used by buffered catalog entities. Keep other values on the regular
/// INSERT path so adding a new entity type cannot silently change its encoding.
pub(super) fn supports(rows: &[Vec<Value>]) -> bool {
    rows.iter().flatten().all(|value| {
        matches!(
            value,
            Value::Bool(_) | Value::BigInt(_) | Value::String(_) | Value::Uuid(_)
        )
    })
}

pub(super) async fn insert(
    connection: &mut PgConnection,
    table: &str,
    columns: &[&str],
    rows: Vec<Vec<Value>>,
) -> DucklakeResult<()> {
    let columns = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>();
    let statement = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text, ENCODING 'UTF8')",
        quote_identifier(table),
        columns.join(", ")
    );
    super::log_sql(&statement, None);
    let mut copy = connection.copy_in_raw(&statement).await?;
    // Stream bounded chunks instead of encoding the entire batch a second time in memory.
    let mut buffer = String::with_capacity(64 * 1024);
    for row in rows {
        encode_row(&mut buffer, row);
        if buffer.len() >= 64 * 1024 {
            if let Err(error) = copy.send(buffer.as_bytes()).await {
                let _ = copy.abort("catalog COPY send failed").await;
                return Err(error.into());
            }
            buffer.clear();
        }
    }
    if !buffer.is_empty()
        && let Err(error) = copy.send(buffer.as_bytes()).await
    {
        let _ = copy.abort("catalog COPY send failed").await;
        return Err(error.into());
    }
    copy.finish().await?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn encode_row(buffer: &mut String, row: Vec<Value>) {
    for (index, value) in row.into_iter().enumerate() {
        if index > 0 {
            buffer.push('\t');
        }
        match value {
            Value::Bool(Some(value)) => buffer.push_str(if value { "t" } else { "f" }),
            Value::BigInt(Some(value)) => write!(buffer, "{value}").unwrap(),
            Value::Uuid(Some(value)) => write!(buffer, "{value}").unwrap(),
            Value::String(Some(value)) => {
                // PostgreSQL COPY text escaping, including a literal backslash-N (not NULL).
                // https://www.postgresql.org/docs/current/sql-copy.html#SQL-COPY-NOTES
                for character in value.chars() {
                    match character {
                        '\\' => buffer.push_str("\\\\"),
                        '\t' => buffer.push_str("\\t"),
                        '\n' => buffer.push_str("\\n"),
                        '\r' => buffer.push_str("\\r"),
                        character => buffer.push(character),
                    }
                }
            }
            Value::Bool(None) | Value::BigInt(None) | Value::String(None) | Value::Uuid(None) => {
                buffer.push_str("\\N")
            }
            _ => unreachable!("COPY input types were checked before starting the stream"),
        }
    }
    buffer.push('\n');
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use rstest::rstest;
    use uuid::Uuid;

    use super::*;

    #[rstest]
    #[case(vec![Value::String(None), "".into(), "\\N".into()], "\\N\t\t\\\\N\n")]
    #[case(vec!["tab\tline\nreturn\rslash\\雪".into()], "tab\\tline\\nreturn\\rslash\\\\雪\n")]
    #[case(vec![true.into(), false.into(), i64::MIN.into()], "t\tf\t-9223372036854775808\n")]
    #[case(vec![Uuid::nil().into()], "00000000-0000-0000-0000-000000000000\n")]
    fn test_encode_row(#[case] row: Vec<Value>, #[case] expected: &str) {
        // Arrange
        let mut buffer = String::new();

        // Act
        encode_row(&mut buffer, row);

        // Assert
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_quote_identifier() {
        // Arrange
        let identifier = "a\"b";

        // Act
        let quoted = quote_identifier(identifier);

        // Assert
        assert_eq!(quoted, "\"a\"\"b\"");
    }

    #[rstest]
    #[case(Value::Bytes(Some(vec![0, 1])))]
    #[case(DateTime::parse_from_rfc3339("2026-09-14T12:34:56.123456789Z").unwrap().to_utc().into())]
    fn test_unsupported_values_use_insert(#[case] value: Value) {
        // Arrange
        let rows = vec![vec![1_i64.into()], vec![value]];

        // Act
        let supported = supports(&rows);

        // Assert
        assert!(!supported);
    }
}
