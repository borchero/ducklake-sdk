use chrono::{DateTime, Utc};
use sqlx::error::BoxDynError;

use crate::spec::literals;

/// A wrapper around DataTime<Utc> to properly parse DuckLake-generated datetime strings.
///
/// This is necessary because DuckDB writes a datetime format to SQLite which is not directly
/// supported by sqlx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct UtcDateTime(pub DateTime<Utc>);

impl UtcDateTime {
    pub(crate) fn now() -> Self {
        UtcDateTime(Utc::now())
    }
}

impl sea_query::Nullable for UtcDateTime {
    fn null() -> sea_query::Value {
        <DateTime<Utc> as sea_query::Nullable>::null()
    }
}

impl From<UtcDateTime> for sea_query::Value {
    fn from(value: UtcDateTime) -> Self {
        sea_query::Value::from(value.0)
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use sqlx::postgres::PgTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::Postgres> for UtcDateTime {
        fn type_info() -> PgTypeInfo {
            <DateTime<Utc> as sqlx::Type<sqlx::Postgres>>::type_info()
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::Postgres> for UtcDateTime {
        fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
            let datetime = <DateTime<Utc> as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
            Ok(UtcDateTime(datetime))
        }
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use sqlx::mysql::MySqlTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::MySql> for UtcDateTime {
        fn type_info() -> MySqlTypeInfo {
            <DateTime<Utc> as sqlx::Type<sqlx::MySql>>::type_info()
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::MySql> for UtcDateTime {
        fn decode(value: sqlx::mysql::MySqlValueRef<'r>) -> Result<Self, BoxDynError> {
            let datetime = <DateTime<Utc> as sqlx::Decode<sqlx::MySql>>::decode(value)?;
            Ok(UtcDateTime(datetime))
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use sqlx::sqlite::SqliteTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::Sqlite> for UtcDateTime {
        fn type_info() -> SqliteTypeInfo {
            <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for UtcDateTime {
        fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
            let s = <&str as sqlx::Decode<sqlx::Sqlite>>::decode(value)?;
            match literals::parse::<DateTime<Utc>>(s) {
                Ok(Some(dt)) => Ok(UtcDateTime(dt)),
                Ok(None) => Err("datetime parsed as None".into()),
                Err(e) => Err(e.into()),
            }
        }
    }
}
