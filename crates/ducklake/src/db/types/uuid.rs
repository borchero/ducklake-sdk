use std::str::FromStr;

use sqlx::error::BoxDynError;
use uuid::Uuid;

/// A wrapper around [`Uuid`] used for DuckLake catalog entities.
///
/// This is necessary because Rust's `sqlx` expects UUID to a BYTE field while the DuckDB SQLite
/// extension expects UUIDs to be stored as TEXT. First and foremost, we want to be compatible with
/// the DuckLake DuckDB extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct UuidText(pub Uuid);

impl UuidText {
    pub fn now_v7() -> Self {
        UuidText(Uuid::now_v7())
    }
}

impl sea_query::Nullable for UuidText {
    fn null() -> sea_query::Value {
        <Uuid as sea_query::Nullable>::null()
    }
}

impl From<UuidText> for sea_query::Value {
    fn from(value: UuidText) -> Self {
        sea_query::Value::from(value.0)
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use sqlx::postgres::PgTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::Postgres> for UuidText {
        fn type_info() -> PgTypeInfo {
            <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::Postgres> for UuidText {
        fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
            let uuid = <Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
            Ok(UuidText(uuid))
        }
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use sqlx::mysql::MySqlTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::MySql> for UuidText {
        fn type_info() -> MySqlTypeInfo {
            <String as sqlx::Type<sqlx::MySql>>::type_info()
        }

        fn compatible(ty: &MySqlTypeInfo) -> bool {
            <String as sqlx::Type<sqlx::MySql>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::MySql> for UuidText {
        fn decode(value: sqlx::mysql::MySqlValueRef<'r>) -> Result<Self, BoxDynError> {
            let s = <String as sqlx::Decode<sqlx::MySql>>::decode(value)?;
            let uuid = Uuid::from_str(&s)?;
            Ok(UuidText(uuid))
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use sqlx::sqlite::SqliteTypeInfo;

    use super::*;

    impl sqlx::Type<sqlx::Sqlite> for UuidText {
        fn type_info() -> SqliteTypeInfo {
            <String as sqlx::Type<sqlx::Sqlite>>::type_info()
        }

        fn compatible(ty: &SqliteTypeInfo) -> bool {
            <String as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for UuidText {
        fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
            let s = <String as sqlx::Decode<sqlx::Sqlite>>::decode(value)?;
            let uuid = Uuid::from_str(&s)?;
            Ok(UuidText(uuid))
        }
    }
}
