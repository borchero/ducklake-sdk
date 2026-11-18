use std::time::Instant;

use crate::{DatabaseOptions, DucklakeError, DucklakeResult};

pub(super) fn validate_pg_schema(schema: &str) -> DucklakeResult<&str> {
    let mut chars = schema.chars();
    let valid = !schema.is_empty()
        && schema.len() <= 63
        && matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric());
    if !valid {
        return Err(DucklakeError::InvalidDatabaseOptions(format!(
            "invalid PostgreSQL schema name `{schema}`"
        )));
    }
    Ok(schema)
}

impl DatabaseOptions {
    pub(super) fn pool_options<DB: sqlx::Database>(
        &self,
        default_max_connections: u32,
    ) -> DucklakeResult<sqlx::pool::PoolOptions<DB>> {
        if let Some(schema) = &self.schema {
            validate_pg_schema(schema)?;
        }
        let max_connections = self.max_connections.unwrap_or(default_max_connections);
        if max_connections == 0 {
            return Err(DucklakeError::InvalidDatabaseOptions(
                "max_connections must be greater than zero".to_string(),
            ));
        }
        if self.min_connections > max_connections {
            return Err(DucklakeError::InvalidDatabaseOptions(
                "min_connections must not exceed max_connections".to_string(),
            ));
        }
        for (name, duration) in [
            ("acquire_timeout", Some(self.acquire_timeout)),
            ("idle_timeout", self.idle_timeout),
            ("max_lifetime", self.max_lifetime),
        ] {
            if let Some(duration) = duration
                && (duration.is_zero() || Instant::now().checked_add(duration).is_none())
            {
                return Err(DucklakeError::InvalidDatabaseOptions(format!(
                    "{name} must be positive"
                )));
            }
        }
        Ok(sqlx::pool::PoolOptions::new()
            .max_connections(max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout)
            .test_before_acquire(self.test_before_acquire)
            .idle_timeout(self.idle_timeout)
            .max_lifetime(self.max_lifetime))
    }
}

/* --------------------------------------------------------------------------------------------- */
/*                                             TESTS                                             */
/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("analytics")]
    #[case("_internal")]
    #[case("Tenant_01")]
    #[case("a")]
    fn test_validate_pg_schema_accepts_valid_identifiers(#[case] schema: &str) {
        // Act
        let result = validate_pg_schema(schema);

        // Assert
        assert_eq!(result.unwrap(), schema);
    }

    #[rstest]
    #[case("")]
    #[case("1leading_digit")]
    #[case("has-dash")]
    #[case("has space")]
    #[case("has\"quote")]
    #[case("bad;stmt")]
    #[case("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    fn test_validate_pg_schema_rejects_invalid_identifiers(#[case] schema: &str) {
        // Act
        let result = validate_pg_schema(schema);

        // Assert
        assert!(matches!(
            result,
            Err(DucklakeError::InvalidDatabaseOptions(_))
        ));
    }
}
