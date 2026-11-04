use std::time::Instant;

use crate::{DatabaseOptions, DucklakeError, DucklakeResult};

impl DatabaseOptions {
    pub(super) fn pool_options<DB: sqlx::Database>(
        &self,
        default_max_connections: u32,
    ) -> DucklakeResult<sqlx::pool::PoolOptions<DB>> {
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
