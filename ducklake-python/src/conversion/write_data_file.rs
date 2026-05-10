use std::collections::HashMap;

use ducklake::{DataFileStatistics, FileColumnStats, Value, WriteDataFile};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::Wrap;

impl FromPyObject<'_, '_> for Wrap<WriteDataFile> {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let path: String = ob.getattr("path")?.extract()?;

        let statistics_obj = ob.getattr("statistics")?;
        let statistics = if statistics_obj.is_none() {
            None
        } else {
            let num_rows: usize = statistics_obj.getattr("num_rows")?.extract()?;
            let file_size_bytes: Option<usize> =
                statistics_obj.getattr("file_size_bytes")?.extract()?;
            let footer_size_bytes: Option<usize> =
                statistics_obj.getattr("footer_size_bytes")?.extract()?;
            let column_stats_dict: Bound<'_, PyDict> = statistics_obj
                .getattr("column_stats")?
                .to_owned()
                .cast_into()?;
            let column_stats: HashMap<i64, FileColumnStats> = column_stats_dict
                .iter()
                .map(|(k, v)| {
                    let field_id = k.extract()?;
                    let stats: Wrap<FileColumnStats> = v.extract()?;
                    Ok((field_id, stats.0))
                })
                .collect::<PyResult<_>>()?;
            Some(DataFileStatistics {
                num_rows,
                file_size_bytes,
                footer_size_bytes,
                column_stats,
            })
        };

        let partition_values_dict: Option<Bound<'_, PyDict>> =
            ob.getattr("partition_values")?.to_owned().extract()?;
        let partition_values = partition_values_dict
            .map(|dict| {
                dict.iter()
                    .map(|(k, v)| {
                        let key: String = k.extract()?;
                        let value: Option<Wrap<Value>> = v.extract::<Option<Wrap<Value>>>()?;
                        Ok((key, value.map(|w| w.0)))
                    })
                    .collect::<PyResult<_>>()
            })
            .transpose()?;

        Ok(Wrap(WriteDataFile {
            path,
            statistics,
            partition_values,
        }))
    }
}
