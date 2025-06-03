use flowgen_core::cache::CacheOptions;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A configuration option for the File Reader.
///
/// Example:
/// ```json
/// {
///     "file_reader": {
///         "path": "some_path",
///         "batch_size": "500",
///         "has_header": true,
///     }
/// }
/// ```

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    pub label: Option<String>,
    pub path: String,
    pub batch_size: Option<usize>,
    pub has_header: Option<bool>,
    pub cache_options: Option<CacheOptions>,
}

/// A configuration option for the File Writer.
///
/// Example:
/// ```json
/// {
///     "file_writer": {
///         "path": "some_path"
///     }
/// }
/// ```
///
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Writer {
    pub path: PathBuf,
}
