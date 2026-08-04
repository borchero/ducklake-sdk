use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};

use object_store::ObjectStore;
#[cfg(feature = "aws")]
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "azure")]
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
#[cfg(feature = "gcp")]
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use url::Url;

use crate::{DucklakeError, DucklakeResult};

/* --------------------------------------- DUCKLAKE PATH --------------------------------------- */

/// Path stored in the DuckLake catalog. It can either be relative (to the catalog's data path) or
/// absolute.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DucklakePath {
    Absolute(Url),
    Relative(String),
}

impl DucklakePath {
    pub(crate) fn new(path: &str, is_relative: bool) -> Self {
        if is_relative {
            DucklakePath::Relative(path.to_string())
        } else {
            // An absolute local filesystem path must be converted to a `file://` URL directly.
            // This must happen before `Url::parse`, as on Windows the drive letter of a path such
            // as `C:\data` would otherwise be interpreted as a URL scheme (`c`).
            if std::path::Path::new(path).is_absolute() {
                return DucklakePath::Absolute(Url::from_file_path(path).unwrap());
            }
            let url = match Url::parse(path) {
                Ok(url) => url,
                Err(e) => panic!("Invalid URL: {}", e),
            };
            DucklakePath::Absolute(url)
        }
    }

    pub(crate) fn join(&self, other: &DucklakePath) -> Self {
        use DucklakePath::*;
        match (self, other) {
            (_, Absolute(other)) => Absolute(other.clone()),
            (Absolute(base), Relative(other)) => Absolute(base.join(other).unwrap()),
            (Relative(base), Relative(other)) => {
                assert!(base.ends_with("/"));
                Relative(format!("{}{}", base, other))
            }
        }
    }

    pub(crate) fn join_str(&self, other: &str) -> Self {
        self.join(&DucklakePath::Relative(other.to_string()))
    }

    pub(crate) fn is_relative(&self) -> bool {
        matches!(self, DucklakePath::Relative(_))
    }

    pub(crate) fn is_absolute(&self) -> bool {
        matches!(self, DucklakePath::Absolute(_))
    }

    pub(crate) fn ensure_directory(&self) -> Self {
        match self {
            DucklakePath::Absolute(url) => {
                let mut url = url.clone();
                if !url.path().ends_with('/') {
                    url.set_path(&format!("{}/", url.path()));
                }
                DucklakePath::Absolute(url)
            }
            DucklakePath::Relative(path) => {
                if !path.ends_with('/') {
                    DucklakePath::Relative(format!("{}/", path))
                } else {
                    self.clone()
                }
            }
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            DucklakePath::Absolute(url) => url.as_str(),
            DucklakePath::Relative(path) => path.as_str(),
        }
    }

    pub(crate) fn resolve(&self) -> DucklakeResult<Path> {
        let url = match self {
            DucklakePath::Absolute(url) => url.clone(),
            DucklakePath::Relative(path) => Url::from_file_path(format!(
                "{}/{}",
                std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy(),
                path
            ))
            .unwrap(),
        };
        Path::new(url)
    }
}

impl Default for DucklakePath {
    fn default() -> Self {
        DucklakePath::Relative("".to_string())
    }
}

impl FromStr for DucklakePath {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // An absolute local filesystem path must be converted to a `file://` URL directly. This
        // must happen before `Url::parse`, as on Windows the drive letter of a path such as
        // `C:\data` would otherwise be interpreted as a URL scheme (`c`).
        if std::path::Path::new(s).is_absolute() {
            return Ok(DucklakePath::Absolute(Url::from_file_path(s).unwrap()));
        }
        match Url::parse(s) {
            Ok(url) => Ok(DucklakePath::Absolute(url)),
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                Ok(DucklakePath::Relative(s.to_string()))
            }
            Err(e) => Err(e),
        }
    }
}

impl Display for DucklakePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DucklakePath::Absolute(url) => write!(f, "{}", url),
            DucklakePath::Relative(path) => write!(f, "{}", path),
        }
    }
}

/* ------------------------------------------ IO PATH ------------------------------------------ */

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub(crate) enum Path {
    Local {
        path: String,
    },
    #[cfg(feature = "aws")]
    S3 {
        bucket: String,
        path: String,
    },
    #[cfg(feature = "gcp")]
    GCS {
        bucket: String,
        path: String,
    },
    #[cfg(feature = "azure")]
    Azure {
        container: String,
        path: String,
    },
}

impl Path {
    fn new(url: Url) -> DucklakeResult<Self> {
        match url.scheme() {
            "file" => Ok(Path::Local {
                path: url.to_file_path().unwrap().to_string_lossy().to_string(),
            }),
            #[cfg(feature = "aws")]
            "s3" => Ok(Path::S3 {
                bucket: url.host_str().unwrap().to_string(),
                path: url.path().to_string(),
            }),
            #[cfg(feature = "gcp")]
            "gs" => Ok(Path::GCS {
                bucket: url.host_str().unwrap().to_string(),
                path: url.path().to_string(),
            }),
            #[cfg(feature = "azure")]
            "az" | "abfs" => Ok(Path::Azure {
                container: url.host_str().unwrap().to_string(),
                path: url.path().to_string(),
            }),
            _ => Err(DucklakeError::UnsupportedUrlScheme(
                url.scheme().to_string(),
            )),
        }
    }

    pub(crate) fn path(&self) -> ObjectStorePath {
        match self {
            // Local paths are OS-native filesystem paths (e.g. `C:\dir\file` on Windows), so they
            // must be converted via `from_absolute_path`, which normalizes the platform's path
            // separators.
            Path::Local { path } => ObjectStorePath::from_absolute_path(path).unwrap(),
            #[cfg(feature = "aws")]
            Path::S3 { path, .. } => ObjectStorePath::parse(path).unwrap(),
            #[cfg(feature = "azure")]
            Path::Azure { path, .. } => ObjectStorePath::parse(path).unwrap(),
            #[cfg(feature = "gcp")]
            Path::GCS { path, .. } => ObjectStorePath::parse(path).unwrap(),
        }
    }

    pub(crate) fn display_location(&self, location: &ObjectStorePath) -> String {
        match self {
            #[cfg(not(windows))]
            Path::Local { .. } => format!("/{}", location),
            #[cfg(windows)]
            Path::Local { .. } => location.as_ref().replace('/', "\\"),
            #[cfg(feature = "aws")]
            Path::S3 { bucket, .. } => format!("s3://{}/{}", bucket, location),
            #[cfg(feature = "azure")]
            Path::Azure { container, .. } => format!("az://{}/{}", container, location),
            #[cfg(feature = "gcp")]
            Path::GCS { bucket, .. } => format!("gs://{}/{}", bucket, location),
        }
    }

    pub(crate) fn object_store(
        &self,
        #[allow(unused_variables)] options: Option<Vec<(String, String)>>,
    ) -> Arc<dyn ObjectStore> {
        let cache_key = match self {
            Path::Local { path: _ } => ObjectStoreCacheKey::Local,
            #[cfg(feature = "aws")]
            Path::S3 { bucket, path: _ } => ObjectStoreCacheKey::S3 {
                bucket: bucket.clone(),
                options: parse_config_options(options),
            },
            #[cfg(feature = "gcp")]
            Path::GCS { bucket, path: _ } => ObjectStoreCacheKey::GCS {
                bucket: bucket.clone(),
                options: parse_config_options(options),
            },
            #[cfg(feature = "azure")]
            Path::Azure { container, path: _ } => ObjectStoreCacheKey::Azure {
                container: container.clone(),
                options: parse_config_options(options),
            },
        };
        get_cached_object_store(cache_key)
    }
}

/// Parses the provided key-value options into the object store's config keys, discarding any
/// options whose key is not a valid config key.
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
fn parse_config_options<K: FromStr>(options: Option<Vec<(String, String)>>) -> Vec<(K, String)> {
    let mut result = Vec::new();
    if let Some(options) = options {
        for (key, value) in options {
            if let Ok(config_key) = key.to_lowercase().parse() {
                result.push((config_key, value));
            }
        }
    }
    result
}

/* ------------------------------------------- CACHE ------------------------------------------- */

static OBJECT_STORE_CACHE: LazyLock<Mutex<HashMap<ObjectStoreCacheKey, Arc<dyn ObjectStore>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ObjectStoreCacheKey {
    Local,
    #[cfg(feature = "aws")]
    S3 {
        bucket: String,
        options: Vec<(AmazonS3ConfigKey, String)>,
    },
    #[cfg(feature = "gcp")]
    GCS {
        bucket: String,
        options: Vec<(GoogleConfigKey, String)>,
    },
    #[cfg(feature = "azure")]
    Azure {
        container: String,
        options: Vec<(AzureConfigKey, String)>,
    },
}

fn get_cached_object_store(key: ObjectStoreCacheKey) -> Arc<dyn ObjectStore> {
    let mut cache = OBJECT_STORE_CACHE.lock().unwrap();
    if let Some(store) = cache.get(&key) {
        store.clone()
    } else {
        let store: Arc<dyn ObjectStore> = match key {
            ObjectStoreCacheKey::Local => Arc::new(LocalFileSystem::new()),
            #[cfg(feature = "aws")]
            ObjectStoreCacheKey::S3 {
                ref bucket,
                ref options,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_allow_http(true);
                for (config_key, value) in options {
                    builder = builder.with_config(*config_key, value);
                }
                Arc::new(builder.build().unwrap())
            }
            #[cfg(feature = "gcp")]
            ObjectStoreCacheKey::GCS {
                ref bucket,
                ref options,
            } => {
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
                for (config_key, value) in options {
                    builder = builder.with_config(*config_key, value);
                }
                Arc::new(builder.build().unwrap())
            }
            #[cfg(feature = "azure")]
            ObjectStoreCacheKey::Azure {
                ref container,
                ref options,
            } => {
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_container_name(container)
                    .with_allow_http(true);
                for (config_key, value) in options {
                    builder = builder.with_config(*config_key, value);
                }
                Arc::new(builder.build().unwrap())
            }
        };
        cache.insert(key, store.clone());
        store
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

    fn absolute(url: &str) -> DucklakePath {
        DucklakePath::Absolute(Url::parse(url).unwrap())
    }

    fn relative(path: &str) -> DucklakePath {
        DucklakePath::Relative(path.to_string())
    }

    #[rstest]
    #[case("foo/", true, relative("foo/"))]
    #[case("foo/bar/", true, relative("foo/bar/"))]
    #[case("s3://bucket/prefix/", false, absolute("s3://bucket/prefix/"))]
    #[case("file:///data/", false, absolute("file:///data/"))]
    fn test_new(#[case] input: &str, #[case] is_relative: bool, #[case] expected: DucklakePath) {
        assert_eq!(DucklakePath::new(input, is_relative), expected);
    }

    #[rstest]
    #[case("foo/", relative("foo/"))]
    #[case("foo/bar/", relative("foo/bar/"))]
    #[case("s3://bucket/prefix/", absolute("s3://bucket/prefix/"))]
    #[case("file:///data/", absolute("file:///data/"))]
    fn test_from_str(#[case] input: &str, #[case] expected: DucklakePath) {
        assert_eq!(input.parse::<DucklakePath>().unwrap(), expected);
    }

    // Absolute local paths are platform-specific: on Unix they start with `/`, whereas on Windows
    // they are rooted at a drive letter. In particular, a Windows drive-lettered path must not be
    // misinterpreted as a URL whose scheme is the drive letter.
    #[cfg(unix)]
    #[test]
    fn test_from_str_absolute_local_path() {
        assert_eq!(
            "/absolute/path".parse::<DucklakePath>().unwrap(),
            absolute("file:///absolute/path")
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_from_str_absolute_local_path() {
        assert_eq!(
            "C:\\data\\path".parse::<DucklakePath>().unwrap(),
            absolute("file:///C:/data/path")
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_new_absolute_local_path() {
        assert_eq!(
            DucklakePath::new("C:\\data\\path", false),
            absolute("file:///C:/data/path")
        );
    }

    #[test]
    fn test_from_str_invalid_url() {
        assert!("http://[bad".parse::<DucklakePath>().is_err());
    }

    #[rstest]
    #[case(relative(""), true)]
    #[case(relative("foo/"), true)]
    #[case(absolute("s3://bucket/"), false)]
    #[case(absolute("file:///"), false)]
    fn test_is_relative(#[case] path: DucklakePath, #[case] expected: bool) {
        assert_eq!(path.is_relative(), expected);
    }

    #[rstest]
    #[case(relative("foo/"), relative("bar"), relative("foo/bar"))]
    #[case(relative("foo/"), relative("bar/baz"), relative("foo/bar/baz"))]
    #[case(
        absolute("s3://bucket/prefix/"),
        relative("file.parquet"),
        absolute("s3://bucket/prefix/file.parquet")
    )]
    #[case(
        absolute("file:///data/"),
        relative("x.parquet"),
        absolute("file:///data/x.parquet")
    )]
    // Joining with an absolute path returns the absolute path
    #[case(relative("foo/"), absolute("s3://bucket/"), absolute("s3://bucket/"))]
    #[case(absolute("s3://a/"), absolute("s3://b/"), absolute("s3://b/"))]
    fn test_join(
        #[case] base: DucklakePath,
        #[case] other: DucklakePath,
        #[case] expected: DucklakePath,
    ) {
        assert_eq!(base.join(&other), expected);
    }

    #[test]
    #[should_panic]
    fn test_join_relative_base_without_trailing_slash_panics() {
        let base = relative("foo");
        let _ = base.join(&relative("bar"));
    }

    #[rstest]
    #[case(relative("foo/"), "bar", relative("foo/bar"))]
    #[case(
        absolute("s3://bucket/"),
        "file.parquet",
        absolute("s3://bucket/file.parquet")
    )]
    fn test_join_str(
        #[case] base: DucklakePath,
        #[case] other: &str,
        #[case] expected: DucklakePath,
    ) {
        assert_eq!(base.join_str(other), expected);
    }

    #[rstest]
    #[case(relative("foo"), relative("foo/"))]
    #[case(relative("foo/"), relative("foo/"))]
    #[case(absolute("s3://bucket/prefix"), absolute("s3://bucket/prefix/"))]
    #[case(absolute("s3://bucket/prefix/"), absolute("s3://bucket/prefix/"))]
    #[case(absolute("file:///data"), absolute("file:///data/"))]
    fn test_ensure_directory(#[case] input: DucklakePath, #[case] expected: DucklakePath) {
        assert_eq!(input.ensure_directory(), expected);
    }

    #[rstest]
    #[case(relative("foo/"), "foo/")]
    #[case(relative(""), "")]
    #[case(absolute("s3://bucket/prefix/"), "s3://bucket/prefix/")]
    fn test_as_str(#[case] path: DucklakePath, #[case] expected: &str) {
        assert_eq!(path.as_str(), expected);
    }

    #[rstest]
    #[case(relative("foo/"), "foo/")]
    #[case(absolute("s3://bucket/prefix/"), "s3://bucket/prefix/")]
    fn test_display(#[case] path: DucklakePath, #[case] expected: &str) {
        assert_eq!(path.to_string(), expected);
    }

    #[test]
    fn test_default_is_empty_relative() {
        assert_eq!(DucklakePath::default(), relative(""));
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_resolve_s3() {
        let path = absolute("s3://bucket/prefix/file.parquet")
            .resolve()
            .unwrap();
        match path {
            Path::S3 { bucket, path } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(path, "/prefix/file.parquet");
            }
            _ => panic!("expected S3 path"),
        }
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn test_resolve_gcs() {
        let path = absolute("gs://bucket/prefix/file.parquet")
            .resolve()
            .unwrap();
        match path {
            Path::GCS { bucket, path } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(path, "/prefix/file.parquet");
            }
            _ => panic!("expected GCS path"),
        }
    }

    #[rstest]
    #[case("az://container/prefix/file.parquet")]
    #[case("abfs://container/prefix/file.parquet")]
    #[cfg(feature = "azure")]
    fn test_resolve_azure(#[case] input: &str) {
        let path = absolute(input).resolve().unwrap();
        match path {
            Path::Azure { container, path } => {
                assert_eq!(container, "container");
                assert_eq!(path, "/prefix/file.parquet");
            }
            _ => panic!("expected Azure path"),
        }
    }

    // Resolving a `file://` URL to a local path is platform-specific: Unix yields a `/`-rooted
    // path, whereas Windows yields a drive-lettered path with backslash separators.
    #[cfg(unix)]
    #[test]
    fn test_resolve_local() {
        let path = absolute("file:///tmp/file.parquet").resolve().unwrap();
        match path {
            Path::Local { path } => {
                assert_eq!(path, "/tmp/file.parquet");
            }
            _ => panic!("expected Local path"),
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_resolve_local() {
        let path = absolute("file:///C:/tmp/file.parquet").resolve().unwrap();
        match path {
            Path::Local { path } => {
                assert_eq!(path, "C:\\tmp\\file.parquet");
            }
            _ => panic!("expected Local path"),
        }
    }

    #[test]
    fn test_resolve_unsupported_scheme() {
        let path = absolute("http://example.com/file").resolve();
        assert!(matches!(path, Err(DucklakeError::UnsupportedUrlScheme(_))));
    }
}
