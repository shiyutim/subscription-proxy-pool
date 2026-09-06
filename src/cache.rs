use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{Error, ProxyNode, Result, subscription::SubscriptionValidators};

const SCHEMA_VERSION: u32 = 1;
const MAX_CACHE_BYTES: u64 = 8 * 1024 * 1024;

/// Optional persistent subscription cache.
///
/// Files contain proxy credentials, but never the subscription URL. Keep the
/// directory private and outside source control. Each write atomically replaces
/// the previous file; on Unix, new directories and files have owner-only
/// permissions (`0700` and `0600`). Existing directory permissions are preserved.
/// Missing, invalid, and expired files are cache misses; other filesystem errors
/// are returned to the caller instead of silently hiding configuration problems.
#[derive(Clone, Debug)]
pub struct CachePolicy {
    /// Directory in which hashed subscription keys identify cache files.
    pub directory: PathBuf,
    /// How long a successful download or HTTP revalidation remains fresh.
    pub ttl: Duration,
    /// Additional time after `ttl` during which stale fallback is permitted.
    /// Zero disables stale fallback.
    pub max_stale: Duration,
}

impl CachePolicy {
    /// Create a policy with a three-day lifetime and seven additional days of
    /// stale fallback. A failed download never renews either deadline.
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            ttl: Duration::from_secs(3 * 24 * 60 * 60),
            max_stale: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }

    /// Check that cache deadlines can be represented and the lifetime is nonzero.
    pub fn validate(&self) -> Result<()> {
        if self.ttl.is_zero() {
            return Err(Error::Config("cache TTL must be nonzero"));
        }
        if self.ttl.checked_add(self.max_stale).is_none() {
            return Err(Error::Config("cache lifetime is too large"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct CacheStore {
    policy: CachePolicy,
}

pub(crate) struct CachedNodes {
    pub nodes: Vec<ProxyNode>,
    pub fresh: bool,
    pub validators: SubscriptionValidators,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheEntry {
    schema: u32,
    source_key: String,
    saved_at: SystemTime,
    nodes: Vec<ProxyNode>,
    // Version-one cache files written before conditional requests remain usable.
    #[serde(default)]
    validators: SubscriptionValidators,
}

impl CacheStore {
    pub(crate) fn new(policy: CachePolicy) -> Self {
        Self { policy }
    }

    pub(crate) async fn load(&self, source_key: &str) -> Result<Option<CachedNodes>> {
        self.policy.validate()?;
        validate_source_key(source_key)?;
        let policy = self.policy.clone();
        let source_key = source_key.to_owned();
        tokio::task::spawn_blocking(move || load(&policy, &source_key, SystemTime::now()))
            .await
            .map_err(|_| Error::Cache("cache read task failed"))?
    }

    #[cfg(test)]
    pub(crate) async fn save(&self, source_key: &str, nodes: &[ProxyNode]) -> Result<()> {
        self.save_with_validators(source_key, nodes, &SubscriptionValidators::default())
            .await
    }

    /// Save a successfully downloaded or revalidated representation. Callers
    /// must not use this to renew stale data after an unsuccessful download.
    pub(crate) async fn save_with_validators(
        &self,
        source_key: &str,
        nodes: &[ProxyNode],
        validators: &SubscriptionValidators,
    ) -> Result<()> {
        self.policy.validate()?;
        validate_source_key(source_key)?;
        validate_nodes(nodes)?;
        let directory = self.policy.directory.clone();
        let entry = CacheEntry {
            schema: SCHEMA_VERSION,
            source_key: source_key.to_owned(),
            saved_at: SystemTime::now(),
            nodes: nodes.to_vec(),
            validators: validators.clone(),
        };
        tokio::task::spawn_blocking(move || save(&directory, &entry))
            .await
            .map_err(|_| Error::Cache("cache write task failed"))?
    }
}

fn validate_source_key(source_key: &str) -> Result<()> {
    if source_key.len() != 64
        || !source_key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Cache(
            "cache source key must be a lowercase SHA-256 digest",
        ));
    }
    Ok(())
}

fn validate_nodes(nodes: &[ProxyNode]) -> Result<()> {
    if nodes.is_empty() || nodes.iter().any(|node| node.validate().is_err()) {
        return Err(Error::Cache("cache must contain valid proxy nodes"));
    }
    Ok(())
}

fn cache_path(directory: &Path, source_key: &str) -> PathBuf {
    directory.join(format!("{source_key}.json"))
}

fn missing_cache(directory: &Path) -> Result<Option<CachedNodes>> {
    // Windows reports a missing path even when an ancestor is a regular file.
    // Distinguish an absent cache from an unusable configured directory.
    for ancestor in directory
        .ancestors()
        .filter(|path| !path.as_os_str().is_empty())
    {
        match fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(cache_io_error(std::io::ErrorKind::NotADirectory.into()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(cache_io_error(error)),
        }
    }
    Ok(None)
}

fn load(policy: &CachePolicy, source_key: &str, now: SystemTime) -> Result<Option<CachedNodes>> {
    let path = cache_path(&policy.directory, source_key);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return missing_cache(&policy.directory);
        }
        Err(error) => return Err(cache_io_error(error)),
    };
    // Cache entries are regular files. Do not follow links to unrelated files.
    if !metadata.is_file() {
        return Ok(None);
    }
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return missing_cache(&policy.directory);
        }
        Err(error) => return Err(cache_io_error(error)),
    };
    let metadata = file.metadata().map_err(cache_io_error)?;
    if !metadata.is_file() || metadata.len() > MAX_CACHE_BYTES {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.take(MAX_CACHE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(cache_io_error)?;
    if bytes.len() as u64 > MAX_CACHE_BYTES {
        return Ok(None);
    }
    let entry: CacheEntry = match serde_json::from_slice(&bytes) {
        Ok(entry) => entry,
        Err(_) => return Ok(None),
    };
    if entry.schema != SCHEMA_VERSION
        || entry.source_key != source_key
        || entry.saved_at.duration_since(UNIX_EPOCH).is_err()
        || validate_nodes(&entry.nodes).is_err()
        || !entry.validators.is_valid()
    {
        return Ok(None);
    }
    let age = match now.duration_since(entry.saved_at) {
        Ok(age) => age,
        Err(_) => return Ok(None),
    };
    let fresh = age < policy.ttl;
    let stale_deadline = policy
        .ttl
        .checked_add(policy.max_stale)
        .ok_or(Error::Config("cache lifetime is too large"))?;
    if !fresh && (policy.max_stale.is_zero() || age >= stale_deadline) {
        return Ok(None);
    }
    Ok(Some(CachedNodes {
        nodes: entry.nodes,
        fresh,
        validators: entry.validators,
    }))
}

fn save(directory: &Path, entry: &CacheEntry) -> Result<()> {
    let bytes =
        serde_json::to_vec(entry).map_err(|_| Error::Cache("cache serialization failed"))?;
    if bytes.len() as u64 > MAX_CACHE_BYTES {
        return Err(Error::Cache("cache exceeds maximum file size"));
    }
    let mut directories = fs::DirBuilder::new();
    directories.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directories.mode(0o700);
    }
    directories.create(directory).map_err(cache_io_error)?;
    let mut builder = tempfile::Builder::new();
    builder.prefix(".subscription-proxy-").suffix(".tmp");
    #[cfg(not(windows))]
    let mut temporary = builder.tempfile_in(directory).map_err(cache_io_error)?;
    #[cfg(windows)]
    let mut temporary = builder
        // std::fs::rename preserves file attributes, so create a normal file
        // instead of tempfile's FILE_ATTRIBUTE_TEMPORARY Windows default.
        .make_in(directory, |path| {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
        })
        .map_err(cache_io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(cache_io_error)?;
    }
    temporary.write_all(&bytes).map_err(cache_io_error)?;
    temporary.as_file().sync_all().map_err(cache_io_error)?;
    #[cfg(windows)]
    {
        // Unlike tempfile's MoveFileExW-only persist, std::fs::rename can
        // replace an open destination using Windows POSIX rename semantics.
        // Keep the cleanup guard until the rename has succeeded or failed.
        let temporary = temporary.into_temp_path();
        fs::rename(&temporary, cache_path(directory, &entry.source_key)).map_err(cache_io_error)?;
    }
    #[cfg(not(windows))]
    temporary
        .persist(cache_path(directory, &entry.source_key))
        .map_err(|error| cache_io_error(error.error))?;
    Ok(())
}

fn cache_io_error(error: std::io::Error) -> Error {
    // Some filesystem helpers include paths in their contextual error strings.
    // Preserve OS codes or the error kind without retaining caller-supplied paths.
    Error::Io(match error.raw_os_error() {
        Some(code) => std::io::Error::from_raw_os_error(code),
        None => std::io::Error::from(error.kind()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_KEY: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    fn node(port: u16) -> ProxyNode {
        ProxyNode::from_url(&format!("http://user:password@localhost:{port}")).unwrap()
    }

    fn entry(saved_at: SystemTime) -> CacheEntry {
        CacheEntry {
            schema: SCHEMA_VERSION,
            source_key: KEY.to_owned(),
            saved_at,
            nodes: vec![node(8080)],
            validators: SubscriptionValidators::default(),
        }
    }

    fn put_entry(directory: &Path, entry: &CacheEntry) {
        fs::write(
            cache_path(directory, KEY),
            serde_json::to_vec(entry).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn policy_checks_deadlines() {
        let mut policy = CachePolicy::new("cache");
        assert_eq!(policy.ttl, Duration::from_secs(3 * 24 * 60 * 60));
        assert_eq!(policy.max_stale, Duration::from_secs(7 * 24 * 60 * 60));
        assert!(policy.validate().is_ok());
        policy.ttl = Duration::ZERO;
        assert!(policy.validate().is_err());
        policy.ttl = Duration::MAX;
        assert!(policy.validate().is_err());
        policy.max_stale = Duration::ZERO;
        assert!(policy.validate().is_ok());
    }

    #[tokio::test]
    async fn atomic_replacement_preserves_nodes_and_restricts_file_permissions() {
        let directory = tempfile::tempdir().unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        assert!(store.load(KEY).await.unwrap().is_none());
        store.save(KEY, &[node(8080)]).await.unwrap();
        let original_file = File::open(cache_path(directory.path(), KEY)).unwrap();
        store.save(KEY, &[node(9090)]).await.unwrap();
        let loaded = store.load(KEY).await.unwrap().unwrap();
        assert!(loaded.fresh);
        assert_eq!(loaded.nodes, vec![node(9090)]);
        // An open handle keeps reading the previous file after replacement.
        let old: CacheEntry = serde_json::from_reader(original_file).unwrap();
        assert_eq!(old.nodes, vec![node(8080)]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(cache_path(directory.path(), KEY))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn validators_round_trip_and_revalidation_renews_freshness() {
        let directory = tempfile::tempdir().unwrap();
        let policy = CachePolicy::new(directory.path());
        let mut old = entry(SystemTime::now() - policy.ttl - Duration::from_secs(1));
        old.validators = SubscriptionValidators {
            etag: Some("W/\"revision-1\"".to_owned()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".to_owned()),
        };
        put_entry(directory.path(), &old);
        let store = CacheStore::new(policy);
        let cached = store.load(KEY).await.unwrap().unwrap();
        assert!(!cached.fresh);
        assert_eq!(cached.validators.etag, old.validators.etag);
        assert_eq!(
            cached.validators.last_modified,
            old.validators.last_modified
        );

        // A 304 confirms these nodes are still current and renews the deadlines.
        store
            .save_with_validators(KEY, &cached.nodes, &cached.validators)
            .await
            .unwrap();
        let cached = store.load(KEY).await.unwrap().unwrap();
        assert!(cached.fresh);
        assert_eq!(cached.nodes, old.nodes);
        assert_eq!(cached.validators.etag, old.validators.etag);
        assert_eq!(
            cached.validators.last_modified,
            old.validators.last_modified
        );
    }

    #[tokio::test]
    async fn original_schema_one_files_without_validators_remain_readable() {
        let directory = tempfile::tempdir().unwrap();
        let mut legacy = serde_json::to_value(entry(SystemTime::now())).unwrap();
        legacy.as_object_mut().unwrap().remove("validators");
        fs::write(
            cache_path(directory.path(), KEY),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        let cached = store.load(KEY).await.unwrap().unwrap();
        assert!(cached.fresh);
        assert_eq!(cached.nodes, vec![node(8080)]);
        assert!(cached.validators.etag.is_none());
        assert!(cached.validators.last_modified.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn new_directories_are_private_without_changing_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o750)).unwrap();
        let parent = directory.path().join("new-parent");
        let child = parent.join("cache");
        let store = CacheStore::new(CachePolicy::new(&child));
        store.save(KEY, &[node(8080)]).await.unwrap();
        for path in [&parent, &child] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        assert_eq!(
            fs::metadata(directory.path()).unwrap().permissions().mode() & 0o777,
            0o750
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symbolic_link_entries_are_misses_and_writes_replace_only_the_link() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original-secret");
        let original_bytes = serde_json::to_vec(&entry(SystemTime::now())).unwrap();
        fs::write(&original, &original_bytes).unwrap();
        let cached = cache_path(directory.path(), KEY);
        symlink(&original, &cached).unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        assert!(store.load(KEY).await.unwrap().is_none());
        store.save(KEY, &[node(9090)]).await.unwrap();
        assert_eq!(fs::read(&original).unwrap(), original_bytes);
        assert!(fs::symlink_metadata(cached).unwrap().is_file());
        assert_eq!(
            store.load(KEY).await.unwrap().unwrap().nodes,
            vec![node(9090)]
        );
    }

    #[tokio::test]
    async fn concurrent_replacements_keep_validators_with_their_nodes() {
        let directory = tempfile::tempdir().unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        let mut writers = tokio::task::JoinSet::new();
        for port in 8080..8096 {
            let store = store.clone();
            writers.spawn(async move {
                store
                    .save_with_validators(
                        KEY,
                        &[node(port)],
                        &SubscriptionValidators {
                            etag: Some(format!("\"{port}\"")),
                            last_modified: None,
                        },
                    )
                    .await
                    .unwrap();
                let cached = store.load(KEY).await.unwrap().unwrap();
                let etag = cached.validators.etag.unwrap();
                let port = etag.trim_matches('"').parse().unwrap();
                assert_eq!(cached.nodes, vec![node(port)]);
            });
        }
        while let Some(result) = writers.join_next().await {
            result.unwrap();
        }
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn contextual_io_errors_do_not_reveal_private_paths() {
        let error = cache_io_error(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "could not create /private/token=secret/cache.json",
        ));
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(
            matches!(error, Error::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn lifetime_and_stale_fallback_have_fixed_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let mut policy = CachePolicy::new(directory.path());
        policy.ttl = Duration::from_secs(10);
        policy.max_stale = Duration::from_secs(20);
        let saved_at = UNIX_EPOCH + Duration::from_secs(100);
        put_entry(directory.path(), &entry(saved_at));

        assert!(
            load(&policy, KEY, saved_at + Duration::from_secs(9))
                .unwrap()
                .unwrap()
                .fresh
        );
        for age in [10, 29] {
            assert!(
                !load(&policy, KEY, saved_at + Duration::from_secs(age))
                    .unwrap()
                    .unwrap()
                    .fresh
            );
        }
        for age in [30, 31] {
            assert!(
                load(&policy, KEY, saved_at + Duration::from_secs(age))
                    .unwrap()
                    .is_none()
            );
        }
        policy.max_stale = Duration::ZERO;
        assert!(
            load(&policy, KEY, saved_at + Duration::from_secs(10))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn corrupt_schema_source_time_and_nodes_are_cache_misses() {
        let directory = tempfile::tempdir().unwrap();
        let policy = CachePolicy::new(directory.path());
        let now = SystemTime::now();
        fs::write(cache_path(directory.path(), KEY), b"{invalid").unwrap();
        assert!(load(&policy, KEY, now).unwrap().is_none());

        let mut bad_schema = entry(now);
        bad_schema.schema += 1;
        let mut wrong_source = entry(now);
        wrong_source.source_key = OTHER_KEY.to_owned();
        // Windows SystemTime has 100 ns precision; 1 ns can round back to now.
        let future = entry(now + Duration::from_secs(1));
        let mut empty = entry(now);
        empty.nodes.clear();
        let mut invalid = entry(now);
        invalid.nodes = vec![
            serde_json::from_value(serde_json::json!({
                "name": "secret", "endpoint": "http://localhost:0"
            }))
            .unwrap(),
        ];
        for bad in [bad_schema, wrong_source, future, empty, invalid] {
            put_entry(directory.path(), &bad);
            assert!(load(&policy, KEY, now).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn validates_source_keys_and_empty_lists_before_writing() {
        let directory = tempfile::tempdir().unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        for key in [
            "../secret",
            "/tmp/secret",
            "https://subscription/?token=secret",
            "",
            "ABCDEF",
        ] {
            assert!(store.load(key).await.is_err());
            assert!(store.save(key, &[node(8080)]).await.is_err());
        }
        assert!(store.save(KEY, &[]).await.is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn separate_sources_never_share_nodes() {
        let directory = tempfile::tempdir().unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        store.save(KEY, &[node(8080)]).await.unwrap();
        assert!(store.load(OTHER_KEY).await.unwrap().is_none());
        store.save(OTHER_KEY, &[node(9090)]).await.unwrap();
        assert_eq!(
            store.load(KEY).await.unwrap().unwrap().nodes,
            vec![node(8080)]
        );
        assert_eq!(
            store.load(OTHER_KEY).await.unwrap().unwrap().nodes,
            vec![node(9090)]
        );
    }

    #[tokio::test]
    async fn oversized_files_are_misses_and_oversized_writes_preserve_old_cache() {
        let directory = tempfile::tempdir().unwrap();
        let store = CacheStore::new(CachePolicy::new(directory.path()));
        let path = cache_path(directory.path(), KEY);
        File::create(&path)
            .unwrap()
            .set_len(MAX_CACHE_BYTES + 1)
            .unwrap();
        assert!(store.load(KEY).await.unwrap().is_none());
        store.save(KEY, &[node(8080)]).await.unwrap();
        let large_node = node(9090).with_name("x".repeat(MAX_CACHE_BYTES as usize));
        assert!(store.save(KEY, &[large_node]).await.is_err());
        assert_eq!(
            store.load(KEY).await.unwrap().unwrap().nodes,
            vec![node(8080)]
        );
    }

    #[tokio::test]
    async fn filesystem_errors_are_reported() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        fs::write(&path, b"not a directory").unwrap();
        for invalid in [path.clone(), path.join("child")] {
            let store = CacheStore::new(CachePolicy::new(invalid));
            assert!(matches!(store.load(KEY).await, Err(Error::Io(_))));
            assert!(matches!(
                store.save(KEY, &[node(8080)]).await,
                Err(Error::Io(_))
            ));
        }
        let missing = directory.path().join("missing").join("child");
        let store = CacheStore::new(CachePolicy::new(missing));
        assert!(store.load(KEY).await.unwrap().is_none());
    }
}
