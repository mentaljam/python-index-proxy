use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;

use crate::{index::PackageDetail, settings::Settings, state::State};

pub(crate) fn index_failure_state(sources: Vec<String>) -> (State, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "pipr-index-failure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let mut settings = Settings::for_test();
    settings.indices = [("default".into(), sources)].into();
    let mut config = opendal::services::FsConfig::default();
    config.root = Some(root.to_str().unwrap().into());
    settings.storage = crate::storage::Storage::Fs(config);
    (State::try_from(settings).unwrap(), root)
}

pub(crate) fn page(html: &'static [u8]) -> Arc<PackageDetail> {
    Arc::new(PackageDetail {
        html: Bytes::from_static(html),
        files: HashMap::new(),
    })
}
