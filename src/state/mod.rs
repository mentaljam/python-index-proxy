use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use moka::future::Cache;
use opendal::Operator;
use reqwest::Client;
use tokio::sync::{Mutex, Semaphore};

use crate::{
    coordination::KeyedFlights,
    index::{ConfiguredIndex, IndexRefresh, IndexUrlTemplate, PackageDetail, format_indices_list},
    settings::{Limits, Settings, Upstream},
    shutdown::Shutdown,
};

#[derive(Clone)]
pub(crate) struct State(pub(crate) Arc<StateData>);

impl State {
    pub fn get_indices_list(&self) -> Bytes {
        self.0.indices_list.clone()
    }
}

impl TryFrom<Settings> for State {
    type Error = anyhow::Error;

    fn try_from(settings: Settings) -> Result<Self, Self::Error> {
        let Settings {
            bind,
            path_prefix,
            public_url,
            index_cache,
            indices,
            storage,
            limits,
            upstream,
            storage_http,
            ..
        } = settings;

        let index_cache = crate::telemetry::index_cache(
            Cache::builder()
                .time_to_live(index_cache.ttl.into())
                .max_capacity(index_cache.max_capacity.into())
                // Account for key, rendered HTML/JSON and source mappings, not allocator overhead.
                .weigher(|key: &String, value: &Arc<PackageDetail>| {
                    u32::try_from(key.len().saturating_add(value.byte_len())).unwrap_or(u32::MAX)
                }),
        )
        .build();

        let public_url = public_url.unwrap_or_else(|| format!("http://{bind}{path_prefix}"));
        let indices_list = format_indices_list(&public_url, &indices).into();

        let indices = indices
            .into_iter()
            .map(|(slug, index)| {
                (
                    slug,
                    ConfiguredIndex {
                        sources: index.sources.into_iter().map(IndexUrlTemplate).collect(),
                        policy: index.policy,
                    },
                )
            })
            .collect();

        let storage = storage.operator(storage_http)?;

        let data = StateData {
            shutdown: Shutdown::default(),
            public_url,
            http_client: Client::builder()
                .connect_timeout(upstream.connect_timeout)
                .build()?,
            index_permits: Arc::new(Semaphore::new(limits.max_index_refreshes)),
            artifact_permits: Arc::new(Semaphore::new(limits.max_artifact_downloads)),
            limits,
            upstream,
            index_cache,
            indices,
            indices_list,
            storage,
            download_locks: KeyedFlights::default(),
            index_refreshes: KeyedFlights::default(),
        };

        Ok(Self(Arc::new(data)))
    }
}

pub(crate) struct StateData {
    pub(crate) shutdown: Shutdown,
    pub(crate) limits: Limits,
    pub(crate) upstream: Upstream,
    pub(crate) index_permits: Arc<Semaphore>,
    pub(crate) artifact_permits: Arc<Semaphore>,
    pub(crate) public_url: String,
    pub(crate) http_client: Client,
    pub(crate) index_cache: Cache<String, Arc<PackageDetail>>,
    pub(crate) indices: HashMap<String, ConfiguredIndex>,
    indices_list: Bytes,
    pub(crate) storage: Operator,
    pub(crate) download_locks: KeyedFlights<Mutex<()>>,
    pub(crate) index_refreshes: KeyedFlights<IndexRefresh>,
}

#[cfg(test)]
mod tests;
