use std::{collections::HashMap, sync::Arc};

use moka::future::Cache;
use opendal::Operator;
use reqwest::Client;
use tokio::sync::{Mutex, RwLock, Semaphore};

use crate::{
    coordination::KeyedFlights,
    index::{IndexRefresh, IndexUrlTemplate, PackageDetail, format_indices_list},
    settings::{Limits, Settings, Upstream},
};

#[derive(Clone)]
pub(crate) struct State(pub(crate) Arc<RwLock<StateData>>);

impl State {
    pub async fn get_indices_list(&self) -> String {
        self.0.read().await.indices_list.clone()
    }
}

impl TryFrom<Settings> for State {
    type Error = anyhow::Error;

    fn try_from(settings: Settings) -> Result<Self, Self::Error> {
        settings.validate_resources()?;
        let Settings {
            base_url,
            index_cache,
            indices,
            storage,
            limits,
            upstream,
            ..
        } = settings;

        let index_cache = Cache::builder()
            .time_to_live(index_cache.ttl.into())
            .max_capacity(index_cache.max_capacity.into())
            // Account for key, rendered HTML and source mappings, not allocator overhead.
            .weigher(|key: &String, value: &Arc<PackageDetail>| {
                u32::try_from(key.len().saturating_add(value.byte_len())).unwrap_or(u32::MAX)
            })
            .build();

        let indices_list = format_indices_list(&base_url, &indices);

        let indices = indices
            .into_iter()
            .map(|(slug, urls)| (slug, urls.into_iter().map(IndexUrlTemplate).collect()))
            .collect();

        let storage = storage.operator()?;

        let data = StateData {
            base_url,
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

        Ok(Self(Arc::new(data.into())))
    }
}

pub(crate) struct StateData {
    pub(crate) limits: Limits,
    pub(crate) upstream: Upstream,
    pub(crate) index_permits: Arc<Semaphore>,
    pub(crate) artifact_permits: Arc<Semaphore>,
    pub(crate) base_url: String,
    pub(crate) http_client: Client,
    pub(crate) index_cache: Cache<String, Arc<PackageDetail>>,
    pub(crate) indices: HashMap<String, Vec<IndexUrlTemplate>>,
    indices_list: String,
    pub(crate) storage: Operator,
    pub(crate) download_locks: KeyedFlights<Mutex<()>>,
    pub(crate) index_refreshes: KeyedFlights<IndexRefresh>,
}

#[cfg(test)]
mod tests;
