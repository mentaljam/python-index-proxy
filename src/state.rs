use std::{collections::HashMap, sync::Arc};

use moka::future::Cache;
use opendal::Operator;
use reqwest::Client;
use tokio::sync::{Mutex, RwLock};

use crate::{
    coordination::KeyedFlights,
    index::{IndexRefresh, IndexUrlTemplate, PackageDetail, format_indices_list},
    settings::Settings,
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
        let Settings {
            base_url,
            index_cache,
            indices,
            storage,
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
            http_client: Client::new(),
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
