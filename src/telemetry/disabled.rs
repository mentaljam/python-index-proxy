use axum::{Router, extract::Request};
use opendal::Operator;

use crate::state::State;

pub(crate) fn install(router: Router<State>, _path_prefix: &str) -> anyhow::Result<Router<State>> {
    Ok(router)
}

pub(crate) fn storage(operator: Operator) -> Operator {
    operator
}

pub(crate) fn index_cache(builder: super::IndexCacheBuilder) -> super::IndexCacheBuilder {
    builder
}

pub(crate) fn rejected(_kind: &'static str) {}
pub(crate) fn upstream_response(_kind: &'static str, _status: u16) {}
pub(crate) fn upstream_bytes(_bytes: usize) {}
pub(crate) fn delivery(_kind: &'static str) {}

pub(crate) struct Http;

impl Http {
    pub(crate) fn start(_request: &Request) -> Self {
        Self
    }
    pub(crate) fn headers(&mut self, _status: u16) {}
    pub(crate) fn bytes(&mut self, _bytes: usize) {}
    pub(crate) fn finish(
        &mut self,
        _outcome: &'static str,
        _cache: Option<&'static str>,
        _cache_result: Option<&'static str>,
        _error: Option<&'static str>,
    ) {
    }
}

pub(crate) struct Operation;

impl Operation {
    pub(crate) fn start(_kind: &'static str) -> Self {
        Self
    }
    pub(crate) fn finish(&mut self, _outcome: &'static str) {}
    pub(crate) fn result<T>(&mut self, _result: &anyhow::Result<T>) {}
}
