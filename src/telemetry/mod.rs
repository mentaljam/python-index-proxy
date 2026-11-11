//! Optional instrumentation. Call sites use the same API in both builds.

#[cfg(not(feature = "metrics"))]
mod disabled;
#[cfg(feature = "metrics")]
mod prometheus;

#[cfg(not(feature = "metrics"))]
pub(crate) use disabled::*;
#[cfg(feature = "metrics")]
pub(crate) use prometheus::*;

pub(crate) type IndexCacheBuilder = moka::future::CacheBuilder<
    String,
    std::sync::Arc<crate::index::PackageDetail>,
    moka::future::Cache<String, std::sync::Arc<crate::index::PackageDetail>>,
>;
