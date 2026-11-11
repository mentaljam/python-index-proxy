use std::{future::Future, pin::Pin, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};

const GRACE_PERIOD: Duration = Duration::from_secs(25);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

mod http;

#[derive(Clone, Default)]
pub(crate) struct Shutdown {
    cancellation: CancellationToken,
    tasks: TaskTracker,
}

impl Shutdown {
    async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    // Only use this for cancellation-safe waits, never storage writer operations.
    pub(crate) async fn until_cancelled<F: Future>(&self, future: F) -> Option<F::Output> {
        tokio::select! {
            biased;
            _ = self.cancelled() => None,
            output = future => Some(output),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn token(&self) -> TaskTrackerToken {
        self.tasks.token()
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }

    pub(crate) fn spawn_blocking<F, T>(&self, work: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.tasks.spawn_blocking(work)
    }

    async fn finish(
        &self,
        server: impl Future<Output = std::io::Result<()>>,
    ) -> anyhow::Result<()> {
        self.finish_with_timeouts(server, GRACE_PERIOD, CLEANUP_TIMEOUT)
            .await
    }

    async fn finish_with_timeouts(
        &self,
        server: impl Future<Output = std::io::Result<()>>,
        grace: Duration,
        cleanup: Duration,
    ) -> anyhow::Result<()> {
        let started = tokio::time::Instant::now();
        let grace_deadline = started + grace;
        let final_deadline = grace_deadline + cleanup;
        let mut server = std::pin::pin!(server);
        let mut server_finished = false;
        self.tasks.close();
        if let Ok(result) = tokio::time::timeout_at(
            grace_deadline,
            self.drain(server.as_mut(), &mut server_finished),
        )
        .await
        {
            return Ok(result?);
        }

        tracing::warn!("Shutdown grace period ended; cancelling active requests and downloads");
        self.cancel();
        tokio::time::timeout_at(
            final_deadline,
            self.drain(server.as_mut(), &mut server_finished),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Shutdown deadline exceeded; forcing process exit with {} tracked task(s) unfinished",
                self.tasks.len()
            )
        })??;
        Ok(())
    }

    async fn drain<F>(&self, server: Pin<&mut F>, server_finished: &mut bool) -> std::io::Result<()>
    where
        F: Future<Output = std::io::Result<()>>,
    {
        if !*server_finished {
            let result = server.await;
            *server_finished = true;
            result?;
        }
        // HTTP work holds a token until its body is dropped. Child work is
        // registered before spawning, so completion cannot race registration.
        self.tasks.wait().await;
        Ok(())
    }
}

pub(crate) fn exit(result: anyhow::Result<()>) -> ! {
    if let Err(err) = result {
        tracing::error!(
            error = crate::logging::error_kind(&err),
            "Application failed"
        );
        std::process::exit(1);
    }
    // Explicitly drained HTTP requests and owned work are complete. Do not let
    // Runtime::drop wait forever for library-internal DNS or filesystem jobs.
    std::process::exit(0);
}

#[cfg(test)]
mod tests;
