use std::{process::Command, time::Instant};

use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn graceful_completion_waits_for_child_work_registered_before_parent_finishes() {
    let shutdown = Shutdown::default();
    let parent = shutdown.token();
    let (release, ready) = oneshot::channel();
    let (child_release, child_ready) = oneshot::channel();
    let (registered, registration) = oneshot::channel();
    shutdown.spawn({
        let shutdown = shutdown.clone();
        async move {
            ready.await.unwrap();
            shutdown.spawn(async move { child_ready.await.unwrap() });
            drop(parent);
            registered.send(()).unwrap();
        }
    });
    let mut finished = Box::pin(shutdown.finish_with_timeouts(
        async { Ok(()) },
        Duration::from_secs(5),
        Duration::from_secs(1),
    ));
    assert!(futures::poll!(&mut finished).is_pending());
    release.send(()).unwrap();
    // The child remains counted even if the parent's final token is dropped.
    registration.await.unwrap();
    assert!(futures::poll!(&mut finished).is_pending());
    child_release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("completed work must stop without waiting for the grace deadline")
        .unwrap();
    assert!(!shutdown.is_cancelled());
}

#[tokio::test]
async fn grace_expiry_cancels_then_waits_for_cleanup_and_the_server() {
    let shutdown = Shutdown::default();
    let (cleaned, mut cleanup) = oneshot::channel();
    shutdown.spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown.cancelled().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            cleaned.send(()).unwrap();
        }
    });
    let server = async {
        shutdown.cancelled().await;
        Ok(())
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        shutdown.finish_with_timeouts(server, Duration::from_millis(20), Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        cleanup.try_recv().is_ok(),
        "shutdown returned before owned cleanup completed"
    );
    assert!(shutdown.is_cancelled());
}

#[test]
fn process_exit_does_not_wait_for_tracked_or_untracked_blocking_work() {
    for (mode, expected) in [("deadline", 1), ("clean", 0)] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shutdown::tests::process_exit_child",
                "--ignored",
            ])
            .env("PIPR_TEST_EXIT_MODE", mode)
            .spawn()
            .unwrap();
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if started.elapsed() > Duration::from_secs(5) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("{mode} shutdown waited for a permanently blocked runtime worker");
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(status.code(), Some(expected), "{mode}");
    }
}

#[test]
#[ignore = "Subprocess fixture invoked by process_exit_does_not_wait_for_tracked_or_untracked_blocking_work"]
fn process_exit_child() {
    let mode = std::env::var("PIPR_TEST_EXIT_MODE").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let shutdown = Shutdown::default();
        let (started, running) = oneshot::channel();
        let blocked = move || {
            started.send(()).unwrap();
            loop {
                std::thread::park();
            }
        };
        if mode == "deadline" {
            shutdown.spawn_blocking(blocked);
        } else {
            // DNS and filesystem libraries can leave blocking work outside our tracker.
            tokio::task::spawn_blocking(blocked);
        }
        running.await.unwrap();
        let result = shutdown
            .finish_with_timeouts(
                async { Ok(()) },
                Duration::from_millis(20),
                Duration::from_millis(20),
            )
            .await;
        assert_eq!(result.is_err(), mode == "deadline");
        super::exit(result);
    });
}
