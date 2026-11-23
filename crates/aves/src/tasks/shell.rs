use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout};

use super::types::{
    SHELL_DRAIN_WINDOW, SHELL_OUTPUT_CAP_BYTES, SHELL_REAP_TIMEOUT, ShellPostSpawnFailureKind, ShellRunSpec,
    ShellTerminal, ShellTerminalCause,
};

#[derive(Default)]
struct Capture {
    bytes: Vec<u8>,
    dropped: u64,
    complete: bool,
    failed: bool,
}

/// Assembles one script without changing the interpreter's failure semantics.
pub(crate) fn join_shell_commands(commands: &[String]) -> String {
    commands.join("\n")
}

async fn collect<R: AsyncRead + Unpin>(mut reader: R, capture: Arc<Mutex<Capture>>) {
    let mut buffer = [0_u8; 8192];
    loop {
        let result = reader.read(&mut buffer).await;
        let mut capture = capture.lock().unwrap_or_else(|error| error.into_inner());
        match result {
            Ok(0) => {
                capture.complete = true;
                return;
            }
            Ok(count) => {
                let retained = count.min(SHELL_OUTPUT_CAP_BYTES - capture.bytes.len());
                capture.bytes.extend_from_slice(&buffer[..retained]);
                capture.dropped = capture.dropped.saturating_add((count - retained) as u64);
            }
            Err(_) => {
                capture.failed = true;
                return;
            }
        }
    }
}

fn failure(kind: ShellPostSpawnFailureKind) -> ShellTerminalCause {
    ShellTerminalCause::PostSpawnFailed { kind }
}

fn exit_cause(status: ExitStatus) -> ShellTerminalCause {
    if let Some(code) = status.code() {
        return ShellTerminalCause::Exited { code };
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return ShellTerminalCause::Signaled {
                description: format!("signal {signal}"),
            };
        }
    }
    ShellTerminalCause::Signaled {
        description: status.to_string(),
    }
}

async fn terminate_child(child: &mut Child, cause: ShellTerminalCause) -> ShellTerminalCause {
    let kill_failed = child.start_kill().is_err();
    let reaped = matches!(timeout(SHELL_REAP_TIMEOUT, child.wait()).await, Ok(Ok(_)));
    if kill_failed {
        failure(ShellPostSpawnFailureKind::Kill)
    } else if !reaped {
        failure(ShellPostSpawnFailureKind::Reap)
    } else {
        cause
    }
}

/// Runs one interpreter; only that child is terminated, never its descendants.
/// Reaping after termination is bounded to 2s, followed by one shared 5s pipe drain.
pub async fn run_shell(spec: ShellRunSpec, mut terminate: oneshot::Receiver<()>) -> ShellTerminal {
    let deadline = Instant::now() + spec.timeout;
    let mut child = match Command::new(&spec.interpreter)
        .arg("-c")
        .arg(&spec.script)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .current_dir(&spec.cwd)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => {
            return ShellTerminal {
                cause: ShellTerminalCause::SpawnFailed {
                    interpreter: spec.interpreter,
                },
                stdout: String::new(),
                stderr: String::new(),
                stdout_dropped_bytes: 0,
                stderr_dropped_bytes: 0,
                stdout_abandoned_tail: false,
                stderr_abandoned_tail: false,
            };
        }
    };

    let stdout = Arc::new(Mutex::new(Capture::default()));
    let stderr = Arc::new(Mutex::new(Capture::default()));
    // JoinSet aborts both collectors if the runner itself is dropped.
    let mut collectors = JoinSet::new();
    if let Some(pipe) = child.stdout.take() {
        collectors.spawn(collect(pipe, Arc::clone(&stdout)));
    } else {
        stdout
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failed = true;
    }
    if let Some(pipe) = child.stderr.take() {
        collectors.spawn(collect(pipe, Arc::clone(&stderr)));
    } else {
        stderr
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failed = true;
    }

    let mut terminate_closed = false;
    let mut cause = loop {
        tokio::select! {
            biased;
            result = child.wait() => break match result {
                Ok(status) => exit_cause(status),
                Err(_) => failure(ShellPostSpawnFailureKind::Reap),
            },
            _ = sleep_until(deadline) => {
                break terminate_child(&mut child, ShellTerminalCause::TimedOut { seconds: spec.timeout.as_secs() }).await;
            }
            result = &mut terminate, if !terminate_closed => {
                if result.is_ok() {
                    break terminate_child(&mut child, ShellTerminalCause::Cancelled).await;
                }
                terminate_closed = true;
            }
        }
    };

    let mut collector_failed = false;
    let drained = timeout(SHELL_DRAIN_WINDOW, async {
        while let Some(result) = collectors.join_next().await {
            collector_failed |= result.is_err();
        }
    })
    .await;
    if drained.is_err() {
        collectors.shutdown().await;
    }

    let stdout = stdout.lock().unwrap_or_else(|error| error.into_inner());
    let stderr = stderr.lock().unwrap_or_else(|error| error.into_inner());
    // Termination and kill/reap failures win; pipe failures override natural exits.
    if (stdout.failed || stderr.failed || collector_failed)
        && matches!(
            cause,
            ShellTerminalCause::Exited { .. } | ShellTerminalCause::Signaled { .. }
        )
    {
        cause = failure(ShellPostSpawnFailureKind::PipeRead);
    }
    ShellTerminal {
        cause,
        stdout: String::from_utf8_lossy(&stdout.bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
        stdout_dropped_bytes: stdout.dropped,
        stderr_dropped_bytes: stderr.dropped,
        stdout_abandoned_tail: !stdout.complete,
        stderr_abandoned_tail: !stderr.complete,
    }
}
