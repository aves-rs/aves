use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::runtime::Handle;
use wasmtime::{Config, Engine, Error as WasmtimeError, Instance, Linker, Memory, Module, Store, Trap, TypedFunc};
use wasmtime_wasi::cli::{AsyncStdinStream, AsyncStdoutStream};
use wasmtime_wasi::p1::{self, WasiP1Ctx};
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use wasmtime_wasi::{FsPerms, WasiCtxBuilder};

use crate::flow::LifecycleCorrelation;

use super::client::GuestRpcBindingsHandle;
use super::types::{ScriptNodeCompletion, TasksError};

const QUICKJS_RUNTIME: &[u8] = include_bytes!("./runtime/qjs-wasi-reactor.wasm");
const STDOUT_CAPACITY: usize = 64 * 1024;
const STDERR_CAPACITY: usize = 64 * 1024;
const GUEST_RPC_PREFIX: &str = "\u{1e}SLUICE_RPC ";
const GUEST_OUTPUT_PREFIX: &str = "\u{1e}SLUICE_OUTPUT ";
const USER_MODULE_ALIASES: &str = concat!(
    "const __sluice_outputs = globalThis.__sluice_outputs; ",
    "const __sluice_errors = globalThis.__sluice_errors; ",
    "const __sluice_logs = globalThis.__sluice_logs; ",
    "const sluice = globalThis.sluice;\n",
);

#[derive(Deserialize)]
struct GuestRpcRequest {
    operation: String,
    input: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GuestRpcResponse {
    ok: bool,
    value: Option<Value>,
    error: Option<String>,
}

struct State {
    wasi: WasiP1Ctx,
    stderr: MemoryOutputPipe,
}
struct Exports {
    malloc: TypedFunc<i32, i32>,
    free: TypedFunc<i32, ()>,
    init: TypedFunc<(i32, i32), i32>,
    context: TypedFunc<(), i32>,
    loop_once: TypedFunc<(i32,), i32>,
    destroy: TypedFunc<(), ()>,
    memory: Memory,
}
struct Region {
    ptr: i32,
}
struct TimeoutGuard {
    cancel: mpsc::Sender<()>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ScriptAbortState {
    cancelled: AtomicBool,
    engine: Mutex<Option<Engine>>,
}

/// Immediate interruption handle for one running ScriptNode runtime.
#[derive(Clone)]
pub(super) struct ScriptAbortHandle {
    state: Arc<ScriptAbortState>,
}

impl ScriptAbortHandle {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(ScriptAbortState {
                cancelled: AtomicBool::new(false),
                engine: Mutex::new(None),
            }),
        }
    }

    pub(super) fn abort(&self) {
        self.state.cancelled.store(true, Ordering::SeqCst);
        if let Some(engine) = self
            .state
            .engine
            .lock()
            .expect("ScriptNode abort lock poisoned")
            .as_ref()
        {
            engine.increment_epoch();
        }
    }

    fn attach(&self, engine: &Engine) {
        *self
            .state
            .engine
            .lock()
            .expect("ScriptNode abort lock poisoned") = Some(engine.clone());
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::SeqCst)
    }
}

/// One fully resolved ScriptNode invocation at the sandbox boundary.
pub(super) struct CompiledScript {
    source: String,
    filename: String,
}

impl CompiledScript {
    pub(super) fn new(source: String, filename: String) -> Self {
        Self { source, filename }
    }
}

pub(super) struct ScriptExecution {
    correlation: LifecycleCorrelation,
    script: CompiledScript,
    input: Value,
    fuel: u64,
    timeout: u64,
    workspace_path: PathBuf,
    guest_rpc: GuestRpcBindingsHandle,
}

impl ScriptExecution {
    pub(super) fn new(
        correlation: LifecycleCorrelation,
        script: CompiledScript,
        input: Value,
        fuel: u64,
        timeout: u64,
        workspace_path: PathBuf,
        guest_rpc: GuestRpcBindingsHandle,
    ) -> Self {
        Self {
            correlation,
            script,
            input,
            fuel,
            timeout,
            workspace_path,
            guest_rpc,
        }
    }
}

struct BridgeExecution<'a> {
    correlation: LifecycleCorrelation,
    compiled: &'a str,
    filename: &'a str,
    input: &'a Value,
    fuel: u64,
    timeout: u64,
    workspace_path: &'a Path,
}

struct GuestStreams {
    stdin: DuplexStream,
    stdout: DuplexStream,
}

/// Fresh QuickJS-in-Wasmtime execution boundary with fuel and epoch limits.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ScriptSandbox;

impl ScriptSandbox {
    /// Constructs a runner that creates an isolated engine and module per invocation.
    pub(super) fn new() -> Result<Self, TasksError> {
        Ok(Self)
    }

    /// Runs compiled JavaScript in a fresh guest engine and module.
    pub(super) fn run(
        &self,
        execution: ScriptExecution,
        abort: ScriptAbortHandle,
    ) -> Result<ScriptNodeCompletion, TasksError> {
        let ScriptExecution {
            correlation,
            script,
            input,
            fuel,
            timeout,
            workspace_path,
            guest_rpc,
        } = execution;
        let (guest_stdin, host_responses) = tokio::io::duplex(STDOUT_CAPACITY);
        let (host_requests, guest_stdout) = tokio::io::duplex(STDOUT_CAPACITY);
        let relay = Handle::current().spawn(relay_guest_rpc(host_requests, host_responses, guest_rpc, timeout));
        let execution = BridgeExecution {
            correlation,
            compiled: &script.source,
            filename: &script.filename,
            input: &input,
            fuel,
            timeout,
            workspace_path: workspace_path.as_path(),
        };
        let streams = GuestStreams {
            stdin: guest_stdin,
            stdout: guest_stdout,
        };
        #[cfg(test)]
        let execution_result = {
            let runtime = test_runtime();
            let _runtime_guard = runtime
                .lock
                .lock()
                .expect("QuickJS test runtime lock poisoned");
            execute_bridge_with_runtime(
                &execution,
                streams,
                runtime.engine.clone(),
                runtime.module.clone(),
                &abort,
            )
        };
        #[cfg(not(test))]
        let execution_result = execute_bridge(&execution, streams, &abort);
        if let Err(error) = execution_result {
            relay.abort();
            return Err(error);
        }
        let relay_result = tokio::runtime::Handle::current()
            .block_on(relay)
            .map_err(|error| TasksError::GuestRpcRelay {
                message: error.to_string(),
            })?;
        let output = relay_result?;
        parse_output(&output)
    }
}

fn staging_filenames(correlation: LifecycleCorrelation) -> (String, String) {
    (
        format!(".sluice-script-{}.mjs", correlation.node_execution_id),
        format!(".sluice-bootstrap-{}.mjs", correlation.node_execution_id),
    )
}

fn staging_paths(
    workspace: &Path,
    filename: &str,
    correlation: LifecycleCorrelation,
) -> Result<(PathBuf, PathBuf, String), TasksError> {
    let entry_path = Path::new(filename);
    if filename.is_empty()
        || entry_path.is_absolute()
        || entry_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(TasksError::Execution {
            message: format!("invalid ScriptNode entry point: {filename}"),
        });
    }
    let (script_name, bootstrap_name) = staging_filenames(correlation);
    let parent = entry_path.parent().unwrap_or_else(|| Path::new(""));
    let script_relative = parent.join(script_name);
    let script_guest_path = format!("/{}", script_relative.to_string_lossy().replace('\\', "/"));
    Ok((
        workspace.join(script_relative),
        workspace.join(bootstrap_name),
        script_guest_path,
    ))
}

fn execute_bridge_exports(
    execution: &BridgeExecution<'_>,
    timed_out: &Arc<AtomicBool>,
    module: &Module,
    linker: &Linker<State>,
    store: &mut Store<State>,
) -> Result<(), TasksError> {
    let instance = linker
        .instantiate(&mut *store, module)
        .map_err(|error| trap(error, timed_out, execution.timeout))?;
    initialize(&mut *store, &instance, timed_out, execution.timeout)?;
    let exports = bind(&mut *store, &instance)?;
    let result = execute(
        &mut *store,
        &exports,
        execution.correlation.clone(),
        execution.timeout,
        timed_out,
    );
    let destroy = exports.destroy.call(&mut *store, ());
    result?;
    destroy.map_err(|error| trap(error, timed_out, execution.timeout))?;
    Ok(())
}

fn execute_bridge_instance(
    execution: &BridgeExecution<'_>,
    engine: Engine,
    module: Module,
    linker: Linker<State>,
    store: &mut Store<State>,
    abort: &ScriptAbortHandle,
) -> Result<(), TasksError> {
    store.set_fuel(execution.fuel)?;
    let timed_out = Arc::new(AtomicBool::new(false));
    let guard = arm_timeout(&engine, store, execution.timeout, Arc::clone(&timed_out));
    if abort.is_cancelled() {
        engine.increment_epoch();
    }
    let result = execute_bridge_exports(execution, &timed_out, &module, &linker, store);
    guard.finish()?;
    result
}

fn execute_bridge_store(
    execution: &BridgeExecution<'_>,
    streams: GuestStreams,
    engine: Engine,
    module: Module,
    abort: &ScriptAbortHandle,
) -> Result<(), TasksError> {
    abort.attach(&engine);
    let mut wasi = WasiCtxBuilder::new();
    let stderr = MemoryOutputPipe::new(STDERR_CAPACITY);
    wasi.stdin(AsyncStdinStream::new(streams.stdin));
    wasi.stdout(AsyncStdoutStream::new(STDOUT_CAPACITY, streams.stdout));
    wasi.stderr(stderr.clone());
    wasi.arg("qjs").arg("--std");
    wasi.preopened_dir(execution.workspace_path, "/", FsPerms::ReadWrite)
        .map_err(|error| TasksError::Execution {
            message: format!("failed to preopen ScriptNode workspace: {error}"),
        })?;
    let mut linker = Linker::new(&engine);
    p1::add_to_linker_sync(&mut linker, |state: &mut State| &mut state.wasi)?;
    let mut store = Store::new(
        &engine,
        State {
            wasi: wasi.build_p1(),
            stderr,
        },
    );
    execute_bridge_instance(execution, engine, module, linker, &mut store, abort)
}

#[cfg(not(test))]
fn execute_bridge(
    execution: &BridgeExecution<'_>,
    streams: GuestStreams,
    abort: &ScriptAbortHandle,
) -> Result<(), TasksError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);
    let engine = Engine::new(&config).map_err(|error| TasksError::Compilation {
        message: error.to_string(),
    })?;
    let module = Module::from_binary(&engine, QUICKJS_RUNTIME).map_err(|error| TasksError::Compilation {
        message: error.to_string(),
    })?;
    execute_bridge_with_runtime(execution, streams, engine, module, abort)
}

/// Stages the module and bootstrap files required by QuickJS-NG's reactor.
///
/// The reactor accepts a file entry point but does not reliably execute a
/// module loaded from an `-e` expression; both staged files live under `/`.
fn execute_bridge_with_runtime(
    execution: &BridgeExecution<'_>,
    streams: GuestStreams,
    engine: Engine,
    module: Module,
    abort: &ScriptAbortHandle,
) -> Result<(), TasksError> {
    let (script_path, bootstrap_path, script_guest_path) = staging_paths(
        execution.workspace_path,
        execution.filename,
        execution.correlation.clone(),
    )?;
    let result = (|| {
        if let Some(parent) = script_path.parent() {
            fs::create_dir_all(parent).map_err(|error| TasksError::Execution {
                message: format!("failed to prepare ScriptNode module directory: {error}"),
            })?;
            let canonical_parent = fs::canonicalize(parent).map_err(|error| TasksError::Execution {
                message: format!("failed to resolve ScriptNode module directory: {error}"),
            })?;
            let canonical_workspace =
                fs::canonicalize(execution.workspace_path).map_err(|error| TasksError::Execution {
                    message: format!("failed to resolve ScriptNode workspace: {error}"),
                })?;
            if !canonical_parent.starts_with(canonical_workspace) {
                return Err(TasksError::Execution {
                    message: String::from("ScriptNode module directory escapes the workspace"),
                });
            }
        }
        let staged_source = format!("{USER_MODULE_ALIASES}{}", execution.compiled);
        fs::write(&script_path, staged_source).map_err(|error| TasksError::Execution {
            message: format!("failed to stage ScriptNode module: {error}"),
        })?;
        let bootstrap = wrapper(execution.correlation.clone(), execution.input, &script_guest_path)?;
        fs::write(&bootstrap_path, bootstrap).map_err(|error| TasksError::Execution {
            message: format!("failed to stage ScriptNode bootstrap: {error}"),
        })?;

        execute_bridge_store(execution, streams, engine, module, abort)
    })();
    let cleanup = [script_path, bootstrap_path]
        .into_iter()
        .filter_map(|path| fs::remove_file(path).err())
        .find(|error| error.kind() != ErrorKind::NotFound);
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Some(error)) => Err(TasksError::Execution {
            message: format!("failed to clean up ScriptNode staging files: {error}"),
        }),
        (Ok(()), None) => Ok(()),
    }
}

fn guest_rpc_relay_error(error: IoError) -> TasksError {
    TasksError::GuestRpcRelay {
        message: error.to_string(),
    }
}

async fn dispatch_guest_rpc(bindings: &GuestRpcBindingsHandle, request: &str) -> Result<String, TasksError> {
    let request: GuestRpcRequest = serde_json::from_str(request).map_err(|error| TasksError::GuestRpcProtocol {
        message: error.to_string(),
    })?;
    let (value, error) = match bindings.call_value(&request.operation, request.input).await {
        Ok(value) => (Some(value), None),
        Err(error) => (None, Some(error)),
    };
    serde_json::to_string(&GuestRpcResponse {
        ok: error.is_none(),
        value,
        error,
    })
    .map_err(|error| TasksError::GuestRpcProtocol {
        message: error.to_string(),
    })
}

async fn relay_guest_rpc_inner(
    requests: DuplexStream,
    responses: &mut DuplexStream,
    bindings: GuestRpcBindingsHandle,
) -> Result<String, TasksError> {
    let mut requests = BufReader::new(requests);
    let mut output = String::new();
    let mut line = String::new();
    while requests
        .read_line(&mut line)
        .await
        .map_err(guest_rpc_relay_error)?
        != 0
    {
        if let Some(request) = line.strip_prefix(GUEST_RPC_PREFIX) {
            let response = dispatch_guest_rpc(&bindings, request.trim_end()).await?;
            responses
                .write_all(response.as_bytes())
                .await
                .map_err(guest_rpc_relay_error)?;
            responses
                .write_all(b"\n")
                .await
                .map_err(guest_rpc_relay_error)?;
            responses.flush().await.map_err(guest_rpc_relay_error)?;
        } else if let Some(completion) = line.strip_prefix(GUEST_OUTPUT_PREFIX) {
            return Ok(completion.trim_end().to_owned());
        } else {
            output.push_str(&line);
        }
        line.clear();
    }
    Ok(output)
}

async fn relay_guest_rpc(
    requests: DuplexStream,
    mut responses: DuplexStream,
    bindings: GuestRpcBindingsHandle,
    timeout: u64,
) -> Result<String, TasksError> {
    tokio::time::timeout(
        Duration::from_secs(timeout),
        relay_guest_rpc_inner(requests, &mut responses, bindings),
    )
    .await
    .map_err(|_| TasksError::Timeout { seconds: timeout })?
}

fn wrapper(
    correlation: LifecycleCorrelation,
    input: &Value,
    user_script_guest_path: &str,
) -> Result<String, TasksError> {
    Ok(format!(
        r#"
globalThis.__sluice_inputs = {input};
globalThis.__sluice_logs = {{ log: [], error: [], warn: [], info: [], debug: [] }};
globalThis.__sluice_outputs = Object.create(null);
globalThis.__sluice_errors = [];

const __fmt = value => typeof value === "string" ? value : JSON.stringify(value);
const __cap = (level, ...args) => globalThis.__sluice_logs[level].push(args.map(__fmt).join(" "));

globalThis.console = {{
  log: (...args) => __cap("log", ...args),
  error: (...args) => __cap("error", ...args),
  warn: (...args) => __cap("warn", ...args),
  info: (...args) => __cap("info", ...args),
  debug: (...args) => __cap("debug", ...args),
}};

globalThis.sluice = Object.freeze({{
  rpc: (operation, input) => {{
    std.out.puts({rpc_prefix} + JSON.stringify({{ operation, input }}) + "\n");
    std.out.flush();
    const line = std.in.getline();
    if (line === null) throw new Error("Sluice RPC bridge closed");
    const response = JSON.parse(line);
    if (!response.ok) throw new Error(response.error || "Sluice RPC failed");
    return response.value;
  }},
}});

await import({user_script});
await Promise.resolve();

std.out.puts({output_prefix} + JSON.stringify({{
  correlation: {correlation},
  outputs: globalThis.__sluice_outputs,
  errors: globalThis.__sluice_errors,
  logs: globalThis.__sluice_logs,
}}) + "\n");

std.out.flush();
"#,
        input = serde_json::to_string(input)?,
        correlation = serde_json::to_string(&correlation)?,
        rpc_prefix = serde_json::to_string(GUEST_RPC_PREFIX)?,
        output_prefix = serde_json::to_string(GUEST_OUTPUT_PREFIX)?,
        user_script = serde_json::to_string(user_script_guest_path)?,
    ))
}

fn initialize(
    store: &mut Store<State>,
    instance: &Instance,
    timed: &Arc<AtomicBool>,
    timeout: u64,
) -> Result<(), TasksError> {
    for name in ["wizer.initialize", "_initialize"] {
        if let Some(function) = instance.get_func(&mut *store, name) {
            function
                .typed::<(), ()>(&mut *store)?
                .call(&mut *store, ())
                .map_err(|error| trap(error, timed, timeout))?;
        }
    }
    Ok(())
}

fn bind(store: &mut Store<State>, instance: &Instance) -> Result<Exports, TasksError> {
    Ok(Exports {
        memory: instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| TasksError::MissingExport(String::from("memory")))?,
        malloc: instance.get_typed_func(&mut *store, "malloc")?,
        free: instance.get_typed_func(&mut *store, "free")?,
        init: instance.get_typed_func(&mut *store, "qjs_init_argv")?,
        context: instance.get_typed_func(&mut *store, "qjs_get_context")?,
        loop_once: instance.get_typed_func(&mut *store, "js_std_loop_once")?,
        destroy: instance.get_typed_func(&mut *store, "qjs_destroy")?,
    })
}

fn execute(
    store: &mut Store<State>,
    e: &Exports,
    correlation: LifecycleCorrelation,
    timeout: u64,
    timed: &Arc<AtomicBool>,
) -> Result<(), TasksError> {
    let (_, bootstrap_filename) = staging_filenames(correlation);
    let bootstrap_guest_path = format!("/{bootstrap_filename}");
    let (argv, arguments) = argv(store, e, ["qjs", "--std", &bootstrap_guest_path])?;
    let status = match e
        .init
        .call(&mut *store, (i32::try_from(arguments.len())?, argv.ptr))
    {
        Ok(status) => status,
        Err(error) => return Err(trap(error, timed, timeout)),
    };
    for argument in arguments {
        e.free.call(&mut *store, argument.ptr)?;
    }
    e.free.call(&mut *store, argv.ptr)?;
    if status != 0 {
        let stderr = store.data().stderr.contents();
        let stderr = String::from_utf8_lossy(&stderr);
        let stderr = stderr.trim();
        return Err(TasksError::Execution {
            message: if stderr.is_empty() {
                format!("qjs_init_argv returned {status}")
            } else {
                format!("QuickJS initialization failed: {stderr}")
            },
        });
    }
    drain(store, e, timeout, timed)?;
    Ok(())
}

/// Drains the reactor-owned module loader and job queue before output is read.
fn drain(store: &mut Store<State>, exports: &Exports, timeout: u64, timed: &Arc<AtomicBool>) -> Result<(), TasksError> {
    loop {
        let context = exports
            .context
            .call(&mut *store, ())
            .map_err(|error| trap(error, timed, timeout))?;
        let result = exports
            .loop_once
            .call(&mut *store, (context,))
            .map_err(|error| trap(error, timed, timeout))?;
        match result {
            -1 => return Ok(()),
            -2 => {
                return Err(TasksError::Execution {
                    message: String::from("QuickJS pending job failed"),
                });
            }
            _ => continue,
        }
    }
}

fn bytes(store: &mut Store<State>, e: &Exports, value: &[u8]) -> Result<Region, TasksError> {
    let ptr = e.malloc.call(&mut *store, i32::try_from(value.len())?)?;
    e.memory.write(&mut *store, usize::try_from(ptr)?, value)?;
    Ok(Region { ptr })
}

fn argv(store: &mut Store<State>, e: &Exports, values: [&str; 3]) -> Result<(Region, [Region; 3]), TasksError> {
    let arguments = values.map(|value| {
        if value.as_bytes().contains(&0) {
            return Err(TasksError::Execution {
                message: String::from("QuickJS argument contains an embedded NUL byte"),
            });
        }
        let mut nul_terminated = Vec::with_capacity(value.len() + 1);
        nul_terminated.extend_from_slice(value.as_bytes());
        nul_terminated.push(0);
        bytes(store, e, &nul_terminated)
    });
    let [first, second, third] = arguments;
    let arguments = [first?, second?, third?];
    let mut pointers = [0_u8; 16];
    pointers[0..4].copy_from_slice(&arguments[0].ptr.to_le_bytes());
    pointers[4..8].copy_from_slice(&arguments[1].ptr.to_le_bytes());
    pointers[8..12].copy_from_slice(&arguments[2].ptr.to_le_bytes());
    Ok((bytes(store, e, &pointers)?, arguments))
}

fn trap(error: WasmtimeError, timed: &AtomicBool, timeout: u64) -> TasksError {
    if timed.load(Ordering::SeqCst)
        || error
            .downcast_ref::<Trap>()
            .is_some_and(|trap| matches!(trap, Trap::Interrupt))
    {
        TasksError::Timeout { seconds: timeout }
    } else if error
        .downcast_ref::<Trap>()
        .is_some_and(|trap| matches!(trap, Trap::OutOfFuel))
        || error.to_string().contains("fuel")
    {
        TasksError::FuelExhausted
    } else {
        TasksError::Execution {
            message: error.to_string(),
        }
    }
}

fn parse_output(value: &str) -> Result<ScriptNodeCompletion, TasksError> {
    Ok(serde_json::from_str(value.trim())?)
}

impl TimeoutGuard {
    fn finish(mut self) -> Result<(), TasksError> {
        let _ = self.cancel.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| TasksError::Execution {
                message: String::from("timeout thread panicked"),
            })?;
        }
        Ok(())
    }
}

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        let _ = self.cancel.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn arm_timeout(engine: &Engine, store: &mut Store<State>, seconds: u64, timed: Arc<AtomicBool>) -> TimeoutGuard {
    debug_assert!(seconds > 0);
    store.set_epoch_deadline(1);
    let (cancel, rx) = mpsc::channel();
    let engine = engine.clone();
    let thread = thread::spawn(move || {
        if rx.recv_timeout(Duration::from_secs(seconds)).is_err() {
            timed.store(true, Ordering::SeqCst);
            engine.increment_epoch();
        }
    });
    TimeoutGuard {
        cancel,
        thread: Some(thread),
    }
}

#[cfg(test)]
struct TestRuntime {
    lock: Mutex<()>,
    engine: Engine,
    module: Module,
}

#[cfg(test)]
fn test_runtime() -> &'static TestRuntime {
    static RUNTIME: std::sync::OnceLock<TestRuntime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("QuickJS test engine must initialize");
        let module = Module::from_binary(&engine, QUICKJS_RUNTIME).expect("QuickJS test module must compile");
        TestRuntime {
            lock: std::sync::Mutex::new(()),
            engine,
            module,
        }
    })
}

#[cfg(feature = "wasm-tests")]
#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::flow::{FlowExecutionId, FlowId, FlowNodeId, NodeExecutionId, NodeKind};

    use super::*;

    fn correlation() -> LifecycleCorrelation {
        LifecycleCorrelation {
            node_kind: NodeKind::parse("ScriptNode").expect("static node kind is valid"),
            flow_id: FlowId(Uuid::new_v4()),
            flow_node_id: FlowNodeId(Uuid::new_v4()),
            flow_execution_id: FlowExecutionId(Uuid::new_v4()),
            node_execution_id: NodeExecutionId(Uuid::new_v4()),
        }
    }

    fn execution(
        correlation: LifecycleCorrelation,
        compiled: &str,
        fuel: u64,
        timeout: u64,
        workspace_path: PathBuf,
    ) -> ScriptExecution {
        ScriptExecution::new(
            correlation,
            CompiledScript::new(compiled.to_owned(), "script.ts".to_owned()),
            serde_json::json!({}),
            fuel,
            timeout,
            workspace_path,
            super::super::client::GuestRpcBindingsHandle::fixed_test_response(serde_json::json!(null)),
        )
    }

    #[test]
    fn maps_guest_budget_and_timeout_failures_to_typed_errors() {
        let timed = AtomicBool::new(false);
        assert!(matches!(
            trap(WasmtimeError::msg("all fuel consumed"), &timed, 4),
            TasksError::FuelExhausted
        ));
        timed.store(true, Ordering::SeqCst);
        assert!(matches!(
            trap(WasmtimeError::msg("guest stopped"), &timed, 4),
            TasksError::Timeout { seconds: 4 }
        ));
    }

    #[test]
    fn rejects_invalid_script_node_completion() {
        assert!(matches!(
            parse_output("not json"),
            Err(TasksError::InvalidOutput { .. })
        ));
    }

    #[test]
    fn wrapper_emits_the_generic_script_node_completion_shape() {
        let correlation = correlation();
        let (script_filename, _) = staging_filenames(correlation.clone());
        let rendered = wrapper(correlation, &serde_json::json!({}), &format!("/{script_filename}")).unwrap();

        assert!(rendered.contains("await Promise.resolve()"));
        assert!(rendered.contains(&format!("await import(\"/{script_filename}\")")));
        assert!(!rendered.contains("(async () =>"));
        assert!(!rendered.contains("try {"));
        assert!(rendered.contains("outputs"));
        assert!(rendered.contains("errors"));
        assert!(rendered.contains("logs"));
        assert!(rendered.contains("__sluice_inputs"));
        assert!(rendered.contains(r#"\u001eSLUICE_OUTPUT "#));
        assert!(!rendered.contains("decisions"));
        assert!(!rendered.contains("global_errors"));
        assert!(!rendered.contains("triggered_at"));
    }

    #[tokio::test]
    async fn bundled_guest_preserves_multiple_named_output_values() {
        let correlation = correlation();
        let workspace = tempdir().unwrap();
        let workspace_path = workspace.path().to_path_buf();
        let completion = tokio::task::spawn_blocking({
            let correlation = correlation.clone();
            move || {
                ScriptSandbox::new().unwrap().run(
                    execution(
                        correlation,
                        r#"__sluice_outputs.summary = "complete"; __sluice_outputs.metrics = { count: 2, values: [true, null] }; console.log("kept separate");"#,
                        100_000_000,
                        30,
                        workspace_path,
                    ),
                    ScriptAbortHandle::new(),
                )
            }
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(completion.correlation, correlation);
        assert_eq!(completion.outputs.get("summary"), Some(&serde_json::json!("complete")));
        assert_eq!(
            completion.outputs.get("metrics"),
            Some(&serde_json::json!({"count": 2, "values": [true, null]}))
        );
        assert_eq!(completion.logs["log"][0], "kept separate");
    }

    #[test]
    fn bundled_quickjs_uses_the_argv_reactor_entry() {
        let exports = test_runtime()
            .module
            .exports()
            .map(|export| export.name().to_owned())
            .collect::<Vec<_>>();

        for required in [
            "memory",
            "malloc",
            "free",
            "qjs_init_argv",
            "qjs_get_context",
            "js_std_loop_once",
            "qjs_destroy",
        ] {
            assert!(
                exports.iter().any(|export| export == required),
                "missing bundled QuickJS export {required}"
            );
        }
        assert!(!exports.iter().any(|export| export == "qjs_eval"));
    }

    #[tokio::test]
    async fn javascript_guest_rpc_round_trips_through_live_sandbox_stdio_bridge() {
        let bindings =
            super::super::client::GuestRpcBindingsHandle::fixed_test_response(serde_json::json!({ "answer": 42 }));
        let workspace = tempdir().unwrap();
        let workspace_path = workspace.path().to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            ScriptSandbox::new().unwrap().run(
                ScriptExecution::new(
                    correlation(),
                    CompiledScript::new(
                        "const response = sluice.rpc('test.read', { value: 7 }); if (response.answer !== 42) throw new Error('wrong response'); console.log(response.answer);".to_owned(),
                        "script.ts".to_owned(),
                    ),
                    serde_json::json!({}),
                    100_000_000,
                    30,
                    workspace_path,
                    bindings,
                ),
                ScriptAbortHandle::new(),
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert!(result.errors.is_empty());
        assert_eq!(result.logs["log"][0], "42");
    }

    #[tokio::test]
    async fn guest_filesystem_is_scoped_to_the_execution_workspace() {
        let workspace = tempdir().unwrap();
        let outside_name = format!("flow-outside-{}.txt", Uuid::new_v4());
        let outside = workspace.path().parent().unwrap().join(&outside_name);
        let root = workspace.path().to_path_buf();
        let completion = tokio::task::spawn_blocking(move || {
            ScriptSandbox::new().unwrap().run(
                ScriptExecution::new(
                    correlation(),
                    CompiledScript::new(
                        format!(r#"const file = std.open("inside.txt", "w"); file.puts("workspace"); file.close(); const escaped_error = {{}}; const escaped = std.open("../{outside_name}", "w", escaped_error); if (escaped !== null) {{ escaped.puts("escape"); escaped.close(); }} else {{ __sluice_errors.push(`workspace escape rejected (${{escaped_error.errno}})`); }}"#).to_owned(),
                        "script.ts".to_owned(),
                    ),
                    serde_json::json!({}),
                    100_000_000,
                    30,
                    root,
                    super::super::client::GuestRpcBindingsHandle::fixed_test_response(serde_json::json!(null)),
                ),
                ScriptAbortHandle::new(),
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(workspace.path().join("inside.txt")).unwrap(),
            "workspace"
        );
        assert!(!outside.exists());
        assert!(!completion.errors.is_empty());
    }

    #[tokio::test]
    async fn module_guest_resolves_workspace_imports_and_drains_promise_jobs() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir(workspace.path().join("src")).unwrap();
        std::fs::write(
            workspace.path().join("src/helper.js"),
            "export const value = 'from workspace';",
        )
        .unwrap();
        let root = workspace.path().to_path_buf();
        let completion = tokio::task::spawn_blocking(move || {
            ScriptSandbox::new().unwrap().run(
                ScriptExecution::new(
                    correlation(),
                    CompiledScript::new(
                        r#"import { value } from './helper.js'; __sluice_outputs.imported = value; Promise.resolve().then(() => { __sluice_outputs.async = 'first'; }).then(() => { __sluice_outputs.async = 'drained'; });"#.to_owned(),
                        "src/main.ts".to_owned(),
                    ),
                    serde_json::json!({}),
                    100_000_000,
                    30,
                    root,
                    super::super::client::GuestRpcBindingsHandle::fixed_test_response(serde_json::json!(null)),
                ),
                ScriptAbortHandle::new(),
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            completion.outputs.get("imported"),
            Some(&serde_json::json!("from workspace"))
        );
        assert_eq!(completion.outputs.get("async"), Some(&serde_json::json!("drained")));
        let (script_filename, bootstrap_filename) = staging_filenames(completion.correlation);
        assert!(!workspace.path().join("src").join(script_filename).exists());
        assert!(!workspace.path().join(bootstrap_filename).exists());
    }
}
