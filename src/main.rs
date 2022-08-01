#![feature(let_else)]

use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    error::Error,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
};

use axum::{routing::get, Json, Router, Server};
use clap::Parser;
use dashmap::DashMap;
use serde::Deserialize;
use tokio::{
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
    process::Command,
};
use tower_http::trace::TraceLayer;
use tracing::{info, trace, warn};

#[derive(Deserialize)]
struct IncomingProcessData<'s> {
    event_type: &'s str,
    data: &'s str,
}

fn on_drop(f: impl FnMut() + 'static) -> impl Any {
    struct OnDrop<F: FnMut() -> ()>(F);
    impl<F: FnMut() -> ()> Drop for OnDrop<F> {
        fn drop(&mut self) {
            (self.0)()
        }
    }

    OnDrop(f)
}

/// A word counter
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the executable to run and process the output. Processes STDIN if absent.
    exe_path: Option<PathBuf>,

    /// Window size to use for word counting
    #[clap(short, long, default_value_t = 10)]
    window_size: usize,
}

type SharedState = Arc<DashMap<String, VecDeque<u64>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args {
        exe_path,
        window_size,
    } = Args::parse();

    tracing_subscriber::fmt::init();

    // DashMap is a concurrent hash map, probably a bit of a premature optimization
    let shared_state = Arc::new(DashMap::<String, VecDeque<u64>>::new());

    let process_listener_loop = tokio::spawn(process_loop(
        exe_path,
        window_size,
        Arc::clone(&shared_state),
    ));

    let app = Router::new()
        .route(
            "/count",
            get(|| async move {
                Json(
                    shared_state
                        .iter()
                        .map(|r| (r.key().clone(), r.value().iter().sum()))
                        .collect::<BTreeMap<_, u64>>(),
                )
            }),
        )
        .layer(TraceLayer::new_for_http());

    let server_loop = tokio::spawn(async move {
        let _guard = on_drop(|| info!("server loop ended"));

        info!("The server is running at http://localhost:3000/count");

        Server::bind(&"0.0.0.0:3000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    tokio::signal::ctrl_c()
        .await
        .expect("Couldn't await Ctrl-C signal");

    process_listener_loop.abort();
    server_loop.abort();

    Ok(())
}

async fn process_loop(exe_path: Option<PathBuf>, window_size: usize, shared_state: SharedState) {
    let _guard = on_drop(|| info!("process loop ended"));

    match exe_path {
        Some(path) => {
            let path_str: &str = &path.to_string_lossy();
            info!(path = path_str, "Starting child process");

            let mut process = Command::new(path)
                .stdout(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .expect("failed to spawn the child");

            let process_stdout = process
                .stdout
                .take()
                .expect("child did not have a handle to stdout");

            process_loop_inner(process_stdout, window_size, shared_state).await
        }

        None => {
            info!("Exe not found, processing STDIN");
            process_loop_inner(stdin(), window_size, shared_state).await
        }
    }
}

async fn process_loop_inner(
    line_source: impl AsyncRead + Unpin,
    window_size: usize,
    shared_state: SharedState,
) {
    let mut blackbox_lines = BufReader::new(line_source).lines();

    while let Some(line) = blackbox_lines.next_line().await.transpose() {
        let Ok(ref line) = line.map_err(|e| warn!("io error when reading line: {e}")) else { continue };
        trace!(line, "got line from the process");

        let Ok(deserialized) = serde_json::from_str::<IncomingProcessData>(line)
            .map_err(|e| warn!("deserialization error: {e}")) else { continue };

        let word_count = deserialized.data.split_whitespace().count() as u64;

        let mut window_buffer = shared_state
            .entry(deserialized.event_type.into())
            .or_insert_with(|| VecDeque::with_capacity(window_size));

        // NOTE: the order and -1 makes sure that we never go over the capacity so no reallocation
        window_buffer.truncate(window_size - 1);
        window_buffer.push_front(word_count);
    }
}
