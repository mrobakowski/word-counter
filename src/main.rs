#![feature(let_else)]

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    path::PathBuf,
    process::Stdio,
};

use axum::{routing::get, Json, Router, Server};
use clap::Parser;
use inlinable_string::InlinableString as IString;
use serde::Deserialize;
use tokio::{
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
    process::Command,
    sync::watch,
};
use tower_http::trace::TraceLayer;
use tracing::{info, trace, warn};

#[derive(Deserialize)]
struct IncomingProcessData<'s> {
    event_type: &'s str,
    data: &'s str,
}

struct OnDrop<F: FnMut() -> ()>(F);
impl<F: FnMut() -> ()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

fn on_drop(f: impl FnMut()) -> OnDrop<impl FnMut()> {
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

type Response = HashMap<IString, HashMap<IString, u64>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Args {
        exe_path,
        window_size,
    } = Args::parse();

    tracing_subscriber::fmt::init();

    let (tx, rx) = watch::channel(HashMap::new());

    let process_listener_loop = tokio::spawn(process_loop(exe_path, window_size, tx));

    let app = Router::new()
        .route(
            "/",
            get(|| async move {
                // we could get rid of this .clone() by doing serialization in place instead of relying on the web framework
                Json(rx.borrow().clone())
            }),
        )
        .layer(TraceLayer::new_for_http()); // more pretty lights when you run with `RUST_LOG=trace`

    let server_loop = tokio::spawn(async move {
        let _guard = on_drop(|| info!("server loop ended"));

        info!("The server is running at http://localhost:3000/");

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

async fn process_loop(exe_path: Option<PathBuf>, window_size: usize, tx: watch::Sender<Response>) {
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

            process_loop_inner(process_stdout, window_size, tx).await
        }

        None => {
            info!("Exe not found, processing STDIN");
            process_loop_inner(stdin(), window_size, tx).await
        }
    }
}

async fn process_loop_inner(
    line_source: impl AsyncRead + Unpin,
    window_size: usize,
    tx: watch::Sender<Response>,
) {
    let mut line_source = BufReader::new(line_source).lines();
    let mut valid_message_window = VecDeque::with_capacity(window_size);

    while let Some(line) = line_source.next_line().await.transpose() {
        let Ok(ref line) = line.map_err(|e| warn!("io error when reading line: {e}")) else { continue };
        trace!(line, "got line from the process");

        let Ok(deserialized) = serde_json::from_str(line)
            .map_err(|e| warn!("deserialization error: {e}")) else { continue };

        // NOTE: the order and -1 make sure that we never go over the capacity so no reallocation
        valid_message_window.truncate(window_size - 1);
        valid_message_window.push_front(count_words(deserialized));

        match tx.send(sum_counts(&valid_message_window)) {
            Ok(_) => (),
            Err(_) => break, // channel closed
        }
    }
}

// the data from the example processes provided is usually short words, so using strings optimized for short lengths makes sense here

fn count_words(msg: IncomingProcessData) -> (IString, HashMap<IString, u64>) {
    let mut counts = HashMap::new();

    for word in msg.data.split_whitespace() {
        *counts.entry(IString::from(word)).or_default() += 1;
    }

    (IString::from(msg.event_type), counts)
}

fn sum_counts<'c>(
    data: impl IntoIterator<Item = &'c (IString, HashMap<IString, u64>)>,
) -> Response {
    let mut res = HashMap::new();

    for (event_type, counts) in data {
        let per_event_data: &mut HashMap<IString, u64> = res.entry(event_type.clone()).or_default();
        for (word, count) in counts {
            *per_event_data.entry(word.clone()).or_default() += *count;
        }
    }

    res
}
