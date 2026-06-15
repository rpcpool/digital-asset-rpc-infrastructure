//! Command-style integration test that fires the same JSON-RPC requests at
//! two DAS API endpoints and reports semantic differences. Invoke with:
//!
//!   cargo test --test compare_endpoints --release -- \
//!       --rpc1 http://endpoint-a:9090 \
//!       --rpc2 http://endpoint-b:9090 \
//!       [--only <substr>]   [--timeout <secs>]
//!
//! Request fixtures live in `tests/compare_endpoints/requests/*.json`; each
//! file is an array of `{ "label": ..., "method": ..., "params": ... }`.
//! Mismatches are dumped to `compare_endpoints_mismatches.json` at the crate
//! root. Process exits non-zero if any mismatch is found.

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const REQUESTS_DIR: &str = "tests/compare_endpoints/requests";
const MISMATCH_FILE: &str = "compare_endpoints_mismatches.json";

struct Args {
    rpc1: String,
    rpc2: String,
    only: Option<String>,
    timeout: u64,
}

fn parse_args() -> Result<Args> {
    let mut rpc1: Option<String> = None;
    let mut rpc2: Option<String> = None;
    let mut only: Option<String> = None;
    let mut timeout: u64 = 30;

    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--rpc1" => rpc1 = it.next(),
            "--rpc2" => rpc2 = it.next(),
            "--only" => only = it.next(),
            "--timeout" => {
                timeout = it
                    .next()
                    .ok_or_else(|| anyhow!("--timeout requires a number"))?
                    .parse()
                    .context("--timeout must be u64 seconds")?;
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: cargo test --test compare_endpoints --release -- \\\n\
                     \t--rpc1 <url> --rpc2 <url> [--only <substr>] [--timeout <secs>]"
                );
                std::process::exit(0);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    Ok(Args {
        rpc1: rpc1.ok_or_else(|| anyhow!("--rpc1 is required"))?,
        rpc2: rpc2.ok_or_else(|| anyhow!("--rpc2 is required"))?,
        only,
        timeout,
    })
}

fn main() -> Result<()> {
    let args = parse_args()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let files = load_request_files(REQUESTS_DIR)?;
    if files.is_empty() {
        return Err(anyhow!("no fixtures found in {REQUESTS_DIR}"));
    }

    let mut total = 0usize;
    let mut errors = 0usize;
    let mut mismatches: Vec<Value> = Vec::new();

    for path in &files {
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("(unknown)")
            .to_string();
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let cases: Vec<Value> =
            serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;

        for case in cases {
            let label = case
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("(unlabeled)")
                .to_string();

            if let Some(filter) = &args.only {
                if !file_name.contains(filter) && !label.contains(filter) {
                    continue;
                }
            }

            total += 1;
            let body = build_jsonrpc(&case)?;

            let (r1, r2) = tokio::join!(
                call(&client, &args.rpc1, &body),
                call(&client, &args.rpc2, &body),
            );

            match (r1, r2) {
                (Ok((a, da)), Ok((b, db))) => {
                    let na = normalize(&a);
                    let nb = normalize(&b);
                    let a_err = a.get("error").is_some();
                    let b_err = b.get("error").is_some();
                    if (a_err && b_err) || na == nb {
                        println!("{file_name}::{label}  OK ({da}ms vs {db}ms)");
                    } else {
                        println!("{file_name}::{label}  MISMATCH ({da}ms vs {db}ms)");
                        mismatches.push(json!({
                            "file": file_name,
                            "label": label,
                            "request": body,
                            "rpc1": a,
                            "rpc2": b,
                        }));
                    }
                }
                (Err(e), _) => {
                    errors += 1;
                    println!("{file_name}::{label}  ERR rpc1: {e:#}");
                }
                (_, Err(e)) => {
                    errors += 1;
                    println!("{file_name}::{label}  ERR rpc2: {e:#}");
                }
            }
        }
    }

    println!(
        "\n===== {total} requests, {} mismatches, {errors} transport errors =====",
        mismatches.len()
    );

    if !mismatches.is_empty() {
        let dump = serde_json::to_string_pretty(&Value::Array(mismatches.clone()))?;
        std::fs::write(MISMATCH_FILE, dump).with_context(|| format!("write {MISMATCH_FILE}"))?;
        println!("wrote {MISMATCH_FILE} ({} entries)", mismatches.len());
        return Err(anyhow!("{} mismatches", mismatches.len()));
    }
    if errors > 0 {
        return Err(anyhow!("{errors} transport errors"));
    }
    Ok(())
}

fn build_jsonrpc(case: &Value) -> Result<Value> {
    let method = case
        .get("method")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("case missing `method` field"))?;
    let params = case.get("params").cloned().unwrap_or(Value::Null);
    Ok(json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }))
}

async fn call(client: &reqwest::Client, url: &str, body: &Value) -> Result<(Value, u128)> {
    let start = Instant::now();
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("post to {url}"))?;
    let json: Value = resp
        .json()
        .await
        .with_context(|| format!("decode body from {url}"))?;
    Ok((json, start.elapsed().as_millis()))
}

/// Recursively sort arrays-of-objects by an id-shaped field so order doesn't
/// produce false mismatches. Strict equality on everything else.
fn normalize(v: &Value) -> Value {
    let mut v = v.clone();
    walk(&mut v);
    v
}

fn walk(v: &mut Value) {
    match v {
        Value::Array(arr) => {
            if arr.iter().all(Value::is_object) {
                arr.sort_by_key(sort_key);
            }
            for child in arr.iter_mut() {
                walk(child);
            }
        }
        Value::Object(map) => {
            // Strip null/empty-array/empty-object/empty-string entries so that
            // an absent field and an explicitly-empty field compare equal —
            // DAS implementations differ cosmetically on serializing empties.
            map.retain(|_, val| !is_empty(val));
            for (_, child) in map.iter_mut() {
                walk(child);
            }
        }
        _ => {}
    }
}

fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

fn sort_key(v: &Value) -> String {
    for k in &["id", "address", "pubkey", "signature", "mint"] {
        if let Some(s) = v.get(*k).and_then(Value::as_str) {
            return s.to_string();
        }
    }
    String::new()
}

fn load_request_files(dir: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {dir}"))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}
