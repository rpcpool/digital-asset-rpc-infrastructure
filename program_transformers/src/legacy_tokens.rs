use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::OnceLock;

pub struct LegacyEntry {
    pub uri: String,
    pub metadata: Value,
}

static MAP: OnceLock<HashMap<[u8; 32], LegacyEntry>> = OnceLock::new();

const TOKEN_LIST: &str =
    include_str!("../../tools/legacy_token_list_backfill/solana.tokenlist.json");

pub fn get(mint: &[u8; 32]) -> Option<&'static LegacyEntry> {
    MAP.get_or_init(build).get(mint)
}

fn build() -> HashMap<[u8; 32], LegacyEntry> {
    let mut out = HashMap::new();
    let Ok(root): Result<Value, _> = serde_json::from_str(TOKEN_LIST) else {
        return out;
    };
    let Some(tokens) = root.get("tokens").and_then(Value::as_array) else {
        return out;
    };
    for t in tokens {
        if t.get("chainId").and_then(Value::as_u64) != Some(101) {
            continue;
        }
        let addr = t.get("address").and_then(Value::as_str).unwrap_or("");
        let logo = t.get("logoURI").and_then(Value::as_str).unwrap_or("");
        if addr.is_empty() || logo.is_empty() {
            continue;
        }
        let Ok(pk) = Pubkey::from_str(addr) else {
            continue;
        };
        let name = t.get("name").and_then(Value::as_str).unwrap_or("");
        let symbol = t.get("symbol").and_then(Value::as_str).unwrap_or("");
        let mime = mime_of(logo);
        let metadata = json!({
            "name": name,
            "symbol": symbol,
            "image": logo,
            "properties": {
                "files": [{ "uri": logo, "type": mime }],
                "category": "image",
            },
        });
        out.insert(
            pk.to_bytes(),
            LegacyEntry {
                uri: logo.to_string(),
                metadata,
            },
        );
    }
    out
}

fn mime_of(uri: &str) -> &'static str {
    let l = uri.to_lowercase();
    if l.ends_with(".png") {
        "image/png"
    } else if l.ends_with(".jpg") || l.ends_with(".jpeg") {
        "image/jpeg"
    } else if l.ends_with(".gif") {
        "image/gif"
    } else if l.ends_with(".svg") {
        "image/svg+xml"
    } else if l.ends_with(".webp") {
        "image/webp"
    } else {
        "image/png"
    }
}
