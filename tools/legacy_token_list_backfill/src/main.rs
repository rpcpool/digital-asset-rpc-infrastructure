use {
    anyhow::{anyhow, Context, Result},
    clap::Parser,
    serde::Deserialize,
    sqlx::postgres::PgPoolOptions,
};

// Frozen registry snapshot of solana-labs/token-list. Bundled at compile time
// so the tool runs offline; refresh by replacing the file and rebuilding.
const TOKEN_LIST_JSON: &str = include_str!("../solana.tokenlist.json");

#[derive(Parser)]
#[command(about = "Backfill asset_data with legacy token-list metadata for fungibles with empty on-chain URIs.")]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Roll back instead of committing.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Deserialize)]
struct TokenList {
    tokens: Vec<Token>,
}

#[derive(Deserialize)]
struct Token {
    address: String,
    name: String,
    symbol: String,
    #[serde(rename = "chainId")]
    chain_id: u32,
    #[serde(rename = "logoURI", default)]
    logo_uri: Option<String>,
}

const SOLANA_MAINNET_CHAIN_ID: u32 = 101;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let list: TokenList = serde_json::from_str(TOKEN_LIST_JSON)
        .context("parsing bundled solana.tokenlist.json")?;

    let mut mints = Vec::new();
    let mut names = Vec::new();
    let mut symbols = Vec::new();
    let mut uris = Vec::new();
    let mut mimes = Vec::new();
    for t in &list.tokens {
        if t.chain_id != SOLANA_MAINNET_CHAIN_ID {
            continue;
        }
        let uri = t.logo_uri.as_deref().unwrap_or("").trim();
        if uri.is_empty() {
            continue;
        }
        let mint = bs58::decode(&t.address).into_vec()
            .map_err(|e| anyhow!("bad mint base58 {}: {}", t.address, e))?;
        if mint.len() != 32 {
            continue;
        }
        mints.push(mint);
        names.push(t.name.clone());
        symbols.push(t.symbol.clone());
        mimes.push(guess_mime(uri));
        uris.push(uri.to_string());
    }
    println!("loaded {} legacy tokens from bundled list", mints.len());

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&args.database_url)
        .await
        .context("connecting to postgres")?;

    let mut tx = pool.begin().await?;

    sqlx::query(
        "CREATE TEMP TABLE legacy_tokens (
            mint     bytea PRIMARY KEY,
            name     text NOT NULL,
            symbol   text NOT NULL,
            logo_uri text NOT NULL,
            mime     text NOT NULL
        ) ON COMMIT DROP",
    )
    .execute(&mut tx)
    .await?;

    sqlx::query(
        "INSERT INTO legacy_tokens (mint, name, symbol, logo_uri, mime)
         SELECT * FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::text[], $5::text[])",
    )
    .bind(&mints)
    .bind(&names)
    .bind(&symbols)
    .bind(&uris)
    .bind(&mimes)
    .execute(&mut tx)
    .await?;

    let result = sqlx::query(
        "UPDATE asset_data ad
         SET
             metadata_url = lt.logo_uri,
             metadata = jsonb_build_object(
                 'name',   lt.name,
                 'symbol', lt.symbol,
                 'image',  lt.logo_uri,
                 'properties', jsonb_build_object(
                     'files', jsonb_build_array(
                         jsonb_build_object('uri', lt.logo_uri, 'type', lt.mime)
                     ),
                     'category', 'image'
                 )
             ),
             metadata_mutability = 'immutable',
             reindex = false
         FROM legacy_tokens lt
         WHERE ad.id = lt.mint AND ad.metadata_url = ''",
    )
    .execute(&mut tx)
    .await?;

    println!("enriched {} asset_data rows", result.rows_affected());

    if args.dry_run {
        tx.rollback().await?;
        println!("dry-run: rolled back");
    } else {
        tx.commit().await?;
        println!("committed");
    }
    Ok(())
}

fn guess_mime(uri: &str) -> String {
    let u = uri.to_lowercase();
    if u.ends_with(".svg") {
        "image/svg+xml".into()
    } else if u.ends_with(".jpg") || u.ends_with(".jpeg") {
        "image/jpeg".into()
    } else if u.ends_with(".gif") {
        "image/gif".into()
    } else if u.ends_with(".webp") {
        "image/webp".into()
    } else {
        "image/png".into()
    }
}
