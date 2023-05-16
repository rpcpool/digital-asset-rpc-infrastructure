mod utils;

use clap::Parser;
use redis::{
    aio::{Connection, ConnectionLike, ConnectionManager},
    cmd,
    streams::{StreamPendingCountReply, StreamPendingReply, StreamRangeReply},
    Commands, RedisError,
};
#[derive(Parser)]
#[command(next_line_help = true)]
struct Cli {
    #[arg(long)]
    redis_url: String,
    #[arg(long)]
    stream: String,
}

const CONSUMER_GROUP_NAME: &str = "plerkle";
const CONSUMER_ID: &str = "pelhelper";

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let redis_uri = cli.redis_url;
    let stream = cli.stream;
    let mut client = redis::Client::open(redis_uri).unwrap();

    // Get connection.
    let mut connection = client.get_tokio_connection_manager().await.unwrap();

    let info: StreamPendingReply = client.xpending(&stream, CONSUMER_GROUP_NAME).unwrap();
    println!("PEL has {} items", info.count());
    if info.count() == 0 {
        println!("PEL is empty!");
        return;
    }

    let batch_size = 100;
    let res: StreamPendingCountReply = client
        .xpending_count(&stream, CONSUMER_GROUP_NAME, "-", "+", batch_size)
        .unwrap();

    let mut ids: Vec<String> = Vec::new();
    for sid in res.ids {
        // TODO: Analyze these further.
        println!(
            "Processing id: {} (delivered: {})",
            sid.id, sid.times_delivered
        );
        ids.push(sid.id)
    }

    ack_msgs(&mut connection, &stream, &ids).await;
    println!("Acked all messages");
}

async fn ack_msgs(connection: &mut ConnectionManager, stream_key: &str, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    let mut pipe = redis::pipe();
    pipe.xack(stream_key, CONSUMER_GROUP_NAME, ids);
    pipe.xdel(stream_key, ids);

    let _: Result<(), RedisError> = pipe.query_async(connection).await;
}
