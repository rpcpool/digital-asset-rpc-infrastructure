use crate::dao::GroupingSize;
use sea_orm::{ConnectionTrait, DbErr, Statement};

pub async fn get_grouping(
    conn: &impl ConnectionTrait,
    group_key: String,
    group_value: String,
) -> Result<GroupingSize, DbErr> {
    // MIP-11 (#270): for keys that use the multi-row NULL-sentinel pattern
    // (e.g. "group"), an asset can have both a stale value row and a newer
    // NULL sentinel. Count only rows whose slot_updated matches the max for
    // that (asset_id, group_key) pair so stale rows aren't double-counted.
    let result = conn
        .query_one(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"SELECT COUNT(*) AS cnt FROM asset_grouping ag
               WHERE ag.group_key = $1
               AND ag.group_value = $2
               AND (ag.verified = true OR ag.verified IS NULL)
               AND ag.slot_updated = (
                   SELECT MAX(ag2.slot_updated)
                   FROM asset_grouping ag2
                   WHERE ag2.asset_id = ag.asset_id
                   AND ag2.group_key = $1
               )"#,
            vec![group_key.into(), group_value.into()],
        ))
        .await?;

    let size = result
        .map(|r| r.try_get::<i64>("", "cnt").unwrap_or(0) as u64)
        .unwrap_or(0);

    Ok(GroupingSize { size })
}
