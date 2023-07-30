use crate::config::Config;

pub struct FeatureFlags {
    pub enable_grand_total_query: bool,
}

pub fn get_feature_flags(config: &Config) -> FeatureFlags {
    FeatureFlags {
        enable_grand_total_query: config.enable_grand_total_query.unwrap_or(false),
    }
}
