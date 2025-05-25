use dashmap::DashMap;
use ethers::contract::abigen;
use ethers::prelude::*;
use ethers::types::Address;
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, fs};
use tokio::time::sleep;


abigen!(
    ERC20,
    r#"[{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
    {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]"#
);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub symbol: String,
    pub decimals: u8,
}

pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;

const TOKEN_CACHE_JSON_PATH: &str = "token_cache.json";

pub async fn get_single_token_data<M: Middleware + 'static>(
    address: Address,
    provider: Arc<M>,
    token_cache: &TokenCache,
) -> Result<TokenInfo,anyhow::Error > {

    if let Some(cached) = token_cache.get(&address) {
        return Ok(cached.clone());
    }

    let contract = ERC20::new(address, provider.clone());

    sleep(Duration::from_millis(300)).await;

    let symbol = if let Ok(sym) = contract.symbol().call().await {
        let sym_trimmed = sym.trim();
        if sym_trimmed.is_empty()
            || sym_trimmed.to_lowercase().contains("test")
            || sym_trimmed.to_lowercase().contains("null")
        {
            return Err(anyhow::anyhow!("Невалидный символ токена"));
        }
        if let Ok(re) = regex::Regex::new(r"^[a-zA-Z0-9_]{1,20}$") {
            if !re.is_match(sym_trimmed) {
                return Err(anyhow::anyhow!("Невалидный символ токена"));
            }
        }
        sym_trimmed.to_string()
    } else {
        return Err(anyhow::anyhow!("Ошибка вызова symbol()"));
    };

    let decimals = contract.decimals().call().await?;

    let token_info = TokenInfo {
        symbol: symbol.clone(),
        decimals,
    };

    token_cache.insert(address, token_info.clone());

    // Сохранение в JSON
    if let Err(e) = save_token_cache_to_json(token_cache).await {
        error!("[TOKEN][КЭШ] Ошибка записи JSON: {:?}", e);
    }

    Ok(token_info)
}

pub async fn save_token_cache_to_json(
    token_cache: &TokenCache,
) -> Result<(), Box<dyn std::error::Error>> {
    let map: HashMap<_, _> = token_cache
        .iter()
        .map(|kv| (*kv.key(), kv.value().clone()))
        .collect();
    let json = serde_json::to_string_pretty(&map)?;
    std::fs::write(TOKEN_CACHE_JSON_PATH, json)?;
    Ok(())
}

pub fn load_token_list_from_json() -> HashMap<Address, TokenInfo> {
    let json = fs::read_to_string(TOKEN_CACHE_JSON_PATH)
        .expect("Не удалось прочитать token_cache.json");
    let raw_map: HashMap<String, TokenInfo> =
        serde_json::from_str(&json).expect("Ошибка парсинга token_cache.json");

    raw_map
        .into_iter()
        .filter_map(|(addr_str, info)| {
            addr_str.parse::<Address>().ok().map(|addr| (addr, info))
        })
        .collect()
}

pub fn load_token_whitelist(path: &str) -> HashSet<Address> {
    let json = fs::read_to_string(path).expect("Не удалось прочитать token_white_list.json");
    let raw_map: HashMap<String, String> =
        serde_json::from_str(&json).expect("Ошибка парсинга token_white_list.json");

    raw_map
        .into_iter()
        .filter_map(|(addr_str, _symbol)| addr_str.parse::<Address>().ok())
        .collect()
}

