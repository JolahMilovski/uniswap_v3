use ethers::contract::abigen;
use ethers::types::Address;
use ethers::prelude::*;
use log::error;
use std::{collections::HashMap, fs};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Serialize, Deserialize};

const TOKEN_CACHE_BIN_PATH: &str = "token_cache.bin";
const TOKEN_CACHE_JSON_PATH: &str = "token_cache.json";

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

pub type TokenCache = Arc<Mutex<HashMap<Address, TokenInfo>>>;



pub async fn get_single_token_data<M: Middleware + 'static>(
    address: H160,
    provider: Arc<M>,
    token_cache: &TokenCache,
) -> Result<TokenInfo, anyhow::Error> {
    {
        let cache = token_cache.lock().await;
        if let Some(cached) = cache.get(&address) {
            return Ok(cached.clone());
        }
    }

    let contract = ERC20::new(address, provider.clone());

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

    {
        let mut cache = token_cache.lock().await;
        cache.insert(address, token_info.clone());

        if let Ok(data) = bincode::serialize(&*cache) {
            if let Err(e) = std::fs::write(TOKEN_CACHE_BIN_PATH, &data) {
                error!("[КЭШ] Ошибка записи BIN: {:?}", e);
            }
        }
    }

    Ok(token_info)
}

pub async fn load_token_cache() -> Option<HashMap<Address, TokenInfo>> {
    if let Ok(bytes) = tokio::fs::read("token_cache.bin").await {
        match bincode::deserialize::<HashMap<Address, TokenInfo>>(&bytes) {
            Ok(map) => Some(map),
            Err(e) => {
                error!("Ошибка при десериализации token_cache.bin: {:?}", e);
                None
            }
        }
    } else {
        None
    }
}

pub async fn save_token_cache_to_json(token_cache: &TokenCache) -> Result<(), Box<dyn std::error::Error>> {
    let cache = token_cache.lock().await;
    let json = serde_json::to_string_pretty(&*cache)?;
    std::fs::write(TOKEN_CACHE_JSON_PATH, json)?;
    Ok(())
}

pub fn load_token_list_from_json(path: &str) -> HashMap<Address, String> {
    let json = fs::read_to_string(path).expect("Не удалось прочитать token_list.json");
    let raw_map: HashMap<String, String> = serde_json::from_str(&json).expect("Ошибка парсинга token_list.json");

    raw_map
        .into_iter()
        .filter_map(|(addr_str, symbol)| {
            addr_str.parse::<Address>().ok().map(|addr| (addr, symbol))
        })
        .collect()
}