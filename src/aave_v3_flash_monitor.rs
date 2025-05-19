use anyhow::Result;
use ethers::{
    prelude::*,
    types::{Address, U256},
};
use chrono::Utc;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::{env, fs::OpenOptions, io::Write};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

abigen!(
    AavePoolDataProvider,
    r#"[{
        "inputs":[],
        "name":"getAllReservesTokens",
        "outputs":[{"components":[{"internalType":"string","name":"symbol","type":"string"},{"internalType":"address","name":"tokenAddress","type":"address"}],"internalType":"struct AaveProtocolDataProvider.TokenData[]","name":"","type":"tuple[]"}],
        "stateMutability":"view",
        "type":"function"
    },{
        "inputs":[{"internalType":"address","name":"asset","type":"address"}],
        "name":"getReserveTokensAddresses",
        "outputs":[
            {"internalType":"address","name":"aTokenAddress","type":"address"},
            {"internalType":"address","name":"stableDebtTokenAddress","type":"address"},
            {"internalType":"address","name":"variableDebtTokenAddress","type":"address"}
        ],
        "stateMutability":"view",
        "type":"function"
    }]"#
);

abigen!(
    AavePool,
    r#"[{
        "inputs":[{"internalType":"address","name":"asset","type":"address"}],
        "name":"getReserveData",
        "outputs":[{
            "components":[
                {"internalType":"uint256","name":"availableLiquidity","type":"uint256"},
                {"internalType":"uint256","name":"totalStableDebt","type":"uint256"},
                {"internalType":"uint256","name":"totalVariableDebt","type":"uint256"},
                {"internalType":"uint256","name":"liquidityRate","type":"uint256"},
                {"internalType":"uint256","name":"variableBorrowRate","type":"uint256"},
                {"internalType":"uint256","name":"stableBorrowRate","type":"uint256"},
                {"internalType":"uint40","name":"lastUpdateTimestamp","type":"uint40"},
                {"internalType":"address","name":"aTokenAddress","type":"address"},
                {"internalType":"address","name":"stableDebtTokenAddress","type":"address"},
                {"internalType":"address","name":"variableDebtTokenAddress","type":"address"}
            ],
            "internalType":"struct DataTypes.ReserveData",
            "name":"",
            "type":"tuple"
        }],
        "stateMutability":"view",
        "type":"function"
    }]"#
);

#[derive(Debug, Serialize, Deserialize)]
pub struct AaveTokenLiquidity {
    pub token_address: Address,
    pub symbol: String,
    pub available_liquidity: U256,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AaveLiquiditySnapshot {
    pub timestamp: String,
    pub data: Vec<AaveTokenLiquidity>,
}


pub async fn get_aave_data(provider: Arc<Provider<Http>>) -> Result<()> {
    
            let pool_address: Address = env::var("ARBITRUM_AAVE_V3_POOL_ADDRESS")?.parse()?;
            let data_provider_address: Address = env::var("ARBITRUM_AAVE_V3_POOL_DATA_PROVIDER_ADDRESS")?.parse()?;
    

        let pool_data_provider = AavePoolDataProvider::new(data_provider_address, provider.clone());
        let pool_v3 = AavePool::new(pool_address, provider.clone());

    loop {
        info!("🔄  [AAVE] Начинаем обновление ликвидности Aave");

        match pool_data_provider.get_all_reserves_tokens().call().await {
            Ok(reserves) => {
                info!("✅  [AAVE] Получено {} токенов из data provider", reserves.len());

                let mut snapshot_data = Vec::with_capacity(reserves.len());

                for token in &reserves {
                    match pool_v3.get_reserve_data(token.token_address).call().await {
                        Ok(reserve_data) => {
                            info!("💧  [AAVE] Токен {} ({:?}): ликвидность = {}", token.symbol, token.token_address, reserve_data.available_liquidity);

                            snapshot_data.push(AaveTokenLiquidity {
                                token_address: token.token_address,
                                symbol: token.symbol.clone(),
                                available_liquidity: reserve_data.available_liquidity,
                            });
                        }
                        Err(e) => {
                            error!("❌ [AAVE] Ошибка при получении ликвидности для токена {}: {:?}", token.symbol, e);
                        }
                    }
                }

                let snapshot = AaveLiquiditySnapshot {
                    timestamp: Utc::now().to_rfc3339(),
                    data: snapshot_data,
                };

                match serde_json::to_string_pretty(&snapshot) {
                    Ok(json) => {
                        match OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open("aave_liquidity.json")
                        {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(json.as_bytes()) {
                                    error!("❌  [AAVE]  Ошибка записи JSON в файл: {:?}", e);
                                }
                                if let Err(e) = file.write_all(b"\n") {
                                    error!("❌  [AAVE] Ошибка записи новой строки в файл: {:?}", e);
                                }

                                info!("🟢  [AAVE]  [{}] Сохранили {} записей в aave_liquidity.json", snapshot.timestamp, snapshot.data.len());
                            }
                            Err(e) => {
                                error!("❌  [AAVE]  Не удалось открыть файл aave_liquidity.json: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("❌  [AAVE]  Ошибка сериализации JSON: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("❌  [AAVE]  Ошибка получения списка резервов из data provider: {:?}", e);
            }
        }

        sleep(Duration::from_secs(30)).await;
    }

} 
