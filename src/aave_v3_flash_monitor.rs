use anyhow::Result;
use chrono::Utc;
use ethers::{
    prelude::*,
    types::{Address, U256},
};
use log::{error, info, debug};
use serde::{Deserialize, Serialize, Deserializer};
use serde_with::{serde_as, DisplayFromStr};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    sync::Arc,
    io::{Read, Write},
};
use std::{env};
use tokio::{
    sync::watch,
    time::{interval, Duration},
};

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

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct AaveTokenLiquidity {
    pub token_address: HashSet<Address>,
    #[serde_as(as = "HashMap<_, (String, DisplayFromStr)>")]
    pub token_info: HashMap<Address, (String, U256)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AaveLiquiditySnapshot {
    pub timestamp: String,
    pub data: AaveTokenLiquidity,
}

pub async fn get_aave_data(
    provider: Arc<Provider<Http>>,
    aave_sender: watch::Sender<AaveTokenLiquidity>,
) -> Result<()> {
    let pool_address: Address = env::var("ARBITRUM_AAVE_V3_POOL_ADDRESS")?.parse()?;
    let data_provider_address: Address =
        env::var("ARBITRUM_AAVE_V3_POOL_DATA_PROVIDER_ADDRESS")?.parse()?;
    info!("[AAVE] Используется пул: {:?}, провайдер данных: {:?}", pool_address, data_provider_address);

    let pool_data_provider = AavePoolDataProvider::new(data_provider_address, provider.clone());
    let pool_v3 = AavePool::new(pool_address, provider.clone());

    // Десериализация aave_liquidity.json
    let mut liquidity_data = AaveTokenLiquidity::default();
    match File::open("aave_liquidity.json") {
        Ok(mut file) => {
            let mut contents = String::new();
            if let Ok(_) = file.read_to_string(&mut contents) {
                match serde_json::from_str::<AaveLiquiditySnapshot>(&contents) {
                    Ok(snapshot) => {
                        liquidity_data = snapshot.data;
                        info!("[AAVE] Загружено {} токенов из aave_liquidity.json", liquidity_data.token_address.len());
                        if let Err(e) = aave_sender.send(liquidity_data.clone()) {
                            error!("[AAVE] Ошибка отправки данных из JSON через канал: {}", e);
                        } else {
                            info!("[AAVE] Данные из JSON отправлены в канал");
                        }
                    }
                    Err(e) => error!("[AAVE] Ошибка десериализации aave_liquidity.json: {:?}", e),
                }
            } else {
                error!("[AAVE] Не удалось прочитать aave_liquidity.json");
            }
        }
        Err(e) => error!("[AAVE] Не удалось открыть aave_liquidity.json: {:?}", e),
    }

    // Интервал опроса блокчейна (2 минуты)
    let mut interval = interval(Duration::from_secs(120));

    loop {
        interval.tick().await;
        info!("🔄 [AAVE] Начинаем обновление ликвидности Aave");

        match pool_data_provider.get_all_reserves_tokens().call().await {
            Ok(reserves) => {
                info!("✅ [AAVE] Получено {} токенов из data provider", reserves.len());
                let token_addresses: Vec<String> = reserves.iter().map(|t| format!("{:?}", t.token_address)).collect();
                info!("[AAVE] Адреса токенов: {:?}", token_addresses);

                let mut token_address = HashSet::with_capacity(reserves.len());
                let mut token_info = HashMap::with_capacity(reserves.len());

                for token in &reserves {
                    info!("[AAVE] Обработка токена: {}, адрес: {:?}", token.symbol, token.token_address);
                    info!("[AAVE] Вызов get_reserve_data для токена: {}", token.symbol);
                    match pool_v3.get_reserve_data(token.token_address).call().await {
                        Ok(reserve_data) => {
                            info!("[AAVE] Успешно получены данные для токена: {}", token.symbol);
                            token_address.insert(token.token_address);
                            token_info.insert(
                                token.token_address,
                                (token.symbol.clone(), reserve_data.available_liquidity),
                            );
                            info!("[AAVE] Токен {}: адрес {:?}, ликвидность {}", token.symbol, token.token_address, reserve_data.available_liquidity);
                        }
                        Err(e) => {
                            error!(
                                "❌ [AAVE] [{}] Ошибка при получении ликвидности для токена {}: {:?}", 
                                Utc::now().to_rfc3339(), token.symbol, e
                            );
                            continue;
                        }
                    }
                }

                let snapshot = AaveLiquiditySnapshot {
                    timestamp: Utc::now().to_rfc3339(),
                    data: AaveTokenLiquidity {
                        token_address,
                        token_info,
                    },
                };

                if let Err(e) = aave_sender.send(snapshot.data.clone()) {
                    error!("[AAVE] Ошибка отправки обновленных данных через канал: {}", e);
                } else {
                    info!("[AAVE] Данные о {} токенах отправлены в канал", snapshot.data.token_address.len());
                }

                match serde_json::to_string_pretty(&snapshot) {
                    Ok(json) => {
                        match File::create("aave_liquidity.json") {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(json.as_bytes()) {
                                    error!("[AAVE] Ошибка записи в JSON: {:?}", e);
                                } else {
                                    info!(
                                        "🟢 [AAVE] [{}] Сохранено {} токенов в aave_liquidity.json",
                                        snapshot.timestamp,
                                        snapshot.data.token_info.len()
                                    );
                                    info!("📤 [AAVE] Данные: \n{}", json);
                                }
                            }
                            Err(e) => {
                                error!("[AAVE] Не удалось создать файл aave_liquidity.json: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("[AAVE] Ошибка сериализации JSON: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("[AAVE] Ошибка получения списка резервов из data provider: {:?}", e);
            }
        }

        info!("[AAVE] Ожидание следующего обновления");
    }
}