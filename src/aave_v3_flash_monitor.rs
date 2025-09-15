use anyhow::Result;
use chrono::Utc;
use ethers::{
    prelude::*,
    types::{Address, U256},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    io::{Read, Write},
    sync::Arc,
};
use tokio::{
    sync::watch,
    time::{interval, Duration},
};
use tracing::{error, info};

abigen!(
    AavePoolDataProvider,
    r#"[{
        "inputs":[],
        "name":"getAllReservesTokens",
        "outputs":[{"components":[{"internalType":"string",
        "name":"symbol",
        "type":"string"},{"internalType":"address","name":"tokenAddress","type":"address"}],"internalType":"struct AaveProtocolDataProvider.TokenData[]","name":"","type":"tuple[]"}],
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
        "name":"getVirtualUnderlyingBalance",
        "outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
        "stateMutability":"view",
        "type":"function"
    }]"#
);

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct AaveTokenLiquidity {
    #[serde_as(as = "HashSet<DisplayFromStr>")]
    pub aave_token_address: HashSet<Address>,
    #[serde_as(as = "HashMap<DisplayFromStr, (_, DisplayFromStr)>")]
    pub aave_token_info: HashMap<Address, (String, U256)>, // symbol, virtual_balance
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

    info!(
        "[ AAVE ] Используется пул: {:?}, провайдер данных: {:?}",
        pool_address, data_provider_address
    );

    let pool_data_provider = AavePoolDataProvider::new(data_provider_address, provider.clone());
    let pool_v3 = AavePool::new(pool_address, provider.clone());

    let mut interval = interval(Duration::from_secs(120));
    loop {
        interval.tick().await;
        info!("[ AAVE ] Обновление данных с блокчейна...");

        let mut new_data = AaveTokenLiquidity::default();
        let mut all_ok = true;

        // Попытка загрузки данных с блокчейна
        match pool_data_provider.get_all_reserves_tokens().call().await {
            Ok(reserves) => {
                info!("[ AAVE ] Получено {} токенов", reserves.len());

                let tasks: Vec<_> = reserves
                    .iter()
                    .map(|token| {
                        let pool_v3 = pool_v3.clone();
                        let token = token.clone();
                        async move {
                            match pool_v3
                                .get_virtual_underlying_balance(token.token_address)
                                .call()
                                .await
                            {
                                Ok(virtual_balance) => {
                                    info!(
                                        "[ AAVE ] Токен {} c блокчейна ({:?}): virtualBalance {}",
                                        token.symbol, token.token_address, virtual_balance
                                    );
                                    Some((token.token_address, (token.symbol, virtual_balance)))
                                }
                                Err(e) => {
                                    error!(
                                        "[ AAVE ] Ошибка получения virtualBalance для {}: {:?}",
                                        token.symbol, e
                                    );
                                    None
                                }
                            }
                        }
                    })
                    .collect();

                let results = futures::future::join_all(tasks).await;

                for result in results {
                    if let Some((addr, (symbol, virtual_balance))) = result {
                        new_data.aave_token_address.insert(addr);
                        new_data
                            .aave_token_info
                            .insert(addr, (symbol, virtual_balance));
                    } else {
                        all_ok = false;
                    }
                }

                if all_ok && !new_data.aave_token_info.is_empty() {
                    let snapshot = AaveLiquiditySnapshot {
                        timestamp: Utc::now().to_rfc3339(),
                        data: new_data.clone(),
                    };

                    let _ = aave_sender.send(new_data.clone());

                    if let Ok(json) = serde_json::to_string_pretty(&snapshot) {
                        match File::create("aave_liquidity.json") {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(json.as_bytes()) {
                                    error!("[ AAVE ] Ошибка записи JSON: {:?}", e);
                                } else {
                                    info!(
                                        "[ AAVE ] Сохранено {} токенов в aave_liquidity.json",
                                        snapshot.data.aave_token_info.len()
                                    );
                                }
                            }
                            Err(e) => error!("[ AAVE ] Ошибка создания файла: {:?}", e),
                        }
                    }
                    continue; // Данные успешно загружены с блокчейна, продолжаем цикл
                }
            }
            Err(e) => {
                error!("[ AAVE ] Ошибка запроса токенов с блокчейна: {:?}", e);
            }
        }

        // Если данные с блокчейна не получены, пытаемся загрузить из файла
        info!("[ AAVE ] Попытка загрузки данных из файла aave_liquidity.json");

        let mut fallback_data = AaveTokenLiquidity::default();
        match File::open("aave_liquidity.json") {
            Ok(mut file) => {
                let mut contents = String::new();
                if file.read_to_string(&mut contents).is_ok() {
                    match serde_json::from_str::<AaveLiquiditySnapshot>(&contents) {
                        Ok(snapshot) => {
                            fallback_data = snapshot.data.clone();
                            info!(
                                "[ AAVE ] Загружены данные из файла: {} токенов, время: {}",
                                snapshot.data.aave_token_info.len(),
                                snapshot.timestamp
                            );
                            let _ = aave_sender.send(fallback_data.clone());
                        }
                        Err(e) => {
                            error!("[ AAVE ] Ошибка парсинга файла: {:?}", e);
                        }
                    }
                } else {
                    error!("[ AAVE ] Не удалось прочитать файл");
                }
            }
            Err(e) => {
                error!("[ AAVE ] Не удалось открыть файл: {:?}", e);
                if e.kind() == std::io::ErrorKind::NotFound {
                    match File::create("aave_liquidity.json") {
                        Ok(mut file) => {
                            let empty_snapshot = AaveLiquiditySnapshot {
                                timestamp: Utc::now().to_rfc3339(),
                                data: AaveTokenLiquidity::default(),
                            };
                            if let Ok(json) = serde_json::to_string_pretty(&empty_snapshot) {
                                let _ = file.write_all(json.as_bytes());
                                info!("[ AAVE ] Создан пустой файл aave_liquidity.json");
                            }
                        }
                        Err(e) => error!("[ AAVE ] Ошибка создания файла: {:?}", e),
                    }
                }
            }
        }

        // Если данные из файла пусты, отправляем пустые данные
        if fallback_data.aave_token_info.is_empty() {
            error!("[ AAVE ] Нет данных для fallback, отправка пустых данных");
            let _ = aave_sender.send(AaveTokenLiquidity::default());
        }

        info!("[ AAVE ] Ожидание следующего обновления");
    }
}
