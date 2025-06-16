use anyhow::Result;
use ethers::types::{Address, U256};
use log::{error, info, debug};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    sync::Arc,
    io::Read,
};
use tokio::sync::watch;

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct AaveTokenLiquidity {
    #[serde_as(as = "HashSet<DisplayFromStr>")]
    pub token_address: HashSet<Address>,
    #[serde_as(as = "HashMap<DisplayFromStr, (_, DisplayFromStr)>")]
    pub token_info: HashMap<Address, (String, U256)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AaveLiquiditySnapshot {
    pub timestamp: String,
    pub data: AaveTokenLiquidity,
}

pub async fn get_aave_data(
    _provider: Arc<ethers::providers::Provider<ethers::providers::Http>>, // Оставлен для совместимости
    aave_sender: watch::Sender<AaveTokenLiquidity>,
) -> Result<()> {
    debug!("[ AAVE_DEBUG ] Начало get_aave_data");
    // Загрузка данных из файла
    debug!("[ AAVE_DEBUG ] Попытка загрузки данных из файла aave_liquidity.json");
    let mut liquidity_data = AaveTokenLiquidity::default();
    match File::open("aave_liquidity.json") {
        Ok(mut file) => {
            debug!("[ AAVE_DEBUG ] Файл aave_liquidity.json успешно открыт");
            let mut contents = String::new();
            if file.read_to_string(&mut contents).is_ok() {
                debug!("[ AAVE_DEBUG ] Файл прочитан, размер содержимого: {} байт", contents.len());
                match serde_json::from_str::<AaveLiquiditySnapshot>(&contents) {
                    Ok(snapshot) => {
                        liquidity_data = snapshot.data;
                        info!(
                            "[ AAVE ] Загружены данные из файла: {} токенов, время: {}",
                            liquidity_data.token_info.len(),
                            snapshot.timestamp
                        );
                        /*
                        for (addr, (symbol, liquidity)) in &liquidity_data.token_info {
                            info!("[ AAVE ] Токен {} ({:?}): ликвидность {}", symbol, addr, liquidity);
                            debug!("[ AAVE_DEBUG ] Токен: symbol: {}, address: {:?}, liquidity: {}", symbol, addr, liquidity);
                        }
                        */
                    }
                    Err(e) => {
                        error!("[ AAVE ] Ошибка парсинга файла: {:?}", e);
                        debug!("[ AAVE_DEBUG ] Ошибка десериализации JSON: {:?}", e);
                    }
                }
            } else {
                error!("[ AAVE ] Не удалось прочитать файл");
            }
        }
        Err(e) => {
            error!("[ AAVE ] Не удалось открыть файл: {:?}", e);
        }
    } 

    // Однократная отправка данных в канал
    debug!("[ AAVE_DEBUG ] Отправка данных в канал");
    if let Err(e) = aave_sender.send(liquidity_data.clone()) {
        error!("[ AAVE ] Ошибка отправки данных в канал: {:?}", e);
    } else {
        info!("[ AAVE ] Данные отправлены в канал: {} токенов", liquidity_data.token_info.len());
    }

    Ok(())
}






/*use anyhow::Result;
use chrono::Utc;
use ethers::{
    prelude::*,
    types::{Address, U256},
};
use log::{error, info};
use serde::{Deserialize, Serialize};
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
    #[serde_as(as = "HashSet<DisplayFromStr>")]
    pub token_address: HashSet<Address>,
    #[serde_as(as = "HashMap<DisplayFromStr, (_, DisplayFromStr)>")]
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
    let data_provider_address: Address = env::var("ARBITRUM_AAVE_V3_POOL_DATA_PROVIDER_ADDRESS")?.parse()?;

    info!("[ AAVE ] Используется пул: {:?}, провайдер данных: {:?}", pool_address, data_provider_address);

    let pool_data_provider = AavePoolDataProvider::new(data_provider_address, provider.clone());
    let pool_v3 = AavePool::new(pool_address, provider.clone());

    // Запускаем загрузку из файла в отдельной задаче
    let file_load_task = tokio::spawn({
        let aave_sender = aave_sender.clone();
        async move {
            match File::open("aave_liquidity.json") {
                Ok(mut file) => {
                    let mut contents = String::new();
                    if file.read_to_string(&mut contents).is_ok() {
                        match serde_json::from_str::<AaveLiquiditySnapshot>(&contents) {
                            Ok(snapshot) => {
                                info!("[ AAVE ] Загружены данные из файла: {} токенов, время: {}", snapshot.data.token_info.len(), snapshot.timestamp);
                                for (addr, (symbol, liquidity)) in &snapshot.data.token_info {
                                    info!("[ AAVE ] Токен {} ({:?}): ликвидность {}", symbol, addr, liquidity);
                                }
                                if let Err(e) = aave_sender.send(snapshot.data) {
                                    error!("[ AAVE ] Ошибка отправки данных из файла: {:?}", e);
                                }
                            }
                            Err(e) => error!("[ AAVE ] Ошибка парсинга файла: {:?}", e),
                        }
                    } else {
                        error!("[ AAVE ] Не удалось прочитать файл: {:?}", contents);
                    }
                }
                Err(e) => error!("[ AAVE ] Не удалось открыть файл: {:?}", e),
            }
        }
    });

    // Основной цикл обновления данных
    let mut interval = interval(Duration::from_secs(120));
    loop {
        interval.tick().await;
        info!("[ AAVE ] Обновление данных с блокчейна...");

        match pool_data_provider.get_all_reserves_tokens().call().await {
            Ok(reserves) => {
                info!("[ AAVE ] Получено {} токенов с блокчейна", reserves.len());
                let mut new_data = AaveTokenLiquidity::default();
                let mut all_tokens_processed = true;

                // Параллельная обработка токенов
                let tasks: Vec<_> = reserves.iter().map(|token| {
                    let pool_v3 = pool_v3.clone();
                    let token = token.clone();
                    async move {
                        match pool_v3.get_reserve_data(token.token_address).call().await {
                            Ok(reserve_data) => {
                                info!(
                                    "[ AAVE ] Токен {} ({:?}): ликвидность {}",
                                    token.symbol, token.token_address, reserve_data.available_liquidity
                                );
                                Some((token.token_address, (token.symbol, reserve_data.available_liquidity)))
                            }
                            Err(e) => {
                                error!("[ AAVE ] Ошибка для токена {}: {:?}", token.symbol, e);
                                None
                            }
                        }
                    }
                }).collect();

                // Ожидаем завершения всех запросов
                let results = futures::future::join_all(tasks).await;

                for result in results {
                    if let Some((address, (symbol, liquidity))) = result {
                        new_data.token_address.insert(address);
                        new_data.token_info.insert(address, (symbol, liquidity));
                    } else {
                        all_tokens_processed = false;
                    }
                }

                // Сохраняем и отправляем данные, только если все токены обработаны
                if all_tokens_processed && !new_data.token_info.is_empty() {
                    let snapshot = AaveLiquiditySnapshot {
                        timestamp: Utc::now().to_rfc3339(),
                        data: new_data.clone(),
                    };

                    if let Err(e) = aave_sender.send(new_data) {
                        error!("[ AAVE ] Ошибка отправки данных: {:?}", e);
                    } else {
                        info!("[ AAVE ] Данные успешно отправлены в канал");
                    }

                    if let Ok(json) = serde_json::to_string_pretty(&snapshot) {
                        match File::create("aave_liquidity.json") {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(json.as_bytes()) {
                                    error!("[ AAVE ] Ошибка записи в JSON: {:?}", e);
                                } else {
                                    info!("[ AAVE ] Сохранено {} токенов в aave_liquidity.json", snapshot.data.token_info.len());
                                }
                            }
                            Err(e) => error!("[ AAVE ] Ошибка создания файла: {:?}", e),
                        }
                    } else {
                        error!("[ AAVE ] Ошибка сериализации: {:?}", snapshot);
                    }
                } else {
                    error!("[ AAVE ] Не все токены обработаны, пропускаем обновление");
                }
            }
            Err(e) => error!("[ AAVE ] Ошибка получения списка токенов: {:?}", e),
        }

        info!("[ AAVE ] Ожидание следующего обновления");
    }
}*/



