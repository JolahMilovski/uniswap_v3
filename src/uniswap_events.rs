use std::{
    collections::HashMap, env, sync::{
        atomic::{AtomicU64, Ordering}, Arc
    }, thread::sleep, time::Duration
};

use anyhow::Context;
use colored::Colorize;
use dashmap::DashSet;
use ethers::{contract::EthEvent, types::H160};
use ethers::{abi::RawLog, utils::keccak256};
use ethers::{
    contract::EthLogDecode,
    providers::{Http, Middleware, Provider, Ws},
    types::{Address, BlockNumber, Filter, H256, I256, U256, U512},
};
use ethers_contract::abigen;
use futures::StreamExt;
use im::OrdMap;
use log::{debug, error, info, warn};
use tokio::sync::{
         mpsc::{
            self}, watch
    };

use crate::{
    uniswap_graph::UniversalGraph, uniswap_v3::{calculate_current_price, process_pool_data, UniswapV3Pool}
};

abigen!(
    UniswapPool,
    r#"[{
        "inputs": [],
        "name": "liquidity",
        "outputs": [
            { "internalType": "uint128", "name": "", "type": "uint128" }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            { "internalType": "int24", "name": "tick", "type": "int24" }
        ],
        "name": "ticks",
        "outputs": [
            { "internalType": "uint128", "name": "liquidityGross", "type": "uint128" },
            { "internalType": "int128", "name": "liquidityNet", "type": "int128" },
            { "internalType": "uint256", "name": "feeGrowthOutside0X128", "type": "uint256" },
            { "internalType": "uint256", "name": "feeGrowthOutside1X128", "type": "uint256" },
            { "internalType": "int56", "name": "tickCumulativeOutside", "type": "int56" },
            { "internalType": "uint160", "name": "secondsPerLiquidityOutsideX128", "type": "uint160" },
            { "internalType": "uint32", "name": "secondsOutside", "type": "uint32" },
            { "internalType": "bool", "name": "initialized", "type": "bool" }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            { "indexed": true, "internalType": "address", "name": "sender", "type": "address" },
            { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" },
            { "indexed": false, "internalType": "int256", "name": "amount0", "type": "int256" },
            { "indexed": false, "internalType": "int256", "name": "amount1", "type": "int256" },
            { "indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160" },
            { "indexed": false, "internalType": "uint128", "name": "liquidity", "type": "uint128" },
            { "indexed": false, "internalType": "int24", "name": "tick", "type": "int24" }
        ],
        "name": "Swap",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            { "indexed": true, "internalType": "address", "name": "sender", "type": "address" },
            { "indexed": true, "internalType": "address", "name": "owner", "type": "address" },
            { "indexed": false, "internalType": "int24", "name": "tickLower", "type": "int24" },
            { "indexed": false, "internalType": "int24", "name": "tickUpper", "type": "int24" },
            { "indexed": false, "internalType": "uint128", "name": "amount", "type": "uint128" },
            { "indexed": false, "internalType": "uint256", "name": "amount0", "type": "uint256" },
            { "indexed": false, "internalType": "uint256", "name": "amount1", "type": "uint256" }
        ],
        "name": "Mint",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            { "indexed": true, "internalType": "address", "name": "sender", "type": "address" },
            { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" },
            { "indexed": false, "internalType": "uint256", "name": "amount0", "type": "uint256" },
            { "indexed": false, "internalType": "uint256", "name": "amount1", "type": "uint256" },
            { "indexed": false, "internalType": "uint256", "name": "paid0", "type": "uint256" },
            { "indexed": false, "internalType": "uint256", "name": "paid1", "type": "uint256" }
        ],
        "name": "Flash",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            { "indexed": true, "internalType": "address", "name": "owner", "type": "address" },
            { "indexed": false, "internalType": "int24", "name": "tickLower", "type": "int24" },
            { "indexed": false, "internalType": "int24", "name": "tickUpper", "type": "int24" },
            { "indexed": false, "internalType": "uint128", "name": "amount", "type": "uint128" },
            { "indexed": false, "internalType": "uint256", "name": "amount0", "type": "uint256" },
            { "indexed": false, "internalType": "uint256", "name": "amount1", "type": "uint256" }
        ],
        "name": "Burn",
        "type": "event"
    }]"#
);

abigen!(
    Multicall3,
    r#"[{
        "inputs": [
            {
                "components": [
                    { "internalType": "address", "name": "target", "type": "address" },
                    { "internalType": "bool", "name": "allowFailure", "type": "bool" },
                    { "internalType": "bytes", "name": "callData", "type": "bytes" }
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    { "internalType": "bool", "name": "success", "type": "bool" },
                    { "internalType": "bytes", "name": "returnData", "type": "bytes" }
                ],
                "internalType": "struct Multicall3.Call3Result[]",
                "name": "",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }]"#
);




#[derive(Debug, Clone, EthEvent)]
#[ethevent(
    name = "Swap",
    abi = "Swap(address,address,int256,int256,uint160,uint128,int24)"
)]
struct SwapEvent {
    #[ethevent(indexed)]
    sender: Address,
    #[ethevent(indexed)]
    recipient: Address,
    amount0: I256,
    amount1: I256,
    sqrt_price_x96: U256,
    liquidity: u128,
    tick: i32,
}

#[derive(Debug, Clone, EthEvent)]
#[ethevent(
    name = "Mint",
    abi = "Mint(address,address,int24,int24,uint128,uint256,uint256)"
)]
struct MintEvent {
    sender: Address,
    #[ethevent(indexed)]
    owner: Address,
    #[ethevent(indexed)]
    tick_lower: i32,
    #[ethevent(indexed)]
    tick_upper: i32,
    liquidity: u128,
    amount0: U256,
    amount1: U256,
}

#[derive(Debug, Clone, EthEvent)]
#[ethevent(
    name = "Burn",
    abi = "Burn(address,int24,int24,uint128,uint256,uint256)"
)]
pub struct BurnEvent {
    #[ethevent(indexed)]
    pub owner: Address,
    #[ethevent(indexed)]
    pub tick_lower: i32,
    #[ethevent(indexed)]
    pub tick_upper: i32,
    pub liquidity: u128,
    pub amount0: U256,
    pub amount1: U256,
}

#[derive(Debug, Clone, EthEvent)]
#[ethevent(
    name = "Flash",
    abi = "Flash(address,address,uint256,uint256,uint256,uint256)"
)]
pub struct FlashEvent {
    #[ethevent(indexed)]
    pub sender: Address,
    #[ethevent(indexed)]
    pub recipient: Address,
    pub amount0: U256,
    pub amount1: U256,
    pub paid0: U256,
    pub paid1: U256,
}

#[derive(Debug, Default, Clone)]
pub struct EventPoolUpdate {
    pub liquidity: U512,
    pub sqrt_price_x96: U256,
    pub current_tick: i32,
    pub tick_map: OrdMap<i32, (i128, U512)>,
    pub current_price: U512,
}

#[derive(Debug, Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Http>>,
    pub subscribed_pools: DashSet<Address>,
    last_processed_block: Arc<AtomicU64>,
}
#[derive(Debug, Clone)]
pub struct PoolEventInfo {
    pub event_id: H256,
    pub address: Address,
    pub tick_updates: DashSet<i32>,
    pub current_tick: i32,
    pub block_number: u64,
}

impl PoolEventInfo {
    pub fn new(address: Address, tick_updates: DashSet<i32>, current_tick: i32, block_number: u64) -> Self {

            debug!("[ UNISWAP_EVENTS_DEBUG_NEW_1 ] Создание нового экземпляра UniswapEventSubscriber");
            
        let hasher = keccak256(&[
            address.as_bytes(),
            &current_tick.to_le_bytes(),
            &block_number.to_le_bytes(),
        ].concat());
        Self {
            event_id: H256(hasher),
            address,
            tick_updates,
            current_tick,
            block_number,
        }
    }
}

impl UniswapEventSubscriber {

    pub fn new(provider: Arc<Provider<Http>>) -> Self {

        debug!("[ UNISWAP_EVENTS_DEBUG_NEW_1 ] Создание нового экземпляра UniswapEventSubscriber");

        let subscriber = Self {
            provider,
            subscribed_pools: DashSet::new(),
            last_processed_block: Arc::new(AtomicU64::new(0)),
        };
        debug!("[ UNISWAP_EVENTS_DEBUG_NEW_2 ] Экземпляр UniswapEventSubscriber создан");
        subscriber
    }

    /// Подписывается на получение новых блоков из блокчейна через WebSocket соединение
    /// 
    /// # Описание
    /// Функция устанавливает постоянное соединение с блокчейном для получения информации о новых блоках.
    /// При обрыве соединения автоматически выполняет переподключение после заданной задержки.
    /// Отслеживает и отправляет номера новых блоков через канал watch::Sender.
    /// 
    /// # Аргументы
    /// * `provider_ws` - WebSocket провайдер для подключения к блокчейну
    /// * `block_sender` - Канал для отправки номеров новых блоков
    /// 
    /// # Возвращаемое значение
    /// * `anyhow::Result<()>` - Результат выполнения операции
    /// 
    /// # Детали реализации
    /// * Использует бесконечный цикл для поддержания постоянного соединения
    /// * Отслеживает последний отправленный блок для избежания дубликатов
    /// * Логирует каждый 100-й блок для мониторинга
    /// * При ошибках переподключается через 1 секунду
    pub async fn subscribe_to_new_blocks(
        provider_ws: &Arc<Provider<Ws>>,
        block_sender: watch::Sender<u64>,
    ) -> anyhow::Result<()> {
        debug!("[ UNISWAP_EVENTS_DEBUG ] Начало subscribe_to_new_blocks");
        info!("[ UNISWAP_EVENTS_BLOCKS ] Запускаем подписку на новые блоки...");
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);
        let mut last_sent_block: u64 = 0;
        loop {
            match provider_ws.subscribe_blocks().await {
                Ok(mut stream) => {
                    debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Успешная подписка на поток блоков");
                    while let Some(block) = stream.next().await {
                        debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Получен новый блок: {:?}", block.number);
                        if let Some(number) = block.number {
                            let n = number.as_u64();
                            if n != last_sent_block {
                                debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Отправка блока {} в канал", n);
                                last_sent_block = n;
                                let _ = block_sender.send(n);
                            }
                            if n % 10 == 0 {
                                info!("[ UNISWAP_EVENTS_BLOCKS ] Новый блок: {}", n);
                                debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Логирование блока: {}", n);
                            }
                        }
                    }
                    info!("[ UNISWAP_EVENTS_BLOCKS ] Поток блоков завершился. Переподключение...");
                }
                Err(e) => {
                    error!("[ UNISWAP_EVENTS_BLOCKS_ERROR ] Ошибка подписки: {e}. Переподключение...");
                }
            }
            debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Ожидание {} секунд перед переподключением", RECONNECT_DELAY.as_secs());
            sleep(RECONNECT_DELAY);
        }
    }



    /// Returns a vector of event topics for Uniswap V3 pool events
    /// 
    /// # Returns
    /// * `Vec<H256>` - Vector containing the following event topics:
    ///   * Swap event - tracks token swaps in the pool
    ///   * Mint event - tracks liquidity additions
    ///   * Burn event - tracks liquidity removals
    ///   * Flash event - tracks flash loan events
    fn get_event_topics() -> Vec<H256> {
        debug!("[ UNISWAP_EVENTS_GET_TOPIC_DEBUG ] Получение топиков событий");
        let topics = vec![
            H256::from_slice(&keccak256(
                b"Swap(address,address,int256,int256,uint160,uint128,int24)",
            )),
            H256::from_slice(&keccak256(
                b"Mint(address,address,int24,int24,uint128,uint256,uint256)",
            )),
            H256::from_slice(&keccak256(
                b"Burn(address,int24,int24,uint128,uint256,uint256)",
            )),
            H256::from_slice(&keccak256(
                b"Flash(address,address,uint256,uint256,uint256,uint256)",
            )),
        ];
        debug!("[ UNISWAP_EVENTS_GET_TOPIC_DEBUG ] Возвращено {} топиков", topics.len());
        topics
    } 
    /// Получает события из блокчейна для подписанных пулов Uniswap V3
    /// 
    /// # Аргументы
    /// * `from_block` - Начальный блок для получения событий
    /// * `to_block` - Конечный блок для получения событий
    /// 
    /// # Возвращает
    /// * `Result<Vec<PoolEventInfo>>` - Вектор с информацией о событиях для каждого пула
    pub async fn fetch_events(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<PoolEventInfo>> {
        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Начало fetch_events, from_block: {}, to_block: {}", from_block, to_block);
        
        // Проверка корректности диапазона блоков
        if from_block > to_block {
            warn!("[ UNISWAP_FETCH_EVENT_WARN! ] Ошибка: from_block ({}) больше to_block ({})",
                from_block, to_block
            );
            return Ok(vec![]);
        }

        // Получаем список адресов подписанных пулов
        let subscribed_pool_addresses: Vec<Address> = self
            .subscribed_pools
            .iter()
            .map(|entry| *entry.key())
            .collect();
        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Подписано пулов: {}", subscribed_pool_addresses.len());

        // Проверяем наличие подписанных пулов
        if subscribed_pool_addresses.is_empty() {
            info!("[ UNISWAP_FETCH_EVENT ] Нет подписанных пулов");
            return Ok(vec![]);
        }

        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Создание фильтра для логов");
        
        // Создаем фильтр для получения логов
        let filter = Filter::new()
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .address(subscribed_pool_addresses)
            .topic0(Self::get_event_topics());

        // Получаем логи из блокчейна
        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Запрос логов через провайдер");
        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => {
                debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Получено {} логов", logs.len());
                logs
            }
            Err(e) => {
                warn!("[ UNISWAP_EVENT_WARN! ] Ошибка RPC: {}", e);
                Vec::new()
            }
        };

        // Инициализируем структуры для хранения событий и счетчиков
        let mut event_map = HashMap::new();
        let mut swap_count = 0;
        let mut mint_count = 0;
        let mut burn_count = 0;
        let mut flash_count = 0;

        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Обработка полученных логов");
        // Обрабатываем каждый полученный лог
        for log in logs {
            let address = log.address;
            let block_number = log.block_number.map_or("неизвестен".to_string(), |n| {
                n.as_u64().to_string()
            });
            debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Обработка лога, блок: {}", address, block_number);

            // Получаем или создаем запись для пула
            let entry = event_map.entry(address).or_insert_with(|| {
                let block_number_u64 = log.block_number.map(|n| n.as_u64()).unwrap_or(0);
                debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Создание нового PoolEventInfo, block_number: {}", address, block_number_u64);
                PoolEventInfo {
                    address,
                    tick_updates: DashSet::new(),
                    current_tick: 0,
                    block_number: block_number_u64,
                    event_id:  H256::zero(),
                }
            });

            // Определяем тип события и обрабатываем его
            match log.topics.first() {
                // Обработка события Swap
                Some(topic) if *topic == SwapEvent::signature() => {
                    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Декодирование события Swap", address);
                    let raw_log = RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                        Ok(swap) => {
                            info!("[ UNISWAP_FETCH_EVENT ] Swap: пул {:?}, блок {}, amount0: {}, amount1: {}, sqrtPriceX96: {}, ликвидность: {}, тик: {}",
                                address,
                                block_number,
                                swap.amount0,
                                swap.amount1,
                                swap.sqrt_price_x96,
                                swap.liquidity,
                                swap.tick
                            );
                            debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Swap обработан, current_tick: {}", address, swap.tick);
                            entry.current_tick = swap.tick;
                            swap_count += 1;
                        }
                        Err(e) => {
                            warn!("[ UNISWA_FETCH_EVENT_WARN! ] [{:?}] Ошибка декодирования Swap: {:?}", address, e);
                        }
                    }
                }
                // Обработка события Mint
                Some(topic) if *topic == MintEvent::signature() => {
                    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Декодирование события Mint", address);
                    let raw_log = RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <MintEvent as EthLogDecode>::decode_log(&raw_log) {
                        Ok(mint) => {
                            info!("[ UNISWAP_FETCH_EVENT ] Mint: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}",
                                address,
                                block_number,
                                mint.tick_lower,
                                mint.tick_upper,
                                mint.liquidity,
                                mint.amount0,
                                mint.amount1
                            );
                            debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Mint обработан, tick_lower: {}, tick_upper: {}", address, mint.tick_lower, mint.tick_upper);
                            entry
                                .tick_updates
                                .extend([mint.tick_lower, mint.tick_upper]);
                            mint_count += 1;
                        }
                        Err(e) => {
                            warn!("[ UNISWAP_FETCH_EVENTS_WARN!][{:?}] Ошибка декодирования Mint: {:?}", address, e);
                        }
                    }
                }
                // Обработка события Burn
                Some(topic) if *topic == BurnEvent::signature() => {
                    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Декодирование события Burn", address);
                    let raw_log = RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                        Ok(burn) => {
                            info!("[ UNISWAP_FETCH_EVENT ] Burn: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}",
                                address,
                                block_number,
                                burn.tick_lower,
                                burn.tick_upper,
                                burn.liquidity,
                                burn.amount0,
                                burn.amount1
                            );
                            debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Burn обработан, tick_lower: {}, tick_upper: {}", address, burn.tick_lower, burn.tick_upper);
                            entry
                                .tick_updates
                                .extend([burn.tick_lower, burn.tick_upper]);
                            burn_count += 1;
                        }
                        Err(e) => {
                            warn!("[ UNISWAP_FETCH_EVENTS_WARN! ][{:?}] Ошибка декодирования Burn: {:?}", address, e);
                        }
                    }
                }
                // Обработка события Flash
                Some(topic) if *topic == FlashEvent::signature() => {
                    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Декодирование события Flash", address);
                    let raw_log = RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <FlashEvent as EthLogDecode>::decode_log(&raw_log) {
                        Ok(flash) => {
                            info!("[ UNISWAP_FETCH_EVENT ] Flash: пул {:?}, блок {}, заимствовано {} token0, {} token1, уплачено {} token0, {} token1",
                                address,
                                block_number,
                                flash.amount0,
                                flash.amount1,
                                flash.paid0,
                                flash.paid1
                            );
                            debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Flash обработан, amount0: {}, amount1: {}", address, flash.amount0, flash.amount1);
                            flash_count += 1;
                        }
                        Err(e) => {
                            warn!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Ошибка декодирования Flash: {:?}", address, e);
                        }
                    }
                }
                _ => {
                    debug!("[ UNISWAP_EVENTS_DEBUG ][{:?}] Неизвестный топик события", address);
                }
            }
        }

        // Обновляем номер последнего обработанного блока
        self.last_processed_block.store(to_block, Ordering::Release);
        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Обновлен last_processed_block: {}", to_block);

        // Логируем итоговую статистику, если были обработаны какие-либо события
        if swap_count > 0 || mint_count > 0 || burn_count > 0 || flash_count > 0 {
            let pool_addresses: Vec<String> =
                event_map.keys().map(|addr| format!("{:?}", addr)).collect();
            let pools_str = format!("{} пулов", pool_addresses.len());
            info!("[{}][Блоки {}-{}] Обработано {} событий (Swap: {}, Mint: {}, Burn: {}, Flash: {}) для {}",
                " UNISWAP_FETCH_EVENT ".bright_blue(),
                from_block,
                to_block,
                swap_count + mint_count + burn_count + flash_count,
                swap_count,
                mint_count,
                burn_count,
                flash_count,
                pools_str
            );
            debug!(" UNISWAP_FETCH_EVENTS_DEBUG ] Итог обработки: Swap: {}, Mint: {}, Burn: {}, Flash: {}", swap_count, mint_count, burn_count, flash_count);
        }

        // Возвращаем результат
        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Конец fetch_events, возвращено {} событий", event_map.len());
        Ok(event_map.into_values().collect())
    }





 

    /// Получает данные о тиках пула Uniswap V3 с использованием мультиколла
    ///
    /// # Описание
    /// Запрашивает данные о ликвидности, текущей цене и тиках пула, используя мультиколл для оптимизации
    /// запросов к тикам. Обновляет tick_map на основе полученных событий.
    ///
    /// # Параметры
    /// * `pool_event_info` - Информация о событиях пула
    /// * `pool_address` - Адрес пула в сети
    /// * `provider` - WebSocket-провайдер для взаимодействия с блокчейном
    /// * `graph` - Граф с данными о пулах
    ///
    /// # Возвращаемое значение
    /// * `Result<EventPoolUpdate, anyhow::Error>` - Обновленные данные пула
    pub async fn fetch_tick_data(
        &self,
        pool_event_info: &PoolEventInfo,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
        graph: Arc<UniversalGraph>,
    ) -> anyhow::Result<EventPoolUpdate> {
        debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Начало получения данных для тиков пула", pool_address);

        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

        let (liquidity, slot0, tick_spacing, _, _) =
            process_pool_data(pool_address, pool_contract.clone().into())
                .await
                .context(format!("[UNISWAP_EVENT_FETCH_TICK] Не удалось получить данные пула: {:?}", pool_address))?;

        let current_tick = slot0.1;
        debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Текущий тик: {}", pool_address, current_tick);

        let pool_info = graph
            .edges
            .get(&pool_address)
            .ok_or_else(|| anyhow::anyhow!("Пул не найден в графе: {:?}", pool_address))?;

        let tick_indices: Vec<i32> = pool_event_info
            .tick_updates
            .iter()
            .map(|tick| *tick)
            .collect();

        debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Запрашивается {} тиков", pool_address, tick_indices.len());

        let tick_results = Self::fetch_ticks_multicall(
            pool_address,
            &pool_contract,
            tick_indices,
            provider.clone(),
        ).await;

        debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Получено {} результатов тиков", pool_address, tick_results.len());

        let mut tick_map: OrdMap<i32, (i128, U512)> = OrdMap::new();
        for result in tick_results {
            if let Some((tick, data)) = result {
                if (data.0 != 0 || data.1 != 0) && tick % tick_spacing == 0 {
                    debug!("[UNISWAP_EVENTS_FETCH_TICK_DEBUG][{:?}] Добавление тика {} в tick_map", pool_address, tick);
                    tick_map.insert(tick, (data.1, U512::from(data.0)));
                } else {
                    info!(
                        "[UNISWAP_EVENT_FETCH_TICK][{:?}] Пропущен тик {} (нулевая ликвидность: gross: {}, net: {} или не кратен tick_spacing: {})",
                        pool_address, tick, data.0, data.1, tick_spacing
                    );
                }
            }
        }

        debug!("[UNISWAP_EVENTS_FETCH_TICK_DEBUG][{:?}] tick_map заполнен: {} тиков", pool_address, tick_map.len());

        let sqrt_price = U512::from(slot0.0);
        debug!("[UNISWAP_EVENTS_FETCH_TICK_DEBUG][{:?}] sqrt_price: {}", pool_address, sqrt_price);

        let current_price = calculate_current_price(
            sqrt_price,
            pool_info.uniswap_token_a_decimals,
            pool_info.uniswap_token_b_decimals,
        )
        .map_err(anyhow::Error::msg)?;
        debug!("[UNISWAP_EVENTS_FETCH_TICK_DEBUG][{:?}] current_price: {}", pool_address, current_price);

        Ok(EventPoolUpdate {
            liquidity,
            sqrt_price_x96: slot0.0,
            current_tick,
            tick_map,
            current_price,
        })
    }

    /// Выполняет мультиколл для получения данных тиков пула
    ///
    /// # Описание
    /// Использует контракт Multicall3 для одновременного запроса данных о тиках пула, что сокращает количество
    /// сетевых вызовов. Обрабатывает результаты и возвращает их в виде опциональных кортежей.
    ///
    /// # Параметры
    /// * `pool_address` - Адрес пула
    /// * `pool_contract` - Контракт пула Uniswap V3
    /// * `tick_indices` - Список индексов тиков для запроса
    /// * `provider` - WebSocket-провайдер
    ///
    /// # Возвращает
    /// * `Vec<Option<(i32, (u128, i128, U256, U256, i64, u128, u32, bool))>>` - Результаты запросов
    async fn fetch_ticks_multicall(
        pool_address: Address,
        pool_contract: &UniswapV3Pool<Provider<Ws>>,
        tick_indices: Vec<i32>,
        provider: Arc<Provider<Ws>>,
    ) -> Vec<Option<(i32, (u128, i128, U256, U256, i64, u128, u32, bool))>> {
        debug!("[UNISWAP_EVENTS_TICKS_MULTICALL_DEBUG] Начало мультиколла тиков для пула {:?}", pool_address);

        let multicall = Multicall3::new(
            env::var("MULTICALL3_ADDRESS")
                .unwrap_or("0xca11bde05977b3631167028862be2a173976ca11".to_string())
               .parse::<H160>()
                .expect("Некорректный MULTICALL3_ADDRESS"),
            provider.clone(),
        );

        let calls = tick_indices
            .iter()
            .map(|tick| {
                let call_data = pool_contract.ticks(*tick).calldata().unwrap();
                debug!("[UNISWAP_EVENTS_TICKS_MULTICALL_DEBUG][{:?}] Добавлен запрос для тика {}", pool_address, *tick);
                Call3 {
                    target: pool_address,
                    allow_failure: true,
                    call_data,
                }
            })
            .collect::<Vec<_>>();

        let results = multicall
            .aggregate_3(calls)
            .call()
            .await
            .map_err(|e| {
                warn!("[UNISWAP_EVENTS_TICKS_MULTICALL] Ошибка мультиколла тиков для пула {:?}: {:?}", pool_address, e);
                e
            })
            .unwrap_or_default();

        debug!("[UNISWAP_EVENTS_TICKS_MULTICALL_DEBUG][{:?}] Успешный мультиколл, получено {} результатов", pool_address, results.len());

        tick_indices
            .into_iter()
            .zip(results)
            .map(|(tick, result)| {
                if result.success {
                    match ethers::abi::decode(
                        &[
                            ethers::abi::ParamType::Uint(128), // liquidityGross
                            ethers::abi::ParamType::Int(128),  // liquidityNet
                            ethers::abi::ParamType::Uint(256), // feeGrowthOutside0X128
                            ethers::abi::ParamType::Uint(256), // feeGrowthOutside1X128
                            ethers::abi::ParamType::Int(56),   // tickCumulativeOutside
                            ethers::abi::ParamType::Uint(160), // secondsPerLiquidityOutsideX128
                            ethers::abi::ParamType::Uint(32),  // secondsOutside
                            ethers::abi::ParamType::Bool,      // initialized
                        ],
                        &result.return_data,
                    ) {
                        Ok(decoded) => {
                            let data = (
                                decoded[0].clone().into_uint().unwrap().try_into().unwrap(),
                                decoded[1].clone().into_int().unwrap().try_into().unwrap(),
                                decoded[2].clone().into_uint().unwrap(),
                                decoded[3].clone().into_uint().unwrap(),
                                decoded[4].clone().into_int().unwrap().try_into().unwrap(),
                                decoded[5].clone().into_uint().unwrap().try_into().unwrap(),
                                decoded[6].clone().into_uint().unwrap().try_into().unwrap(),
                                decoded[7].clone().into_bool().unwrap(),
                            );
                            debug!("[UNISWAP_EVENTS_TICKS_MULTICALL_DEBUG][{:?}] Тик {} успешно декодирован", pool_address, tick);
                            Some((tick, data))
                        }
                        Err(e) => {
                            warn!("[UNISWAP_EVENTS_TICKS_MULTICALL][{:?}] Ошибка декодирования тика {}: {:?}", pool_address, tick, e);
                            None
                        }
                    }
                } else {
                    warn!("[UNISWAP_EVENTS_TICKS_MULTICALL][{:?}] Неудачный вызов для тика {}", pool_address, tick);
                    None
                }
            })
            .collect()
    }


    /// Функция для обновления графа на основе событий
    pub async fn update_graph_from_event(
        &self,
        pool_event_info: &PoolEventInfo,
        graph: Arc<UniversalGraph>,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {

        debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Начало update_graph_from_event", pool_address);

        // Загружаем свежие данные из Uniswap V3
        let pool_update = self
            .fetch_tick_data(
                pool_event_info,
                pool_address,
                provider.clone(),
                graph.clone(),
            )
            .await?;

    debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Данные тиков получены: liquidity: {}, current_tick: {}", pool_address, pool_update.liquidity, pool_update.current_tick);
    debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Попытка обновления пула в графе", pool_address);

        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
    debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Пул найден в графе, обновление данных", pool_address);
            // Обновление данных
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;
            pool.uniswap_current_price = pool_update.current_price;

    debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Объединение tick_map", pool_address);
            // Tick map объединяется
            pool.tick_map = pool.tick_map.clone().union(pool_update.tick_map);

    debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Объединение завершено", pool_address);
        
        } else {
            // Пул не найден в графе — предупреждение
            warn!(
                "[ UNISWAP_EVENTS_UPDATE_GRAPH_WARN! ] Пул {:?} не найден в графе. Обновление пропущено.",
                pool_address
            );
        }

        debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Конец update_graph_from_event", pool_address);
    Ok(())

    }


    /// Метод для отслеживания событий в пулах Uniswap
    /// 
    /// # Аргументы
    /// * `block_receiver` - Приемник для получения номеров новых блоков
    /// * `event_tx` - Канал для отправки обнаруженных событий пула
    /// 
    /// # Возвращаемое значение
    /// * `anyhow::Result<()>` - Результат выполнения операции
pub async fn polling_event(
    &self,
    block_receiver: &watch::Receiver<u64>,
    graph: Arc<UniversalGraph>,
    event_tx: mpsc::Sender<PoolEventInfo>, // Изменено на mpsc::Sender
) -> anyhow::Result<()> {
    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Начало polling_event");
    let mut block_from = *block_receiver.borrow();
    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Начальный блок: {}", block_from);
    let max_chunk_size: u64 = 200;
    let mut block_receiver = block_receiver.clone();

    loop {
        //debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Ожидание изменения номера блока");
        if block_receiver.changed().await.is_err() {
            warn!("[ UNISWAP_EVENT_POLLING_WARN! ] Канал блоков закрыт");
            debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Канал блоков закрыт");
            break;
        }

        let block_to = *block_receiver.borrow();
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Новый блок: {}", block_to);
        if block_to < block_from {
            warn!(
                "[ UNISWAP_EVENT_POLLING_WARN! ] Некорректный диапазон: from {} > to {}",
                block_from, block_to
            );
            continue;
        }

        let subscribed_pools = self.subscribed_pools.clone();
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Количество подписанных пулов: {}", subscribed_pools.len());
        if subscribed_pools.is_empty() {
            block_from = block_to + 1;
            debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Пустой список подписанных пулов, block_from обновлен: {}", block_from);
            continue;
        }

        let mut current_from = block_from;
        let mut all_events = Vec::new();

        while current_from <= block_to {
            let current_to = (current_from + max_chunk_size - 1).min(block_to);
            debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Обработка диапазона блоков: {}–{}", current_from, current_to);
            match self.fetch_events(current_from, current_to).await {
                Ok(events) => {
                    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Получено {} событий для блоков {}–{}", events.len(), current_from, current_to);
                    all_events.extend(events);
                }
                Err(e) => {
                    warn!(
                        "[ UNISWAP_EVENT_POLLING_WARN ] Ошибка получения событий за блоки {}–{}: {}",
                        current_from, current_to, e
                    );
                    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Ошибка получения событий: {:?}", e);
                }
            }
            current_from = current_to + 1;
        }
       
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Агрегация событий, всего: {}", all_events.len());
        let aggregated_events = self.aggregate_events(all_events, graph.clone());
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Агрегировано {} событий", aggregated_events.len());

        for pool_event in aggregated_events {
            debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ][{:?}] Отправка события в mpsc-канал", pool_event.address);
            if let Err(e) = event_tx.send(pool_event).await {
                error!("[ UNISWAP_EVENT_POLLING_ERROR ] Ошибка отправки в mpsc-канал: {}", e);
            }
        }

        block_from = block_to + 1;
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] block_from обновлен: {}", block_from);
        sleep(Duration::from_secs(1));
    }
    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Конец polling_event");
    Ok(())
}



fn aggregate_events(
    &self,
    events: Vec<PoolEventInfo>,
    graph: Arc<UniversalGraph>)
        -> Vec<PoolEventInfo> {
        
    let mut map: HashMap<Address, PoolEventInfo> = HashMap::new();
    
    for event in events {
        let entry = map.entry(event.address).or_insert_with(|| {
            PoolEventInfo {
                event_id: event.event_id,
                address: event.address,
                tick_updates: DashSet::new(),
                current_tick: event.current_tick,
                block_number: event.block_number,
            }
        });
        
        let tick_spacing = graph
        .edges
        .get(&event.address)
        .map(|pool| match pool.uniswap_fee_tier {
            100 => 1,
            500 => 10,
            3000 => 60,
            10_000 => 200,
            _ => 60,
        })
        .unwrap_or(60);
    
    if event.block_number >= entry.block_number {
        entry.current_tick = event.current_tick;
        entry.block_number = event.block_number;
    }
    
    for tick in event.tick_updates.iter() {
        if *tick % tick_spacing == 0 {
            debug!("[ UNISWAP_EVENTS_AGGREGATE_DEBUG ][{:?}] Добавление тика {} в tick_updates", event.address, *tick);
            entry.tick_updates.insert(*tick);
        } else {
            info!(
                "[ UNISWAP_AGGREGATE_EVENT ][{:?}] Пропущен тик {} в агрегации (не кратен tick_spacing: {})",
                event.address, *tick, tick_spacing
            );
        }
    }
}

let result = map.into_values().collect();
result
}



async fn worker_loop(
    self: Arc<Self>,
    mut rx: mpsc::Receiver<PoolEventInfo>,
    graph: Arc<UniversalGraph>,
    provider: Arc<Provider<Ws>>,
    worker_id: usize,
    simulator_tx: mpsc::Sender<PoolEventInfo>,
) {
    debug!("[ UNISWAP_EVENTS_DEBUG ][ WORKER {}] Начало worker_loop", worker_id);
    while let Some(event) = rx.recv().await {
        let pool_address = event.address;
        debug!("[ UNISWAP_EVENTS_DEBUG ][ WORKER {}][{:?}] Получено событие", worker_id, pool_address);
        if let Err(e) = self
            .update_graph_from_event(&event, graph.clone(), pool_address, provider.clone())
            .await
        {
            error!(
                "[ WORKER_ERROR {}] Ошибка обновления пула {:?}: {:?}",
                worker_id, pool_address, e
            );
            debug!("[ UNISWAP_EVENTS_DEBUG ][ WORKER {}][{:?}] Ошибка обновления: {:?}", worker_id, pool_address, e);
        } else {
            info!(
                "[ {} {} ] Обновил пул {:?}",
                "WORKER_UNISWAP_EVENT".black().on_green(),
                worker_id,
                pool_address
            );
            debug!("[ UNISWAP_EVENTS_DEBUG ][ WORKER {}][{:?}] Пул успешно обновлен", worker_id, pool_address);
            if let Err(e) = simulator_tx.send(event).await {
                error!("[WORKER_ERROR {}] Ошибка отправки в симулятор: {}", worker_id, e);
            }
        }
    }
    warn!("[ WORKER {}] Завершён", worker_id);
    debug!("[ UNISWAP_EVENTS_DEBUG ][ WORKER {}] Конец worker_loop", worker_id);
}

pub async fn start_coordinator_and_workers(
    self: Arc<Self>,
    graph: Arc<UniversalGraph>,
    provider: Arc<Provider<Ws>>,
    num_workers: usize,
    mut event_rx: mpsc::Receiver<PoolEventInfo>,
    simulator_tx: mpsc::Sender<PoolEventInfo>,
) {
    debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Начало start_coordinator_and_workers, num_workers: {}", num_workers);

    // Создаём каналы для каждого воркера
    let mut worker_senders = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let (worker_tx, worker_rx) = mpsc::channel::<PoolEventInfo>(2048);
        worker_senders.push(worker_tx);

        // Запускаем воркер
        let subscriber_clone = Arc::clone(&self);
        let graph_clone = Arc::clone(&graph);
        let provider_clone = Arc::clone(&provider);
        let simulator_tx_clone = simulator_tx.clone();

        tokio::spawn(async move {
            subscriber_clone
                .worker_loop(worker_rx, graph_clone, provider_clone, i, simulator_tx_clone)
                .await;
        });
    }

    // Запускаем координатор
    let mut worker_index = 0;
    while let Some(event) = event_rx.recv().await {
        debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Получено событие для пула {:?}", event.address);
        let sender = &worker_senders[worker_index];
        if let Err(e) = sender.send(event).await {
            error!("[ COORDINATOR_ERROR ] Ошибка отправки события воркеру {}: {}", worker_index, e);
        } else {
            debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Событие отправлено воркеру {}", worker_index);
        }
        worker_index = (worker_index + 1) % num_workers;
    }

    // Дропаем Sender-ы, чтобы воркеры завершились после обработки оставшихся событий
    drop(worker_senders);
    debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Координатор завершён");
}


}



    






