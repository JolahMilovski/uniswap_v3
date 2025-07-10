use std::{
    collections::HashMap, sync::{
        atomic::{AtomicU64, Ordering}, Arc
    }, thread::sleep, time::{Duration, Instant}
};

use anyhow::Context;
use colored::Colorize;
use dashmap::DashSet;
use ethers::contract::EthEvent;
use ethers::{abi::RawLog, utils::keccak256};
use ethers::{
    contract::EthLogDecode,
    providers::{Http, Middleware, Provider, Ws},
    types::{Address, BlockNumber, Filter, H256, I256, U256},
};
use ethers_contract::abigen;
use futures::StreamExt;
use im::OrdMap;
use tracing::{debug, error, info,warn};
use tokio::{sync::{
         mpsc::{
            self, Receiver, Sender}, watch
    }, task::JoinHandle};

use crate::{
    uniswap_graph::UniversalGraph, uniswap_v3::{
        process_pool_data, UniswapV3Pool}
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
    pub liquidity: U256,
    pub sqrt_price_x96: U256,
    pub current_tick: i32,
    pub tick_map: OrdMap<i32, (i128, U256)>,
  //  pub current_price: U256,
}

#[derive(Debug, Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Http>>,
    pub subscribed_pools: DashSet<Address>,
    last_processed_block: Arc<AtomicU64>,
    event_counter: Arc<AtomicU64>,
}
#[derive(Debug, Clone)]
pub struct PoolEventInfo {
    pub event_id: u64,
    pub address: Address,
    pub tick_updates: DashSet<i32>,
    pub current_tick: i32,
    pub block_number: u64,
}



impl UniswapEventSubscriber {

    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        let subscriber = Self {
            provider,
            subscribed_pools: DashSet::new(),
            last_processed_block: Arc::new(AtomicU64::new(0)),
            event_counter: Arc::new(AtomicU64::new(0)),
        };
        info!("[ UNISWAP_EVENTS_DEBUG_NEW ] Экземпляр UniswapEventSubscriber создан");
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
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);
        let mut last_sent_block: u64 = 0;
        loop {
            match provider_ws.subscribe_blocks().await {
                Ok(mut stream) => {
                    debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Успешная подписка на поток блоков");
                    while let Some(block) = stream.next().await {
                        if let Some(number) = block.number {
                            let n = number.as_u64();
                            if n != last_sent_block {
                                //debug!("[ UNISWAP_EVENTS_BLOCKS_DEBUG ] Отправка блока {} в канал", n);
                                last_sent_block = n;
                                let _ = block_sender.send(n);
                            }
                            if n % 100 == 0 {
                                debug!("[ UNISWAP_EVENTS_BLOCKS ] Новый блок: {}", n);
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
        //debug!("[ UNISWAP_EVENTS_GET_TOPIC_DEBUG ] Получение топиков событий");
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
        //debug!("[ UNISWAP_EVENTS_GET_TOPIC_DEBUG ] Возвращено {} топиков", topics.len());
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
    &self, // Ссылка на текущий экземпляр структуры
    from_block: u64, // Начальный номер блока для запроса событий
    to_block: u64, // Конечный номер блока для запроса событий
) -> anyhow::Result<Vec<PoolEventInfo>> {
    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Начало fetch_events, from_block: {}, to_block: {}", from_block, to_block);
    
    // Проверяем корректность диапазона блоков
    if from_block > to_block {
        warn!("[ UNISWAP_FETCH_EVENT_WARN! ] Ошибка: from_block ({}) больше to_block ({})", from_block, to_block);
        return Ok(vec![]);
    }

    // Получаем список адресов подписанных пулов
    let subscribed_pool_addresses: Vec<Address> = self.subscribed_pools.iter().map(|entry| *entry.key()).collect();
    if subscribed_pool_addresses.is_empty() {
        debug!("[ UNISWAP_FETCH_EVENT ] Нет подписанных пулов");

        return Ok(vec![]); // Возвращаем пустой вектор, если нет подписанных пулов
    }

    // Создаем фильтр для получения логов событий
    let filter = Filter::new()
        .from_block(BlockNumber::Number(from_block.into()))
        .to_block(BlockNumber::Number(to_block.into()))
        .address(subscribed_pool_addresses)
        .topic0(Self::get_event_topics());

    // Получаем логи событий из блокчейна
    let logs = match self.provider.get_logs(&filter).await {
        Ok(logs) => logs,
        Err(e) => {
            warn!("[ UNISWAP_EVENT_WARN! ] Ошибка RPC: {}", e);
            Vec::new() // В случае ошибки возвращаем пустой вектор
        }
    };

    // Инициализируем структуры для хранения данных
    let mut event_map = HashMap::new(); // Карта для хранения событий по адресам пулов
    let mut swap_count = 0; // Счетчик событий обмена токенов
    let mut mint_count = 0; // Счетчик событий добавления ликвидности
    let mut burn_count = 0; // Счетчик событий удаления ликвидности
    let mut flash_count = 0; // Счетчик событий флэш

    // Обрабатываем каждый лог события
    for log in logs {
        let address = log.address; // Адрес контракта пула
        let block_number = log.block_number.map_or("неизвестен".to_string(), |n| n.as_u64().to_string()); // Номер блока
        let event_id = self.event_counter.fetch_add(1, Ordering::SeqCst); // Генерируем уникальный ID события
        
        // Логируем информацию о событии
        debug!("[DEBUG_EVENT_ID {}] Fetching event for pool: {}, block: {}", event_id, address, block_number);
        info!("[ UNISWAP_EVENTS_COUNTER_DEBUG ] Обработано событий: {}", event_id + 1);

        // Получаем или создаем запись для пула в карте событий
        let entry = event_map.entry(address).or_insert_with(|| {
            let block_number_u64 = log.block_number.map(|n| n.as_u64()).unwrap_or(0);
            PoolEventInfo {
                address,
                tick_updates: DashSet::new(), // Множество обновленных тиков
                current_tick: 0, // Текущий тик
                block_number: block_number_u64, // Номер блока
                event_id, // ID события
            }
        });

        // Определяем тип события по первому топику и обрабатываем соответственно
        match log.topics.first() {
            // Обработка события Swap (обмен токенов)
            Some(topic) if *topic == SwapEvent::signature() => {
                let raw_log = RawLog { topics: log.topics.clone(), data: log.data.to_vec() };
                match <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(swap) => {
                        info!("[ UNISWAP_FETCH_EVENT ] Swap: пул {:?}, блок {}, amount0: {}, amount1: {}, sqrtPriceX96: {}, ликвидность: {}, тик: {}, event_id: {}",
                            address, block_number, swap.amount0, swap.amount1, swap.sqrt_price_x96, swap.liquidity, swap.tick, event_id);
                        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Swap обработан, current_tick: {}, event_id: {}", address, swap.tick, event_id);
                        entry.current_tick = swap.tick;
                        swap_count += 1;
                    }
                    Err(e) => warn!("[ UNISWAP_FETCH_EVENTS_WARN! ][{:?}] Ошибка декодирования Swap: {:?}, event_id: {}", address, e, event_id),
                }
            }
            // Обработка события Mint (добавление ликвидности)
            Some(topic) if *topic == MintEvent::signature() => {
                let raw_log = RawLog { topics: log.topics.clone(), data: log.data.to_vec() };
                match <MintEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(mint) => {
                        info!("[ UNISWAP_FETCH_EVENT ] Mint: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}, event_id: {}",
                            address, block_number, mint.tick_lower, mint.tick_upper, mint.liquidity, mint.amount0, mint.amount1, event_id);
                        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Mint обработан, tick_lower: {}, tick_upper: {}, event_id: {}", address, mint.tick_lower, mint.tick_upper, event_id);
                        entry.tick_updates.extend([mint.tick_lower, mint.tick_upper]);
                        mint_count += 1;
                    }
                    Err(e) => warn!("[ UNISWAP_FETCH_EVENTS_WARN! ][{:?}] Ошибка декодирования Mint: {:?}, event_id: {}", address, e, event_id),
                }
            }
            // Обработка события Burn (удаление ликвидности)
            Some(topic) if *topic == BurnEvent::signature() => {
                let raw_log = RawLog { topics: log.topics.clone(), data: log.data.to_vec() };
                match <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(burn) => {
                        info!("[ UNISWAP_FETCH_EVENT ] Burn: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}, event_id: {}",
                            address, block_number, burn.tick_lower, burn.tick_upper, burn.liquidity, burn.amount0, burn.amount1, event_id);
                        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Burn обработан, tick_lower: {}, tick_upper: {}, event_id: {}", address, burn.tick_lower, burn.tick_upper, event_id);
                        entry.tick_updates.extend([burn.tick_lower, burn.tick_upper]);
                        burn_count += 1;
                    }
                    Err(e) => warn!("[ UNISWAP_FETCH_EVENTS_WARN! ][{:?}] Ошибка декодирования Burn: {:?}, event_id: {}", address, e, event_id),
                }
            }
            // Обработка события Flash (флэш-займ)
            Some(topic) if *topic == FlashEvent::signature() => {
                let raw_log = RawLog { topics: log.topics.clone(), data: log.data.to_vec() };
                match <FlashEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(flash) => {
                        info!("[ UNISWAP_FETCH_EVENT ] Flash: пул {:?}, блок {}, заимствовано {} token0, {} token1, уплачено {} token0, {} token1, event_id: {}",
                            address, block_number, flash.amount0, flash.amount1, flash.paid0, flash.paid1, event_id);
                        debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Flash обработан, amount0: {}, amount1: {}, event_id: {}", address, flash.amount0, flash.amount1, event_id);
                        flash_count += 1;
                    }
                    Err(e) => warn!("[ UNISWAP_FETCH_EVENTS_DEBUG ][{:?}] Ошибка декодирования Flash: {:?}, event_id: {}", address, e, event_id),
                }
            }
            _ => debug!("[ UNISWAP_EVENTS_DEBUG ][{:?}] Неизвестный топик события, event_id: {}", address, event_id),
        }
    }

    // Обновляем номер последнего обработанного блока
    self.last_processed_block.store(to_block, Ordering::Release);
    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Обновлен last_processed_block: {}", to_block);

    // Если нет событий, возвращаем пустой вектор
    if event_map.is_empty() {
        return Ok(vec![]);
    }

    // Формируем итоговую статистику
    let pool_addresses: Vec<String> = event_map.keys().map(|addr| format!("{:?}", addr)).collect();
    let pools_str = format!("{} пулов", pool_addresses.len());
    info!("[{}][Блоки {}-{}] Обработано {} событий (Swap: {}, Mint: {}, Burn: {}, Flash: {}) для {}",
        " UNISWAP_FETCH_EVENT ".bright_blue(), from_block, to_block,
        swap_count + mint_count + burn_count + flash_count, swap_count, mint_count, burn_count, flash_count, pools_str);
    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Итог обработки: Swap: {}, Mint: {}, Burn: {}, Flash: {}", swap_count, mint_count, burn_count, flash_count);

    // Завершаем выполнение функции
    debug!("[ UNISWAP_FETCH_EVENTS_DEBUG ] Конец fetch_events, возвращено {} событий", event_map.len());
    Ok(event_map.into_values().collect()) // Возвращаем вектор с информацией о событиях
}


/// Получает данные о тиках пула Uniswap V3 с использованием индивидуальных запросов
///
/// # Описание
/// Запрашивает данные о ликвидности, текущей цене и тиках пула через прямые вызовы функции `ticks`
/// с кастомной логикой повторных попыток при ошибках. Обновляет `tick_map` на основе полученных событий.
///
/// # Параметры
/// * `pool_event_info` - Информация о событиях пула
/// * `pool_address` - Адрес пула в сети
/// * `provider` - WebSocket-провайдер для взаимодействия с блокчейном
///
/// # Возвращаемое значение
/// * `Result<EventPoolUpdate, anyhow::Error>` - Обновленные данные пула
pub async fn fetch_tick_data(
    &self,
    pool_event_info: &PoolEventInfo,
    pool_address: Address,
    provider: Arc<Provider<Ws>>,
) -> anyhow::Result<EventPoolUpdate> {
    debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] Начало получения данных для тиков пула", pool_address);

    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

    let (liquidity, slot0, tick_spacing, _, _) =
        process_pool_data(pool_address, pool_contract.clone().into())
            .await
            .context(format!("[ UNISWAP_EVENT_FETCH_TICK ] Не удалось получить данные пула: {:?}", pool_address))?;

    let current_tick = slot0.1;
    debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] Текущий тик: {}", pool_address, current_tick);

    let tick_indices: Vec<i32> = pool_event_info
        .tick_updates
        .iter()
        .map(|tick| *tick)
        .collect();

    if !tick_indices.is_empty() {
        debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] Запрашивается {} тиков: {:?}", pool_address, tick_indices.len(), tick_indices);
    }

    let mut tick_results = Vec::with_capacity(tick_indices.len());
    const MAX_RETRIES: u32 = 5;
    const RETRY_DELAY_MS: u64 = 30;

    for tick in tick_indices {
        let mut attempt = 0;
        let mut result = None;

        while attempt < MAX_RETRIES {
            debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Попытка {} для тика {}", pool_address, attempt + 1, tick);
            match pool_contract.ticks(tick).call().await {
                Ok(data) => {
                    debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Тик {} успешно получен: {:?}", pool_address, tick, data);
                    result = Some((tick, data));
                    break;
                }
                Err(e) => {
                    warn!("[ UNISWAP_EVENT_FETCH_TICK ][{:?}] Ошибка запроса тика {} на попытке {}: {:?}", pool_address, tick, attempt + 1, e);
                    attempt += 1;
                    if attempt < MAX_RETRIES {
                        debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] Ожидание {} мс перед следующей попыткой", pool_address, RETRY_DELAY_MS);
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                }
            }
        }

        if result.is_none() {
            warn!("[ UNISWAP_EVENT_FETCH_TICK ][{:?}] Все попытки запроса тика {} провалились", pool_address, tick);
        }
        tick_results.push(result);
    }

    if !tick_results.is_empty() {
        debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] Получено {} результатов тиков", pool_address, tick_results.len());
    }

    let mut tick_map: OrdMap<i32, (i128, U256)> = OrdMap::new();
    for result in tick_results {
        if let Some((tick, data)) = result {
            if (data.0 != 0 || data.1 != 0) && tick % tick_spacing == 0 {
                debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Добавление тика {} в tick_map", pool_address, tick);
                tick_map.insert(tick, (data.1, U256::from(data.0)));
            } else {
                info!(
                    "[ UNISWAP_EVENT_FETCH_TICK ][{:?}] Пропущен тик {} (нулевая ликвидность: gross: {}, net: {} или не кратен tick_spacing: {})",
                    pool_address, tick, data.0, data.1, tick_spacing
                );
            }
        }
    }

    if !tick_map.is_empty() {
        debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ] [{:?}] tick_map заполнен: {} тиков", pool_address, tick_map.len());
    }

    let sqrt_price_x96 = U256::from(slot0.0);
    debug!("[ UNISWAP_EVENT_FETCH_TICK_DEBUG ][{:?}] sqrt_price: {}", pool_address, sqrt_price_x96);

    Ok(EventPoolUpdate {
        liquidity,
        sqrt_price_x96: slot0.0,
        current_tick,
        tick_map,
    })
}


/// Функция для обновления графа на основе событий
    pub async fn update_graph_from_event(
        &self,
        pool_event_info: &PoolEventInfo,
        graph: Arc<UniversalGraph>,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Начало update_graph_from_event, event_id: {}", pool_address, pool_event_info.event_id);

        let pool_update = self
            .fetch_tick_data(pool_event_info, pool_address, provider.clone())
            .await?;

        debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Данные тиков получены: liquidity: {}, current_tick: {}, event_id: {}", pool_address, pool_update.liquidity, pool_update.current_tick, pool_event_info.event_id);

        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
            debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Пул найден в графе, обновление данных, event_id: {}", pool_address, pool_event_info.event_id);
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;

            debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Попытка объединения tick_map, event_id: {}", pool_address, pool_event_info.event_id);
            pool.tick_map = pool.tick_map.clone().union(pool_update.tick_map);

            debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Объединение tick_map завершено, event_id: {}", pool_address, pool_event_info.event_id);
        } else {
            warn!(
                "[ UNISWAP_EVENTS_UPDATE_GRAPH_WARN! ] Пул {:?} не найден в графе. Обновление пропущено, event_id: {}",
                pool_address, pool_event_info.event_id
            );
        }

        debug!("[ UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG ][{:?}] Конец update_graph_from_event, event_id: {}", pool_address, pool_event_info.event_id);
        Ok(())
    }

    

/// Отслеживает события в пулах Uniswap с автоматическим перезапуском в случае ошибок.
/// 
/// # Описание
/// Функция реализует защищённый цикл обработки событий, который продолжает работу с последнего обработанного блока,
/// перезапуская MPSC-канал при каждой итерации. Логирует последний обработанный блок для возобновления после сбоев.
/// 
/// # Аргументы
/// * `block_receiver` - Приемник для получения номеров новых блоков через канал watch.
/// * `graph` - Граф пулов Uniswap, содержащий информацию о пулах.
/// * `event_tx` - Канал для отправки обнаруженных событий пула в симулятор.
/// * `provider_ws` - WebSocket-провайдер для взаимодействия с блокчейном.
/// 
/// # Возвращаемое значение
/// * `anyhow::Result<()>` - Результат выполнения операции. Функция не завершается в нормальном режиме,
///   так как работает в бесконечном цикле с автоматическим перезапуском.
pub async fn polling_event(
    &self,
    block_receiver: &watch::Receiver<u64>,
    graph: Arc<UniversalGraph>,
    event_tx: mpsc::Sender<PoolEventInfo>,
    provider_ws: Arc<Provider<Ws>>,
) -> anyhow::Result<()> {
    const MAX_RETRIES: u32 = 5;
    const RETRY_DELAY_MS: u64 = 10;
    const CHANNEL_CAPACITY: usize = 4096;

    info!("[ UNISWAP_EVENTS_POLLING ] Начало polling_event");
    let mut block_from = self.last_processed_block.load(Ordering::Acquire);
    debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Начальный блок: {}", block_from);
    let max_chunk_size: u64 = 200;

    loop {
        let mut block_receiver = block_receiver.clone();
        let (new_event_tx, mut event_rx) = mpsc::channel::<PoolEventInfo>(CHANNEL_CAPACITY);

        // Клонируем provider_ws и event_tx перед передачей в замыкание
        let provider_ws_clone = Arc::clone(&provider_ws);
        let event_tx_clone = event_tx.clone();
        let event_task = tokio::spawn({
            let graph = Arc::clone(&graph);
            let subscriber = self.clone();
            async move {
                while let Some(event) = event_rx.recv().await {
                    if let Err(e) = subscriber
                        .update_graph_from_event(&event, Arc::clone(&graph), event.address, Arc::clone(&provider_ws_clone))
                        .await
                    {
                        error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Ошибка обработки события с ID {}: {}", event.event_id, e);
                    }
                    // Клонируем event перед отправкой
                    if let Err(e) = event_tx_clone.send(event.clone()).await {
                        error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Ошибка отправки события с ID {} в канал: {}", event.event_id, e);
                    }
                }
            }
        });

        let mut attempt = 0;
        while attempt < MAX_RETRIES {
            match self.process_block_range(&mut block_receiver, block_from, max_chunk_size, &new_event_tx, &graph).await {
                Ok((new_block_from, events)) => {
                    block_from = new_block_from;
                    self.last_processed_block.store(block_from, Ordering::Release);

                    if block_from % 100 == 0 {
                        info!("[ UNISWAP_EVENTS_POLLING ] Обработан последний блок: {}", block_from);
                    }

                    if !events.is_empty() {
                        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Получено {} событий", events.len());
                    }
                    
                    attempt = 0;
                }
                Err(e) => {
                    error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Ошибка обработки блоков: {}. Попытка {}/{}", e, attempt + 1, MAX_RETRIES);
                    attempt += 1;
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    } else {
                        error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Исчерпаны все попытки обработки блоков");
                        break;
                    }
                }
            }
        }

        if attempt >= MAX_RETRIES {
            error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Максимальное количество попыток исчерпано, перезапуск цикла");
        }

        event_task.abort();
        info!("[ UNISWAP_EVENTS_POLLING ] Перезапуск polling_event с последнего блока: {}", block_from);
        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
    }
}

/// Обрабатывает диапазон блоков, получая и агрегируя события пулов Uniswap.
/// 
/// # Описание
/// Функция извлекает события из указанного диапазона блоков, агрегирует их и отправляет в канал.
/// Используется в `polling_event` для обработки чанков блоков с учётом максимального размера.
/// 
/// # Аргументы
/// * `block_receiver` - Приемник для получения номеров новых блоков.
/// * `block_from` - Начальный блок для обработки.
/// * `max_chunk_size` - Максимальный размер чанка блоков для обработки за раз.
/// * `event_tx` - Канал для отправки агрегированных событий.
/// * `graph` - Граф пулов Uniswap для фильтрации тиков по `tick_spacing`.
/// 
/// # Возвращаемое значение
/// * `anyhow::Result<(u64, Vec<PoolEventInfo>)>` - Кортеж, содержащий следующий начальный блок
///   и вектор агрегированных событий. При ошибке (например, закрытие канала) возвращает `Err`.
/// 
/// # Детали реализации
/// * Проверяет изменение в канале блоков и валидирует диапазон блоков.
/// * Пропускает итерацию, если нет подписанных пулов.
/// * Обрабатывает блоки чанками, вызывая `fetch_events` для каждого диапазона.
/// * Агрегирует события через `aggregate_events` и отправляет их в `event_tx`.
/// * Возвращает обновлённый `block_from` и список событий.
async fn process_block_range(
    &self,
    block_receiver: &mut watch::Receiver<u64>,
    block_from: u64,
    max_chunk_size: u64,
    event_tx: &mpsc::Sender<PoolEventInfo>,
    graph: &Arc<UniversalGraph>,
) -> anyhow::Result<(u64, Vec<PoolEventInfo>)> {
    // Ожидаем изменения в канале блоков
    if block_receiver.changed().await.is_err() {
        error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Канал блоков закрыт");
        return Err(anyhow::anyhow!("Канал блоков закрыт"));
    }

    // Получаем конечный блок из канала
    let block_to = *block_receiver.borrow();
    // Проверяем корректность диапазона блоков
    if block_to < block_from {
        info!(
            "[ UNISWAP_EVENTS_POLLING ] Некорректный диапазон: from {} > to {}",
            block_from, block_to
        );
        return Ok((block_from, vec![]));
    }

    // Проверяем наличие подписанных пулов
    let subscribed_pools = self.subscribed_pools.clone();
    if subscribed_pools.is_empty() {
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Нет подписанных пулов");
        return Ok((block_to + 1, vec![]));
    }

    // Инициализируем переменные для обработки чанков
    let mut current_from = block_from;
    let mut all_events = Vec::new();

    // Обрабатываем блоки чанками
    while current_from <= block_to {
        let current_to = (current_from + max_chunk_size - 1).min(block_to);
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Обработка диапазона блоков: {}–{}", current_from, current_to);
        
        // Получаем события для текущего чанка
        let events = self.fetch_events(current_from, current_to).await?;
        if !events.is_empty() {
            debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Получено {} событий для блоков {}–{}", events.len(), current_from, current_to);
        }
        all_events.extend(events);
        current_from = current_to + 1;
    }

    // Агрегируем события
    let aggregated_events = self.aggregate_events(all_events, Arc::clone(graph));
    // Отправляем агрегированные события в канал
    for pool_event in &aggregated_events {
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ][{:?}] Отправка события с ID {} в mpsc-канал", pool_event.address, pool_event.event_id);
        if let Err(e) = event_tx.send(pool_event.clone()).await {
            error!("[ UNISWAP_EVENTS_POLLING_ERROR ] Ошибка отправки события с ID {} в mpsc-канал: {}", pool_event.event_id, e);
        }
    }

    // Возвращаем следующий начальный блок и агрегированные события
    Ok((block_to + 1, aggregated_events))
}


/// Агрегирует события пулов Uniswap, объединяя события для одного и того же пула
/// и фильтруя тики в соответствии с tick_spacing пула
///
/// # Аргументы
/// * `events` - Вектор событий пулов для агрегации
/// * `graph` - Граф пулов Uniswap, содержащий информацию о fee_tier и tick_spacing
///
/// # Детали работы
/// * Создает HashMap для группировки событий по адресу пула
/// * Для каждого пула сохраняет самое последнее состояние (current_tick и block_number)
/// * Фильтрует тики, оставляя только те, которые кратны tick_spacing пула
/// * tick_spacing определяется на основе fee_tier пула:
///   - 100 -> spacing 1
///   - 500 -> spacing 10
///   - 3000 -> spacing 60
///   - 10000 -> spacing 200
///   - по умолчанию -> spacing 60
///
/// # Возвращает
/// * Вектор агрегированных событий пулов, где каждый пул представлен одним событием
/// с актуальным состоянием и отфильтрованными тиками
fn aggregate_events(
    &self,
    events: Vec<PoolEventInfo>,
    graph: Arc<UniversalGraph>,
) -> Vec<PoolEventInfo> {
    if !events.is_empty() {
        debug!("[ UNISWAP_EVENTS_AGGREGATE_DEBUG ] Начало агрегации {} событий", events.len());
    }
    let mut map: HashMap<Address, PoolEventInfo> = HashMap::new();

    for event in events {
        debug!("[ UNISWAP_EVENTS_AGGREGATE_DEBUG ][{:?}] Обработка события с ID {}", event.address, event.event_id);
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
            entry.event_id = event.event_id;
        }

        for tick in event.tick_updates.iter() {
            if *tick % tick_spacing == 0 {
                debug!("[ UNISWAP_EVENTS_AGGREGATE_DEBUG ][{:?}] Добавление тика {} в tick_updates для события с ID {}", event.address, *tick, event.event_id);
                entry.tick_updates.insert(*tick);
            } else {
                info!("[ UNISWAP_AGGREGATE_EVENT ][{:?}] Пропущен тик {} в агрегации (не кратен tick_spacing: {}) для события с ID {}",
                    event.address, *tick, tick_spacing, event.event_id);
            }
        }
    }

    let result: Vec<PoolEventInfo> = map.into_values().collect();
    if !result.is_empty() {
        debug!("[ UNISWAP_EVENTS_POLLING_DEBUG ] Агрегировано {} событий, last_event_id: {}", result.len(), result.last().map(|e| e.event_id).unwrap_or(0));
    }
    result
}




/// Запускает нового воркера и возвращает его канал и дескриптор
///
/// # Аргументы
/// * `worker_id` - Идентификатор воркера
/// * `graph` - Граф пулов Uniswap
/// * `provider` - WebSocket-провайдер
/// * `simulator_tx` - Канал для передачи событий в симулятор
/// * `subscriber` - Экземпляр UniswapEventSubscriber
///
/// # Возвращает
/// * `(Sender<PoolEventInfo>, JoinHandle<()>)` - Канал отправки событий и дескриптор задачи воркера
    async fn spawn_worker(
        worker_id: usize,
        graph: Arc<UniversalGraph>,
        provider: Arc<Provider<Ws>>,
        simulator_tx: Sender<PoolEventInfo>,
        subscriber: Arc<UniswapEventSubscriber>,
    ) -> (Sender<PoolEventInfo>, JoinHandle<()>) {
        info!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Запуск воркера {}", worker_id);
        let (tx, rx) = mpsc::channel::<PoolEventInfo>(2048);
        let handle = tokio::spawn(Self::worker_loop(
            worker_id,
            rx,
            graph,
            provider,
            simulator_tx,
            subscriber,
        ));
        (tx, handle)
    }




/// Запускает координатор и воркеры для обработки событий Uniswap
/// # Arguments
/// * `graph` - Граф пулов Uniswap
/// * `provider` - WebSocket-провайдер для взаимодействия с блокчейном
/// * `num_workers` - Количество воркеров
/// * `event_rx` - Канал для получения событий
/// * `simulator_tx` - Канал для передачи событий в симулятор
/// * `subscriber` - Экземпляр UniswapEventSubscriber
/// # Returns
/// * `JoinHandle<()>` - Дескриптор задачи координатора
/// Запускает координатор и воркеры для обработки событий Uniswap
/// Проверяет состояние воркеров каждые 60 секунд и перезапускает при необходимости
pub async fn start_coordinator_and_workers(
        graph: Arc<UniversalGraph>,
        provider: Arc<Provider<Ws>>,
        num_workers: usize,
        mut event_rx: Receiver<PoolEventInfo>,
        simulator_tx: Sender<PoolEventInfo>,
        subscriber: Arc<UniswapEventSubscriber>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut worker_senders: Vec<Sender<PoolEventInfo>> = Vec::with_capacity(num_workers);
            let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_workers);

            for i in 0..num_workers {
                let (tx, handle) = Self::spawn_worker(
                    i,
                    Arc::clone(&graph),
                    Arc::clone(&provider),
                    simulator_tx.clone(),
                    Arc::clone(&subscriber),
                )
                .await;
                worker_senders.push(tx);
                worker_handles.push(handle);
            }

            let mut worker_index = 0;
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    Some(event) = event_rx.recv() => {
                        debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Получено событие с ID {} для пула {:?}", event.event_id, event.address);
                        let mut sent = false;
                        let mut attempts = 0;
                        let max_attempts = num_workers;

                        while attempts < max_attempts && !sent {
                            let sender = &worker_senders[worker_index];
                            if !sender.is_closed() {
                                match sender.send(event.clone()).await {
                                    Ok(_) => {
                                        debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Событие с ID {} отправлено воркеру {}", event.event_id, worker_index);
                                        sent = true;
                                    }
                                    Err(e) => {
                                        warn!("[ UNISWAP_EVENT_COORDINATOR_WARN ] Ошибка отправки события с ID {} воркеру {}, перезапуск: {}", event.event_id, worker_index, e);
                                        let (new_tx, new_handle) = Self::spawn_worker(
                                            worker_index,
                                            Arc::clone(&graph),
                                            Arc::clone(&provider),
                                            simulator_tx.clone(),
                                            Arc::clone(&subscriber),
                                        ).await;
                                        worker_senders[worker_index] = new_tx;
                                        worker_handles[worker_index] = new_handle;
                                    }
                                }
                            } else {
                                warn!("[ UNISWAP_EVENT_COORDINATOR_WARN ] Канал воркера {} закрыт, перезапуск для события с ID {}", worker_index, event.event_id);
                                let (new_tx, new_handle) = Self::spawn_worker(
                                    worker_index,
                                    Arc::clone(&graph),
                                    Arc::clone(&provider),
                                    simulator_tx.clone(),
                                    Arc::clone(&subscriber),
                                ).await;
                                worker_senders[worker_index] = new_tx;
                                worker_handles[worker_index] = new_handle;
                            }
                            worker_index = (worker_index + 1) % num_workers;
                            attempts += 1;
                        }

                        if !sent {
                            error!("[ UNISWAP_EVENT_COORDINATOR_ERROR ] Не удалось отправить событие с ID {}, все воркеры недоступны", event.event_id);
                        }
                    }
                    _ = interval.tick() => {
                        for (i, (sender, handle)) in worker_senders.iter_mut().zip(worker_handles.iter_mut()).enumerate() {
                            if sender.is_closed() || handle.is_finished() {
                                warn!("[ UNISWAP_EVENT_COORDINATOR_WARN ] Воркер {} не активен или завершен, перезапуск", i);
                                let (new_tx, new_handle) = Self::spawn_worker(
                                    i,
                                    Arc::clone(&graph),
                                    Arc::clone(&provider),
                                    simulator_tx.clone(),
                                    Arc::clone(&subscriber),
                                ).await;
                                *sender = new_tx;
                                *handle = new_handle;
                            }
                        }
                        debug!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Проверка состояния воркеров завершена");
                    }
                    else => {
                        warn!("[ UNISWAP_EVENT_COORDINATOR_WARN ] Канал событий закрыт, завершение координатора");
                        break;
                    }
                }
            }

            for handle in worker_handles {
                if let Err(e) = handle.await {
                    error!("[ UNISWAP_EVENT_COORDINATOR_ERROR ] Ошибка завершения воркера: {}", e);
                }
            }

            info!("[ UNISWAP_EVENTS_COORDINATOR_DEBUG ] Координатор завершил работу");
        })
    }

/// Цикл обработки событий для воркера
/// # Arguments
/// * `worker_id` - Идентификатор воркера
/// * `rx` - Канал для получения событий
/// * `graph` - Граф пулов Uniswap
/// * `provider` - WebSocket-провайдер
/// * `simulator_tx` - Канал для передачи событий в симулятор
async fn worker_loop(
    worker_id: usize,
    mut rx: Receiver<PoolEventInfo>,
    graph: Arc<UniversalGraph>,
    provider: Arc<Provider<Ws>>,
    simulator_tx: Sender<PoolEventInfo>,
    subscriber: Arc<UniswapEventSubscriber>,
) {
    debug!("[ UNISWAP_EVENTS_WORKER_DEBUG ] Воркер {} запущен", worker_id);
    loop {
        match rx.recv().await {
            Some(event) => {
                let start_time = Instant::now();
                debug!("[ UNISWAP_EVENTS_WORKER_DEBUG ] Воркер {} начал обработку события с ID {} для пула {:?}", worker_id, event.event_id, event.address);
                if let Err(e) = subscriber
                    .update_graph_from_event(&event, Arc::clone(&graph), event.address, Arc::clone(&provider))
                    .await
                {
                    error!("[ UNISWAP_EVENT_WORKER_ERROR ] Воркер {}: ошибка обработки события с ID {} для пула {:?}: {}", worker_id, event.event_id, event.address, e);
                    continue;
                }
                if let Err(e) = simulator_tx.send(event.clone()).await {
                    error!("[ UNISWAP_EVENT_WORKER_ERROR ] Воркер {}: ошибка отправки события с ID {} в симулятор: {}", worker_id, event.event_id, e);
                } else {
                    debug!("[ UNISWAP_EVENTS_WORKER_DEBUG ] Воркер {}: событие с ID {} успешно отправлено в симулятор", worker_id, event.event_id);
                }
                let duration = start_time.elapsed();
                info!("[ UNISWAP_EVENTS_WORKER_DEBUG ] Воркер {} обработал событие с ID {} для пула {:?} за {} мс", worker_id, event.event_id, event.address, duration.as_millis());
            }
            None => {
                error!("[ UNISWAP_EVENT_WORKER_ERROR ] Воркер {}: канал закрыт, завершение работы", worker_id);
                break;
            }
        }
    }
    error!("[ UNISWAP_EVENT_WORKER_ERROR ] Воркер {} завершил работу", worker_id);
}

}



    






