use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use arc_swap::ArcSwap;
use colored::Colorize;
use dashmap::{DashMap, DashSet};
use ethers::contract::EthEvent;
use ethers::{
    providers::{Middleware, Provider},
    types::{Address, BlockNumber, Filter, H256, I256, U256},
};
use ethers_contract::abigen;
use ethers_providers::{Http, Ws};
use futures::StreamExt;
use im::OrdMap;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::{
    sync::{broadcast, mpsc::Sender, watch, Notify},
    task::JoinHandle,
    time::interval,
};
use tracing::{debug, error, info, warn};

use crate::{
    uniswap_graph::UniversalGraph,
    uniswap_v3::{process_pool_data, UniswapV3Pool},
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
}

#[derive(Debug, Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Http>>,
    pub subscribed_pools: DashSet<Address>,
    last_processed_block: Arc<AtomicU64>,
    event_counter: Arc<AtomicU64>,
    pool_handlers: Arc<DashMap<Address, JoinHandle<()>>>,
    pool_notifiers: Arc<DashMap<Address, Arc<Notify>>>,
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
            pool_handlers: Arc::new(DashMap::new()),
            pool_notifiers: Arc::new(DashMap::new()),
        };
        info!("[UNISWAP_EVENTS_DEBUG_NEW] Экземпляр UniswapEventSubscriber создан");
        subscriber
    }

    pub async fn subscribe_to_new_blocks(
        provider_ws: &Arc<Provider<Ws>>,
        block_sender: watch::Sender<u64>,
    ) -> anyhow::Result<()> {
        debug!("[UNISWAP_EVENTS_DEBUG] Начало subscribe_to_new_blocks");
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);
        let mut last_sent_block: u64 = 0;
        loop {
            match provider_ws.subscribe_blocks().await {
                Ok(mut stream) => {
                    debug!("[UNISWAP_EVENTS_BLOCKS_DEBUG] Успешная подписка на поток блоков");
                    while let Some(block) = stream.next().await {
                        if let Some(number) = block.number {
                            let n = number.as_u64();
                            if n != last_sent_block {
                                last_sent_block = n;
                                let _ = block_sender.send(n);
                            }
                            if n % 100 == 0 {
                                debug!("[UNISWAP_EVENTS_BLOCKS] Новый блок: {}", n);
                            }
                        }
                    }
                    info!("[UNISWAP_EVENTS_BLOCKS] Поток блоков завершился. Переподключение...");
                }
                Err(e) => {
                    error!(
                        "[UNISWAP_EVENTS_BLOCKS_ERROR] Ошибка подписки: {e}. Переподключение..."
                    );
                }
            }
            debug!(
                "[UNISWAP_EVENTS_BLOCKS_DEBUG] Ожидание {} секунд перед переподключением",
                RECONNECT_DELAY.as_secs()
            );
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    fn get_event_topics() -> Vec<H256> {
        let topics = vec![
            H256::from_slice(&ethers::utils::keccak256(
                b"Swap(address,address,int256,int256,uint160,uint128,int24)",
            )),
            H256::from_slice(&ethers::utils::keccak256(
                b"Mint(address,address,int24,int24,uint128,uint256,uint256)",
            )),
            H256::from_slice(&ethers::utils::keccak256(
                b"Burn(address,int24,int24,uint128,uint256,uint256)",
            )),
            H256::from_slice(&ethers::utils::keccak256(
                b"Flash(address,address,uint256,uint256,uint256,uint256)",
            )),
        ];
        topics
    }

    pub async fn fetch_events(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<PoolEventInfo>> {
        debug!(
            "[UNISWAP_FETCH_EVENTS_DEBUG] Начало fetch_events, from_block: {}, to_block: {}",
            from_block, to_block
        );

        if from_block > to_block {
            warn!(
                "[UNISWAP_FETCH_EVENT_WARN] Ошибка: from_block ({}) больше to_block ({})",
                from_block, to_block
            );
            return Ok(vec![]);
        }

        let subscribed_pool_addresses: Vec<Address> = self
            .subscribed_pools
            .iter()
            .map(|entry| *entry.key())
            .collect();
        if subscribed_pool_addresses.is_empty() {
            debug!("[UNISWAP_FETCH_EVENT] Нет подписанных пулов");
            return Ok(vec![]);
        }

        let filter = Filter::new()
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .address(subscribed_pool_addresses)
            .topic0(Self::get_event_topics());

        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(e) => {
                warn!("[UNISWAP_EVENT_WARN] Ошибка RPC: {}", e);
                Vec::new()
            }
        };

        let mut event_map = HashMap::new();
        let mut swap_count = 0;
        let mut mint_count = 0;
        let mut burn_count = 0;
        let mut flash_count = 0;

        for log in logs {
            let address = log.address;
            let block_number = log.block_number.map_or("неизвестен".to_string(), |n| {
                n.as_u64().to_string()
            });
            let event_id = self.event_counter.fetch_add(1, Ordering::SeqCst);

            debug!(
                "[DEBUG_EVENT_ID {}] Fetching event for pool: {}, block: {}",
                event_id, address, block_number
            );
            info!(
                "[UNISWAP_EVENTS_COUNTER_DEBUG] Обработано событий: {}",
                event_id + 1
            );

            let entry = event_map.entry(address).or_insert_with(|| {
                let block_number_u64 = log.block_number.map(|n| n.as_u64()).unwrap_or(0);
                PoolEventInfo {
                    address,
                    tick_updates: DashSet::new(),
                    current_tick: 0,
                    block_number: block_number_u64,
                    event_id,
                }
            });

            match log.topics.first() {
                Some(topic) if *topic == SwapEvent::signature() => {
                    let raw_log = ethers::abi::RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <SwapEvent as ethers::contract::EthLogDecode>::decode_log(&raw_log) {
                        Ok(swap) => {
                            info!("[UNISWAP_FETCH_EVENT] Swap: пул {:?}, блок {}, amount0: {}, amount1: {}, sqrtPriceX96: {}, ликвидность: {}, тик: {}, event_id: {}",
                                address, block_number, swap.amount0, swap.amount1, swap.sqrt_price_x96, swap.liquidity, swap.tick, event_id);
                            debug!("[UNISWAP_FETCH_EVENTS_DEBUG][{:?}] Swap обработан, current_tick: {}, event_id: {}", address, swap.tick, event_id);
                            entry.current_tick = swap.tick;
                            swap_count += 1;
                        }
                        Err(e) => warn!("[UNISWAP_FETCH_EVENTS_WARN][{:?}] Ошибка декодирования Swap: {:?}, event_id: {}", address, e, event_id),
                    }
                }
                Some(topic) if *topic == MintEvent::signature() => {
                    let raw_log = ethers::abi::RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <MintEvent as ethers::contract::EthLogDecode>::decode_log(&raw_log) {
                        Ok(mint) => {
                            info!("[UNISWAP_FETCH_EVENT] Mint: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}, event_id: {}",
                                address, block_number, mint.tick_lower, mint.tick_upper, mint.liquidity, mint.amount0, mint.amount1, event_id);
                            debug!("[UNISWAP_FETCH_EVENTS_DEBUG][{:?}] Mint обработан, tick_lower: {}, tick_upper: {}, event_id: {}", address, mint.tick_lower, mint.tick_upper, event_id);
                            entry.tick_updates.extend([mint.tick_lower, mint.tick_upper]);
                            mint_count += 1;
                        }
                        Err(e) => warn!("[UNISWAP_FETCH_EVENTS_WARN][{:?}] Ошибка декодирования Mint: {:?}, event_id: {}", address, e, event_id),
                    }
                }
                Some(topic) if *topic == BurnEvent::signature() => {
                    let raw_log = ethers::abi::RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <BurnEvent as ethers::contract::EthLogDecode>::decode_log(&raw_log) {
                        Ok(burn) => {
                            info!("[UNISWAP_FETCH_EVENT] Burn: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}, event_id: {}",
                                address, block_number, burn.tick_lower, burn.tick_upper, burn.liquidity, burn.amount0, burn.amount1, event_id);
                            debug!("[UNISWAP_FETCH_EVENTS_DEBUG][{:?}] Burn обработан, tick_lower: {}, tick_upper: {}, event_id: {}", address, burn.tick_lower, burn.tick_upper, event_id);
                            entry.tick_updates.extend([burn.tick_lower, burn.tick_upper]);
                            burn_count += 1;
                        }
                        Err(e) => warn!("[UNISWAP_FETCH_EVENTS_WARN][{:?}] Ошибка декодирования Burn: {:?}, event_id: {}", address, e, event_id),
                    }
                }
                Some(topic) if *topic == FlashEvent::signature() => {
                    let raw_log = ethers::abi::RawLog {
                        topics: log.topics.clone(),
                        data: log.data.to_vec(),
                    };
                    match <FlashEvent as ethers::contract::EthLogDecode>::decode_log(&raw_log) {
                        Ok(flash) => {
                            info!("[UNISWAP_FETCH_EVENT] Flash: пул {:?}, блок {}, заимствовано {} token0, {} token1, уплачено {} token0, {} token1, event_id: {}",
                                address, block_number, flash.amount0, flash.amount1, flash.paid0, flash.paid1, event_id);
                            debug!("[UNISWAP_FETCH_EVENTS_DEBUG][{:?}] Flash обработан, amount0: {}, amount1: {}, event_id: {}", address, flash.amount0, flash.amount1, event_id);
                            flash_count += 1;
                        }
                        Err(e) => warn!("[UNISWAP_FETCH_EVENTS_DEBUG][{:?}] Ошибка декодирования Flash: {:?}, event_id: {}", address, e, event_id),
                    }
                }
                _ => debug!(
                    "[UNISWAP_EVENTS_DEBUG][{:?}] Неизвестный топик события, event_id: {}",
                    address, event_id
                ),
            }
        }

        self.last_processed_block.store(to_block, Ordering::Release);
        debug!(
            "[UNISWAP_FETCH_EVENTS_DEBUG] Обновлен last_processed_block: {}",
            to_block
        );

        if event_map.is_empty() {
            return Ok(vec![]);
        }

        let pool_addresses: Vec<String> =
            event_map.keys().map(|addr| format!("{:?}", addr)).collect();
        let pools_str = format!("{} пулов", pool_addresses.len());
        info!("[{}] [Блоки {}-{}] Обработано {} событий (Swap: {}, Mint: {}, Burn: {}, Flash: {}) для {}",
            "UNISWAP_FETCH_EVENT".bright_blue(), from_block, to_block,
            swap_count + mint_count + burn_count + flash_count, swap_count, mint_count, burn_count, flash_count, pools_str);
        debug!(
            "[UNISWAP_FETCH_EVENTS_DEBUG] Итог обработки: Swap: {}, Mint: {}, Burn: {}, Flash: {}",
            swap_count, mint_count, burn_count, flash_count
        );

        debug!(
            "[UNISWAP_FETCH_EVENTS_DEBUG] Конец fetch_events, возвращено {} событий",
            event_map.len()
        );
        Ok(event_map.into_values().collect())
    }

    pub async fn fetch_tick_data(
        &self,
        pool_event_info: &PoolEventInfo,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<EventPoolUpdate> {
        debug!(
            "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Начало получения данных для тиков пула",
            pool_address
        );

        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

        let (liquidity, slot0, tick_spacing, _, _) =
            process_pool_data(pool_address, pool_contract.clone().into())
                .await
                .context(format!(
                    "[UNISWAP_EVENT_FETCH_TICK] Не удалось получить данные пула: {:?}",
                    pool_address
                ))?;

        let current_tick = slot0.1;
        debug!(
            "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Текущий тик: {}",
            pool_address, current_tick
        );

        let tick_indices: Vec<i32> = pool_event_info
            .tick_updates
            .iter()
            .map(|tick| *tick)
            .collect();

        if !tick_indices.is_empty() {
            debug!(
                "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Запрашивается {} тиков: {:?}",
                pool_address,
                tick_indices.len(),
                tick_indices
            );
        }

        let mut tick_results = Vec::with_capacity(tick_indices.len());
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 30;

        for tick in tick_indices {
            let mut attempt = 0;
            let mut result = None;

            while attempt < MAX_RETRIES {
                debug!(
                    "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Попытка {} для тика {}",
                    pool_address,
                    attempt + 1,
                    tick
                );
                match pool_contract.ticks(tick).call().await {
                    Ok(data) => {
                        debug!(
                            "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Тик {} успешно получен: {:?}",
                            pool_address, tick, data
                        );
                        result = Some((tick, data));
                        break;
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT_FETCH_TICK][{:?}] Ошибка запроса тика {} на попытке {}: {:?}", pool_address, tick, attempt + 1, e);
                        attempt += 1;
                        if attempt < MAX_RETRIES {
                            debug!("[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Ожидание {} мс перед следующей попыткой", pool_address, RETRY_DELAY_MS);
                            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        }
                    }
                }
            }

            if result.is_none() {
                warn!(
                    "[UNISWAP_EVENT_FETCH_TICK][{:?}] Все попытки запроса тика {} провалились",
                    pool_address, tick
                );
            }
            tick_results.push(result);
        }

        if !tick_results.is_empty() {
            debug!(
                "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Получено {} результатов тиков",
                pool_address,
                tick_results.len()
            );
        }

        let mut tick_map: OrdMap<i32, (i128, U256)> = OrdMap::new();
        for result in tick_results {
            if let Some((tick, data)) = result {
                if (data.0 != 0 || data.1 != 0) && tick % tick_spacing == 0 {
                    debug!(
                        "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] Добавление тика {} в tick_map",
                        pool_address, tick
                    );
                    tick_map.insert(tick, (data.1, U256::from(data.0)));
                } else {
                    info!(
                        "[UNISWAP_EVENT_FETCH_TICK][{:?}] Пропущен тик {} (нулевая ликвидность: gross: {}, net: {} или не кратен tick_spacing: {})",
                        pool_address, tick, data.0, data.1, tick_spacing
                    );
                }
            }
        }

        if !tick_map.is_empty() {
            debug!(
                "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] tick_map заполнен: {} тиков",
                pool_address,
                tick_map.len()
            );
        }

        let sqrt_price_x96 = U256::from(slot0.0);
        debug!(
            "[UNISWAP_EVENT_FETCH_TICK_DEBUG][{:?}] sqrt_price: {}",
            pool_address, sqrt_price_x96
        );

        Ok(EventPoolUpdate {
            liquidity,
            sqrt_price_x96: slot0.0,
            current_tick,
            tick_map,
        })
    }

pub async fn update_graph_from_event(
    &self,
    pool_event_info: &PoolEventInfo,
    graph: Arc<ArcSwap<UniversalGraph>>,
    pool_address: Address,
    provider: Arc<Provider<Ws>>,
) -> anyhow::Result<EventPoolUpdate> {
    debug!("[UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG][{:?}] Начало update_graph_from_event, event_id: {}", pool_address, pool_event_info.event_id);

    let pool_update = self
        .fetch_tick_data(pool_event_info, pool_address, provider.clone())
        .await?;

    debug!("[UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG][{:?}] Данные тиков получены: liquidity: {}, current_tick: {}, event_id: {}", pool_address, pool_update.liquidity, pool_update.current_tick, pool_event_info.event_id);

    let current_graph = graph.load();
    if let Some(pool) = current_graph.edges.get(&pool_address) {
        debug!("[UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG][{:?}] Пул найден в графе, обновление данных, event_id: {}", pool_address, pool_event_info.event_id);
        let mut new_pool = pool.clone();
        new_pool.uniswap_liquidity = pool_update.liquidity;
        new_pool.uniswap_sqrt_price = pool_update.sqrt_price_x96;
        new_pool.uniswap_tick_current = pool_update.current_tick;
        new_pool.tick_map = pool.tick_map.clone().union(pool_update.tick_map.clone());
        graph.load().upsert_pool(new_pool);

        debug!("[UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG][{:?}] Пул обновлен в графе, event_id: {}", pool_address, pool_event_info.event_id);
    } else {
        warn!(
            "[UNISWAP_EVENTS_UPDATE_GRAPH_WARN] Пул {:?} не найден в графе. Обновление пропущено, event_id: {}",
            pool_address, pool_event_info.event_id
        );
    }

    debug!("[UNISWAP_EVENTS_UPDATE_GRAPH_DEBUG][{:?}] Конец update_graph_from_event, event_id: {}", pool_address, pool_event_info.event_id);
    Ok(pool_update)
}

    pub async fn polling_event(
        &self,
        block_receiver: &watch::Receiver<u64>,
        graph: Arc<ArcSwap<UniversalGraph>>,
        event_tx: BroadcastSender<(HashMap<Address, PoolEventInfo>, Arc<Notify>)>,
        simulator_notify: Arc<Notify>,
    ) -> anyhow::Result<()> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 10;
        const BATCH_INTERVAL: Duration = Duration::from_secs(2);
        let max_chunk_size: u64 = 8; // ~2 сек (4 блока/сек)

        info!("[UNISWAP_EVENTS_POLLING] Начало polling_event с интервалом 2 сек");
        let mut block_from = self.last_processed_block.load(Ordering::Acquire);
        debug!(
            "[UNISWAP_EVENTS_POLLING_DEBUG] Начальный блок: {}",
            block_from
        );
        let mut interval = interval(BATCH_INTERVAL);

        loop {
            interval.tick().await;
            let block_to = *block_receiver.borrow();
            if block_to < block_from {
                info!(
                    "[UNISWAP_EVENTS_POLLING] Некорректный диапазон: from {} > to {}",
                    block_from, block_to
                );
                continue;
            }

            if self.subscribed_pools.is_empty() {
                debug!("[UNISWAP_EVENTS_POLLING_DEBUG] Нет подписанных пулов");
                continue;
            }

            let mut attempt = 0;
            let mut all_events = Vec::new();
            let mut current_from = block_from;

            while current_from <= block_to && attempt < MAX_RETRIES {
                let current_to = (current_from + max_chunk_size - 1).min(block_to);
                debug!(
                    "[UNISWAP_EVENTS_POLLING_DEBUG] Обработка диапазона блоков: {}–{}",
                    current_from, current_to
                );

                match self.fetch_events(current_from, current_to).await {
                    Ok(events) => {
                        if !events.is_empty() {
                            debug!("[UNISWAP_EVENTS_POLLING_DEBUG] Получено {} событий для блоков {}–{}", events.len(), current_from, current_to);
                        }
                        all_events.extend(events);
                        attempt = 0;
                    }
                    Err(e) => {
                        error!(
                            "[UNISWAP_EVENTS_POLLING_ERROR] Ошибка fetch_events: {}. Попытка {}/{}",
                            e,
                            attempt + 1,
                            MAX_RETRIES
                        );
                        attempt += 1;
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        }
                        continue;
                    }
                }
                current_from = current_to + 1;
            }

            if attempt >= MAX_RETRIES {
                error!(
                    "[UNISWAP_EVENTS_POLLING_ERROR] Исчерпаны попытки для диапазона {}-{}",
                    block_from, block_to
                );
                continue;
            }

            block_from = block_to + 1;
            self.last_processed_block
                .store(block_from, Ordering::Release);

            if !all_events.is_empty() {
                let (event_map, changed_pools) =
                    self.aggregate_events(all_events, Arc::clone(&graph));
                let notify = Arc::new(Notify::new());
                for pool_address in &changed_pools {
                    if let Some(notifier) = self.pool_notifiers.get(pool_address) {
                        notifier.notify_one();
                        debug!(
                            "[UNISWAP_EVENTS_POLLING_DEBUG][{:?}] Отправлен сигнал Notify для пула",
                            pool_address
                        );
                    }
                }
                if let Err(e) = event_tx.send((event_map, notify)) {
                    error!(
                        "[UNISWAP_EVENTS_POLLING_ERROR] Ошибка отправки в broadcast-канал: {}",
                        e
                    );
                } else {
                    simulator_notify.notify_one();
                    debug!("[UNISWAP_EVENTS_POLLING_DEBUG] Отправлены события в broadcast-канал, уведомлен симулятор");
                }
            }

            if block_from % 100 == 0 {
                info!(
                    "[UNISWAP_EVENTS_POLLING] Обработан последний блок: {}",
                    block_from
                );
            }
        }
    }

    fn aggregate_events(
        &self,
        events: Vec<PoolEventInfo>,
        graph: Arc<ArcSwap<UniversalGraph>>,
    ) -> (HashMap<Address, PoolEventInfo>, Vec<Address>) {
        if !events.is_empty() {
            debug!(
                "[UNISWAP_EVENTS_AGGREGATE_DEBUG] Начало агрегации {} событий",
                events.len()
            );
        }
        let mut map: HashMap<Address, PoolEventInfo> = HashMap::new();
        let mut changed_pools = Vec::new();

        for event in events {
            debug!(
                "[UNISWAP_EVENTS_AGGREGATE_DEBUG][{:?}] Обработка события с ID {}",
                event.address, event.event_id
            );
            let entry = map.entry(event.address).or_insert_with(|| {
                changed_pools.push(event.address);
                PoolEventInfo {
                    event_id: event.event_id,
                    address: event.address,
                    tick_updates: DashSet::new(),
                    current_tick: event.current_tick,
                    block_number: event.block_number,
                }
            });

            let tick_spacing = graph
                .load()
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
                    debug!("[UNISWAP_EVENTS_AGGREGATE_DEBUG][{:?}] Добавление тика {} в tick_updates для события с ID {}", event.address, *tick, event.event_id);
                    entry.tick_updates.insert(*tick);
                } else {
                    info!("[UNISWAP_AGGREGATE_EVENT][{:?}] Пропущен тик {} в агрегации (не кратен tick_spacing: {}) для события с ID {}",
                        event.address, *tick, tick_spacing, event.event_id);
                }
            }
        }

        if !map.is_empty() {
            debug!(
                "[UNISWAP_EVENTS_AGGREGATE_DEBUG] Агрегировано {} событий, last_event_id: {}",
                map.len(),
                map.values().last().map(|e| e.event_id).unwrap_or(0)
            );
        }
        (map, changed_pools)
    }

    pub fn add_pool_handler(
        &self,
        pool_address: Address,
        graph: Arc<ArcSwap<UniversalGraph>>,
        provider: Arc<Provider<Ws>>,
        event_rx: broadcast::Receiver<(HashMap<Address, PoolEventInfo>, Arc<Notify>)>,
        simulator_tx: Sender<PoolEventInfo>,
    ) {
        if self.pool_handlers.contains_key(&pool_address) {
            debug!(
                "[UNISWAP_EVENTS_DEBUG][{:?}] Обработчик для пула уже существует",
                pool_address
            );
            return;
        }

        let notify = Arc::new(Notify::new());
        self.pool_notifiers
            .insert(pool_address, Arc::clone(&notify));
        let subscriber = Arc::new(self.clone());
        let handle = tokio::spawn({
            let graph = Arc::clone(&graph);
            let provider = Arc::clone(&provider);
            let simulator_tx = simulator_tx.clone();
            async move {
                let mut event_rx = event_rx;
                loop {
                    notify.notified().await;
                    debug!(
                        "[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Получен сигнал Notify",
                        pool_address
                    );
                    match event_rx.recv().await {
                        Ok((event_map, _)) => {
                            if let Some(event) = event_map.get(&pool_address) {
                                debug!(
                                    "[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Обработка события с ID {}",
                                    pool_address, event.event_id
                                );
                                if let Err(e) = subscriber
                                    .update_graph_from_event(
                                        event,
                                        Arc::clone(&graph),
                                        pool_address,
                                        Arc::clone(&provider),
                                    )
                                    .await
                                {
                                    error!("[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Ошибка обработки события с ID {}: {}", pool_address, event.event_id, e);
                                    continue;
                                }
                                if let Err(e) = simulator_tx.send(event.clone()).await {
                                    error!("[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Ошибка отправки события с ID {} в симулятор: {}", pool_address, event.event_id, e);
                                } else {
                                    debug!("[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Событие с ID {} отправлено в симулятор", pool_address, event.event_id);
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Ошибка получения события: {}",
                                pool_address, e
                            );
                            break;
                        }
                    }
                }
                error!(
                    "[UNISWAP_EVENTS_POOL_HANDLER][{:?}] Обработчик завершил работу",
                    pool_address
                );
            }
        });
        self.pool_handlers.insert(pool_address, handle);
        info!(
            "[UNISWAP_EVENTS_DEBUG][{:?}] Обработчик для пула добавлен",
            pool_address
        );
    }
}
