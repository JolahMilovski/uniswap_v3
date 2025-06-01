use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use colored::Colorize;
use dashmap::DashSet;

use ethers::contract::EthEvent;
use ethers::{abi::RawLog, utils::keccak256};
use ethers::{
    contract::EthLogDecode,
    providers::Provider,
    types::{Address, BlockNumber, Filter, H256, I256, U256, U512},
};
use ethers_providers::{Http, Middleware, Ws};

use anyhow::{Context, Result};

use futures::StreamExt;
use im::OrdMap;
use log::{debug, info, warn};
use tokio::{sync::watch, time::sleep};

use crate::{
    uniswap_graph::UniversalGraph,
    uniswap_v3::{calculate_current_price, process_pool_data, UniswapV3Pool},
};

ethers_contract::abigen!(
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
    #[ethevent(indexed)]
    sender: Address,
    #[ethevent(indexed)]
    owner: Address,
    tick_lower: i32,
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
    pub owner: Address,  // address
    pub tick_lower: i32, // int24
    pub tick_upper: i32, // int24
    pub liquidity: U256, // uint128 (можно U256 — безопасно)
    pub amount0: U256,   // uint256
    pub amount1: U256,
}

#[derive(Debug, Clone, EthEvent)]
#[ethevent(
    name = "Flash",
    abi = "Flash(address,address,uint256,uint256,uint256,uint256)"
)]
pub struct FlashEvent {
    pub sender: Address,    // address
    pub recipient: Address, // address
    pub amount0: U256,      // uint256
    pub amount1: U256,      // uint256
    pub paid0: U256,        // uint256
    pub paid1: U256,        // uint256
}

// Структуры данных
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

pub struct PoolEventInfo {
    pub address: Address,
    pub tick_updates: DashSet<i32>,
    pub current_tick: i32,
}



impl UniswapEventSubscriber {

    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        info!("[UNISWAP_EVENT] Создаем подписку на события");
        Self {
            provider,
            subscribed_pools: DashSet::new(),
            last_processed_block: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn add_pools_to_subscription(
        &self,
        pool_address: Address,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.subscribed_pools.insert(pool_address);
        info!(
            "{} Пул с адресом {:?} добавлен в список подписки. Всего подписанных пулов: {}",
            "INFO".bright_yellow().blink(),
            pool_address,
            self.subscribed_pools.len()
        );
        Ok(())
    }

    pub async fn subscribe_to_new_blocks(
        provider_ws: &Arc<Provider<Ws>>,
        block_sender: watch::Sender<u64>,
    ) -> anyhow::Result<()> {
        info!("[BLOCKS] Запускаем подписку на новые блоки...");
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);
        let mut last_sent_block: u64 = 0;
        loop {
            match provider_ws.subscribe_blocks().await {
                Ok(mut stream) => {
                    info!("[BLOCKS] Подписка на блоки активна");
                    while let Some(block) = stream.next().await {
                        if let Some(number) = block.number {
                            let n = number.as_u64();
                            if n != last_sent_block {
                                last_sent_block = n;
                                let _ = block_sender.send(n);
                            }
                            if n % 100 == 0 {
                                info!("[BLOCKS] Новый блок: {}", n);
                            }
                        }
                    }
                    info!("[BLOCKS] Поток блоков завершился. Переподключение...");
                }
                Err(e) => {
                    info!("[BLOCKS] Ошибка подписки: {e}. Переподключение...");
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    fn get_event_topics() -> Vec<H256> {
        vec![
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
        ]
    }

    pub async fn fetch_events(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<PoolEventInfo>> {
        if from_block > to_block {
            warn!(
                "Ошибка: from_block ({}) больше to_block ({})",
                from_block, to_block
            );
        }
        let subscribed_pool_addresses: Vec<Address> = self
            .subscribed_pools
            .iter()
            .map(|entry| *entry.key())
            .collect();
        if subscribed_pool_addresses.is_empty() {
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
                warn!("[UNISWAP_EVENT] RPC error: {}", e);
                Vec::new()
            }
        };
        let mut event_map = std::collections::HashMap::new();
        let mut swap_count = 0;
        let mut mint_count = 0;
        let mut burn_count = 0;
        let mut flash_count = 0;
        for log in logs {
            let address = log.address;
            let entry = event_map.entry(address).or_insert(PoolEventInfo {
                address,
                tick_updates: DashSet::new(),
                current_tick: 0,
            });
            match log.topics.first().map(|t| t.as_bytes()) {
                Some(b) if b == SwapEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(swap) = <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                        entry.current_tick = swap.tick;
                        swap_count += 1;
                    }
                }
                Some(b) if b == MintEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(mint) = <MintEvent as EthLogDecode>::decode_log(&raw_log) {
                        entry
                            .tick_updates
                            .extend([mint.tick_lower, mint.tick_upper]);
                        mint_count += 1;
                    }
                }
                Some(b) if b == BurnEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(burn) = <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                        entry
                            .tick_updates
                            .extend([burn.tick_lower, burn.tick_upper]);
                        burn_count += 1;
                    }
                }
                Some(b) if b == FlashEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(flash) = <FlashEvent as EthLogDecode>::decode_log(&raw_log) {
                        debug!(
                            "[UNISWAP_EVENT] Flash: пул {:?}, заимствовано {} token0, {} token1, уплачено {} token0, {} token1",
                            address, flash.amount0, flash.amount1, flash.paid0, flash.paid1
                        );
                        flash_count += 1;
                    }
                }
                _ => {}
            }
        }
        self.last_processed_block.store(to_block, Ordering::Release);
        if swap_count > 0 || mint_count > 0 || burn_count > 0 || flash_count > 0 {
            let pool_addresses: Vec<String> =
                event_map.keys().map(|addr| format!("{:?}", addr)).collect();
            let pools_str = if pool_addresses.len() <= 3 {
                pool_addresses.join(", ")
            } else {
                format!(
                    "{} пулов, пример: {}",
                    pool_addresses.len(),
                    pool_addresses[0]
                )
            };
            info!(
                "[{}] Обработано {} событий (Swap: {}, Mint: {}, Burn: {}, Flash: {}) для пулов: {}",
                "UNISWAP_EVENT".bright_blue(),
                event_map.len(),
                swap_count,
                mint_count,
                burn_count,
                flash_count,
                pools_str
            );
        }
        Ok(event_map.into_values().collect())
    }

    pub async fn fetch_tick_data(
        &self,
        pool_event_info: &PoolEventInfo,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
        graph: Arc<UniversalGraph>,
    ) -> anyhow::Result<EventPoolUpdate> {
        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
        sleep(Duration::from_millis(200)).await;
        let (liquidity, slot0, _, _, _) = process_pool_data(pool_address, pool_contract.clone().into())
            .await
            .context(format!(
                "[UNISWAP_EVENT] Failed to fetch pool data for pool: {:?}", 
                pool_address
            ))?;
        let current_tick = slot0.1;
        let pool_info = graph.edges.get(&pool_address)
            .ok_or_else(|| anyhow::anyhow!("Pool {:?} not found in graph", pool_address))?;
        sleep(Duration::from_millis(200)).await;
        let tick_indices: Vec<i32> = pool_event_info
            .tick_updates
            .iter()
            .map(|tick| *tick)
            .collect();
        let tick_futures: Vec<_> = tick_indices
            .into_iter()
            .map(|tick| {
                let contract = pool_contract.clone();
                async move {
                    let tick_data = contract.ticks(tick).call().await;
                    tick_data.map_or_else(|_| None, |data| Some((tick, data)))
                }
            })
            .collect();
        let tick_results = futures::future::join_all(tick_futures).await;
        let mut tick_map = OrdMap::new();
        for result in tick_results {
            if let Some((tick, data)) = result {
                tick_map.insert(tick, (data.1, U512::from(data.0)));
            }
        }
        debug!("[UNISWAP_EVENT] Обновлено {} тиков для пула {:?}", tick_map.len(), pool_address);

        let sqrt_price = U512::from(slot0.0);
        let current_price = calculate_current_price(sqrt_price, pool_info.uniswap_token_a_decimals, pool_info.uniswap_token_b_decimals)
            .map_err(anyhow::Error::msg)?;
        info!(
            "[{}] Обновление пула: {:?}, Ликвидность: {}, Текущий тик: {}, Цена: {}",
            "UNISWAP_EVENT".bright_blue(),
            pool_address, liquidity, current_tick, current_price
        );
        Ok(EventPoolUpdate {
            liquidity,
            sqrt_price_x96: slot0.0,
            current_tick,
            tick_map,
            current_price,
        })
    }

    pub async fn update_graph_from_event(
        &self,
        pool_event_info: &PoolEventInfo,
        graph: Arc<UniversalGraph>,
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {


        let pool_update = self
            .fetch_tick_data(pool_event_info, pool_address, provider.clone(), graph.clone()).await?;

        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;
            pool.uniswap_current_price = pool_update.current_price;
            pool.tick_map = pool.tick_map.clone().union(pool_update.tick_map);
            info!(
                "[{}] Обновлен пул: {:?}, Ликвидность: {}, Цена (sqrt): {}, Текущий тик: {}, Цена: {}",
                "UNISWAP_EVENT".bright_blue(),
                pool_address,
                pool.uniswap_liquidity,
                pool.uniswap_sqrt_price,
                pool.uniswap_tick_current,
                pool.uniswap_current_price
            );
                        // Обновляем JSON для этого пула
            if let Err(e) = graph.update_pool_json(pool_address, "graph_final.json") {
                warn!("[UNISWAP_EVENT] Ошибка обновления JSON для пула {:?}: {}", pool_address, e);
            }
        }
        Ok(())
    }

    pub async fn polling_event(
        &self,
        graph: Arc<UniversalGraph>,
        provider_ws: Arc<Provider<Ws>>,
        block_receiver: &watch::Receiver<u64>,
    ) -> anyhow::Result<()> {
        let mut block_from = *block_receiver.borrow();
        let max_chunk_size: u64 = 200;
        loop {
            let subscribed_pools = self.subscribed_pools.clone();
            if subscribed_pools.is_empty() {
                warn!("[UNISWAP_EVENT] Нет пулов для обработки");
                sleep(Duration::from_secs(3)).await;
                continue;
            }
            let block_to = *block_receiver.borrow();
            if block_to < block_from {
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            let mut current_from = block_from;
            let mut all_events = Vec::new();
            while current_from <= block_to {
                let current_to = current_from.min(block_to + max_chunk_size - 1);
                match self.fetch_events(current_from, current_to).await {
                    Ok(events) => {
                        all_events.extend(events);
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT] Ошибка получения событий за блоки {}-{}: {}", 
                            current_from, current_to, e);
                    }
                }
                current_from = current_to + 1;
                sleep(Duration::from_millis(100)).await;
            }
            let unique_pools: Vec<_> = all_events
                .iter()
                .map(|e| format!("{:?}", e.address))
                .collect();
            for pool_event in all_events {
                let pool_address = pool_event.address;
                if let Err(e) = self
                    .update_graph_from_event(
                        &pool_event,
                        graph.clone(),
                        pool_address,
                        provider_ws.clone(),
                    )
                    .await
                {
                    log::error!(
                        "[{}] Ошибка обновления пула: {:?}: {}", 
                        "UNISWAP_EVENT".red(),
                        pool_address, 
                        e
                    );
                }
            }
            info!(
                "[{}] Обработаны блоки {}–{}, пулов: {}",
                "UNISWAP_EVENT".bright_blue(),
                block_from,
                block_to,
                unique_pools.len()
            );
            block_from = block_to + 1;
            sleep(Duration::from_secs(1)).await;
        }
    }
}
