use std::{collections::HashSet, sync::{atomic::Ordering, Arc}};
use ethers::{contract::{abigen, EthLogDecode}, providers::{Provider, StreamExt}, types::{Address, Filter, H256, U512}};
use tokio::time::Duration;
use ethers_providers::{Middleware, Ws};
use log::{error, info};
use std::str::FromStr;
use tokio::sync::Mutex;
use uniswap_graph::UniversalGraph;
use ethers::utils::keccak256;
use ethers::abi::RawLog;

use crate::{uniswap_cache::UniswapPoolCache, uniswap_graph, uniswap_v3::{calculate_current_price, process_pool_data, UniswapV3Pool}}; 
use crate::SYNC_START_BLOCK;
use crate::SYNC_END_BLOCK;

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
    TickLens,
    r#"[{
        "inputs": [
            { "internalType": "address", "name": "pool", "type": "address" },
            { "internalType": "int16", "name": "wordPosition", "type": "int16" }
        ],
        "name": "getPopulatedTicksInWord",
        "outputs": [
            {
                "components": [
                    { "internalType": "int24", "name": "tick", "type": "int24" },
                    { "internalType": "int128", "name": "liquidityNet", "type": "int128" },
                    { "internalType": "uint128", "name": "liquidityGross", "type": "uint128" }
                ],
                "internalType": "struct ITickLens.PopulatedTick[]",
                "name": "",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }]"#
);

pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Ws>>,
    graph: Arc<Mutex<UniversalGraph>>,
}

impl UniswapEventSubscriber {

    ///создали новый экземпляр
    pub fn new(
        provider: Arc<Provider<Ws>>,
        graph: Arc<Mutex<UniversalGraph>>,
    ) -> Self {
        UniswapEventSubscriber {
            provider,
            graph,
        }
    }

    pub async fn subscribe_to_events_for_all_pools(self: Arc<Self>) -> anyhow::Result<()> {
        let provider = self.provider.clone();
        let graph = self.graph.clone();
    
        let pool_cache = UniswapPoolCache::load_from_bin("uniswap_pool_addresses_cache.bin")?;
        let pool_addresses_vec: Vec<Address> = pool_cache.pool_addresses.into_iter().collect();
    
        if pool_addresses_vec.is_empty() {
            return Err(anyhow::anyhow!(" [EVENT] Нет пулов для подписки"));
        }
    
        // 1. Обработка исторических событий
        self.clone().process_historical_events(pool_addresses_vec.clone()).await?;
    
        // 2. Подписка на новые события
        let topics = Self::get_event_topics();
        let self_clone = self.clone(); // клон для использования в потоке
    
        tokio::spawn(async move {
            let mut last_handled_block = SYNC_END_BLOCK.load(Ordering::SeqCst);
            let mut block_counter = 0;
    
            loop {
                let filter = Filter::new()
                    .address(pool_addresses_vec.clone())
                    .topic0(topics.clone());
    
                let subscribe_result = provider.subscribe_logs(&filter).await;
    
                let mut stream = match subscribe_result {
                    Ok(s) => s,
                    Err(e) => {
                        error!(" [EVENT] Ошибка подписки на события: {:?}. Ретрай через 5 секунд...", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
    
                info!(" [EVENT] Успешно подписались на события Uniswap");
    
                while let Some(log) = stream.next().await {
                    if let Some(block_number) = log.block_number {
                        last_handled_block = block_number.as_u64();
                    }
    
                    let pool_address = log.address;
                    let event_clone = log.clone();
    
                    let self_clone_inner = self_clone.clone(); // вложенный клон
                    tokio::spawn(async move {
                        if let Err(err) = self_clone_inner.process_event(&event_clone, pool_address).await {
                            error!(" [EVENT] Ошибка обработки события: {:?}", err);
                        }
                    });
    
                    block_counter = 0;
                }
    
                if block_counter >= 3 {
                    info!(" [EVENT] Нет новых событий, переподключаемся через 3 секунды...");
                    self_clone.clone().handle_block_gap(last_handled_block).await;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    block_counter = 0;
                } else {
                    block_counter += 1;
                }
            }
        });
    
        Ok(())
    }
    

    async fn process_historical_events(self: Arc<Self>, pool_addresses: Vec<Address>) -> anyhow::Result<()> {
        let start_block = SYNC_START_BLOCK.load(Ordering::SeqCst);
        let end_block = SYNC_END_BLOCK.load(Ordering::SeqCst);
        
        if start_block >= end_block {
            info!(" [EVENT] Нет исторических событий для обработки (start_block >= end_block)");
            return Ok(());
        }
    
        info!(" [EVENT] Обработка исторических событий для {} пулов в диапазоне блоков {} - {}", 
            pool_addresses.len(), start_block, end_block);
    
        let topics = Self::get_event_topics();
        let filter = Filter::new()
            .from_block(start_block)
            .to_block(end_block)
            .address(pool_addresses)
            .topic0(topics);
    
        let logs = self.provider.get_logs(&filter).await?;
        info!(" [EVENT] Найдено {} исторических событий для обработки", logs.len());

        // Группировка событий по пулам для избежания дублирования
        let mut unique_pools = HashSet::new();
        for log in &logs {
            unique_pools.insert(log.address);
        }

        // Параллельная обработка каждого уникального пула
        let tasks = unique_pools.into_iter().map(|pool_address| {
            let self_clone = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = self_clone.fully_update_pool(pool_address).await {
                    error!(" [EVENT] Ошибка полного обновления пула {:?}: {:?}", pool_address, e);
                }
            })
        });

        futures::future::join_all(tasks).await;
        info!(" [EVENT] Обработка исторических событий завершена");
    
        Ok(())
    }

    async fn process_event(
        &self,
        log: &ethers::types::Log,
        pool_address: Address,
    ) -> anyhow::Result<()> {
        match UniswapPoolEvents::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        }) {
            Ok(event) => {
                // Для любого типа события выполняем полное обновление пула
                self.fully_update_pool(pool_address).await?;
                
                // Дополнительная обработка для конкретных типов событий
                match event {
                    UniswapPoolEvents::SwapFilter(swap) => {
                        info!(" [EVENT] Swap в пуле {:?}, новый тик: {}", pool_address, swap.tick);
                    }
                    UniswapPoolEvents::MintFilter(mint) => {
                        info!(" [EVENT] Mint в пуле {:?}, диапазон: {} - {}", 
                            pool_address, mint.tick_lower, mint.tick_upper);
                    }
                    UniswapPoolEvents::BurnFilter(burn) => {
                        info!(" [EVENT] Burn в пуле {:?}, диапазон: {} - {}", 
                            pool_address, burn.tick_lower, burn.tick_upper);
                    }
                }
            }
            Err(err) => {
                error!(" [EVENT] Ошибка декодирования события: {:?}", err);
            }
        }
        Ok(())
    }

    async fn fully_update_pool(&self, pool_address: Address) -> anyhow::Result<()> {
        let pool_contract = &UniswapV3Pool::new(pool_address, self.provider.clone());
            
        
        // Use process_pool_data from uniswap_v3 to get all pool data efficiently
        let (liquidity, slot0, tick_spacing, max_liquidity, fee, tick_map) = 
            process_pool_data(
                pool_address,
                pool_contract,
                self.provider.clone()
            ).await.ok_or_else(|| anyhow::anyhow!(" [EVENT] провал process pool data"))?;

        let (sqrt_price_x96, tick, _, _, _, _, _) = slot0;
        let sqrt_price = U512::from_str(&sqrt_price_x96.to_string()).unwrap_or_default();

        // Get token decimals from existing pool data to calculate current price
        let mut graph = self.graph.lock().await;
        if let Some(pool) = graph.edges.get(&pool_address) {
            let current_price = calculate_current_price(
                sqrt_price,
                pool.uniswap_token_a_decimals,
                pool.uniswap_token_b_decimals
            ).map_err(anyhow::Error::msg)?;

            // Update only the necessary fields
            if let Some(existing_pool) = graph.edges.get_mut(&pool_address) {
                existing_pool.uniswap_liquidity = liquidity;
                existing_pool.uniswap_sqrt_price = sqrt_price;
                existing_pool.uniswap_current_price = current_price;
                existing_pool.uniswap_tick_current = tick;
                existing_pool.uniswap_tick_spacing = tick_spacing;
                existing_pool.uniswap_max_liquidity_per_tick = U512::from(max_liquidity);
                existing_pool.uniswap_fee_tier = fee;
                
                // Merge new tick data with existing, preserving unchanged ticks
                for (tick, (net, price)) in tick_map {
                    existing_pool.tick_map.insert(tick, (net, price));
                }
                
                existing_pool.is_active = !liquidity.is_zero();
            }
        }
        Ok(())
    }
   

    async fn handle_block_gap(self: Arc<Self>, last_handled_block: u64) {
        let current_block = match self.provider.get_block_number().await {
            Ok(b) => b.as_u64(),
            Err(_) => return,
        };

        if current_block > last_handled_block {
            info!(" [EVENT] Запрашиваем пропущенные блоки от {} до {}", last_handled_block + 1, current_block);
            self.fetch_and_process_missing_blocks(last_handled_block + 1, current_block).await;
        }
    }

    async fn fetch_and_process_missing_blocks(self: Arc<Self>, from_block: u64, to_block: u64) {
        let filter = Filter::new()
            .from_block(from_block)
            .to_block(to_block);
    
        match self.provider.get_logs(&filter).await {
            Ok(logs) => {
                let mut unique_pools = HashSet::new();
                for log in &logs {
                    unique_pools.insert(log.address);
                }
    
                // Клонируем `self` для использования в асинхронных задачах
                let tasks = unique_pools.into_iter().map(|pool_address| {
                    let self_clone = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = self_clone.fully_update_pool(pool_address).await {
                            error!(" [EVENT] Ошибка обновления пропущенного пула {:?}: {:?}", pool_address, e);
                        }
                    })
                });    
                // Ожидаем завершения всех задач
                futures::future::join_all(tasks).await;
            }
            Err(e) => {
                error!(" [EVENT] Ошибка при запросе пропущенных блоков: {:?}", e);
            }
        }
    }
    

    fn get_event_topics() -> Vec<H256> {
        let swap_topic = "Swap(address,address,int256,int256,uint160,uint128,int24)";
        let mint_topic = "Mint(address,address,int24,int24,uint128,uint256,uint256)";
        let burn_topic = "Burn(address,int24,int24,uint128,uint256,uint256)";
    
        vec![
            H256::from(keccak256(swap_topic.as_bytes())),
            H256::from(keccak256(mint_topic.as_bytes())),
            H256::from(keccak256(burn_topic.as_bytes())),
        ]
    }
}