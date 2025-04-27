use std::sync::Arc;
use ethers::{contract::abigen, providers::{Provider, StreamExt}, types::{Address, Filter, H160, H256, U64}};
use tokio::time::Duration;
use ethers_providers::{Middleware, Ws};
use log::{error, info};
use std::{env, str::FromStr};
use tokio::sync::Mutex;
use uniswap_graph::UniversalGraph;
use ethers::contract::EthLogDecode;
use ethers::utils::keccak256;
use ethers::abi::RawLog;

use crate::{uniswap_cache::UniswapPoolCache, uniswap_graph, uniswap_v3::{calculate_current_price, tick_to_sqrt_price}}; 

abigen!(
    IUniswapV3Pool,
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

    pub async fn subscribe_to_events_for_all_pools(self: Arc<Self>) -> anyhow::Result<()> {
        let provider = self.provider.clone();
        let graph = self.graph.clone();
    
        let pool_cache = UniswapPoolCache::load_from_file()?;
        let pool_addresses_vec: Vec<Address> = pool_cache.pool_addresses.into_iter().collect();
    
        if pool_addresses_vec.is_empty() {
            return Err(anyhow::anyhow!("Нет пулов для подписки"));
        }
    
        let topics = Self::get_event_topics();
    
        // No need for self_arc since we already have Arc<Self>
        tokio::spawn(async move {
            let mut last_handled_block = 0u64;
            let mut block_counter = 0;
    
            loop {
                let filter = Filter::new()
                    .address(pool_addresses_vec.clone())
                    .topic0(topics.clone());
    
                let subscribe_result = provider.subscribe_logs(&filter).await;
    
                let mut stream = match subscribe_result {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Ошибка подписки на события: {:?}. Ретрай через 5 секунд...", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
    
                info!("Успешно подписались на события Uniswap");
    
                while let Some(log) = stream.next().await {
                    if let Some(block_number) = log.block_number {
                        last_handled_block = block_number.as_u64();
                        info!("Событие из блока: {}", last_handled_block);
                    }
    
                    let pool_address = log.address;
    
                    if let Err(err) = Self::process_event(&log, pool_address, &graph, &provider).await {
                        error!("Ошибка обработки события: {:?}", err);
                    }
    
                    block_counter = 0;
                }
    
                if block_counter >= 3 {
                    info!("Нет новых событий, переподключаемся через 3 секунды...");
                    self.handle_block_gap(last_handled_block).await;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    block_counter = 0;
                } else {
                    block_counter += 1;
                }
            }
        });
    
        Ok(())
    }
      
    async fn process_event(
        log: &ethers::types::Log,
        pool_address: Address,
        graph: &Arc<Mutex<UniversalGraph>>,
        provider: &Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        match IUniswapV3PoolEvents::decode_log(&RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        }) {
            Ok(event) => {
                let graph_clone = graph.clone(); // Получаем ссылку на данные
                let mut graph_clone = graph_clone.lock().await; // Блокируем доступ к данным
                match event {
                    IUniswapV3PoolEvents::SwapFilter(swap) => {
                        if let Some(pool) = graph_clone.edges.get_mut(&pool_address) {
                            if let Ok(price) = calculate_current_price(
                                swap.sqrt_price_x96.into(),
                                pool.uniswap_token_a_decimals,
                                pool.uniswap_token_b_decimals,
                            ) {
                                pool.uniswap_current_price = price;
                                info!("Обновили цену для пула {}: {}", pool_address, price);
                            }
                        }
                    }
                    IUniswapV3PoolEvents::MintFilter(mint) => {
                        let graph_clone = graph.clone(); 
                        if let Err(e) = Self::update_ticks_for_words(
                            graph_clone,
                            provider.clone(),
                            pool_address,
                            mint.tick_lower,
                            mint.tick_upper,
                        ).await {
                            error!("Ошибка обновления тиков после Mint: {:?}", e);
                        }
                    }
                    IUniswapV3PoolEvents::BurnFilter(burn) => {
                        if let Err(e) = Self::update_ticks_for_words(
                            graph.clone(),
                            provider.clone(),
                            pool_address,
                            burn.tick_lower,
                            burn.tick_upper,
                        ).await {
                            error!("Ошибка обновления тиков после Burn: {:?}", e);
                        }
                    }
                }
            }
            Err(err) => {
                error!("Ошибка декодирования события: {:?}", err);
            }
        }
        Ok(())
    }

    async fn handle_block_gap(&self, last_handled_block: u64) {
        let current_block = self.provider.get_block_number().await.unwrap_or(U64::zero());
        let current_block = current_block.as_u64();

        if current_block > last_handled_block {
            info!("Запрашиваем пропущенные блоки от {} до {}", last_handled_block + 1, current_block);
            self.fetch_and_process_missing_blocks(last_handled_block + 1, current_block).await;
        }
    }

    async fn fetch_and_process_missing_blocks(&self, from_block: u64, to_block: u64) {
        let filter = Filter::new()
            .from_block(from_block)
            .to_block(to_block);

        match self.provider.get_logs(&filter).await {
            Ok(logs) => {
                for log in logs {
                    if let Err(e) = Self::process_event(&log, log.address, &self.graph, &self.provider).await {
                        error!("Ошибка обработки пропущенного события: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("Ошибка при запросе пропущенных блоков: {:?}", e);
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
    /// Обновление тиков в графе
    async fn update_ticks_for_words(
        graph: Arc<Mutex<UniversalGraph>>,
        provider: Arc<Provider<Ws>>,
        pool_address: Address,
        tick_lower: i32,
        tick_upper: i32,
    ) -> anyhow::Result<()> {
        info!("Обновление тиков с {} до {}", tick_lower, tick_upper);

        let tick_lens_addr = H160::from_str(&env::var("UNISWAP_TICK_LENS_ADDRESS")?)?;
        let tick_lens = TickLens::new(tick_lens_addr, provider.clone());

        let word_lower = tick_lower >> 8;
        let word_upper = tick_upper >> 8;

        let mut all_ticks = Vec::new();
        for word in word_lower..=word_upper {
            let ticks = tick_lens.get_populated_ticks_in_word(pool_address, word.try_into().unwrap()).call().await?;
            info!("Обработано {} тиков в слове {}", ticks.len(), word);
            all_ticks.extend(ticks);
        }

        // Запрос ликвидности вне блокировки графа
        let new_liquidity = IUniswapV3Pool::new(pool_address, provider.clone()).liquidity().call().await?;

        // Блокируем граф один раз для записи ликвидности и обновления тиковой карты
        let mut graph = graph.lock().await;
        if let Some(pool) = graph.edges.get_mut(&pool_address) {
            // Обновляем ликвидность
            pool.uniswap_liquidity = new_liquidity.into();

            // Обновляем тиковую карту
            for tick_info in all_ticks.iter() {
                if let Ok(price) = tick_to_sqrt_price(tick_info.tick) {
                    pool.tick_map.insert(tick_info.tick, (tick_info.liquidity_net, price));
                }
            }

            info!("Обновлена карта тиков с {} тиками и ликвидность пула: {:?}", all_ticks.len(), pool_address);
        }

        info!("Обновление тиков завершено для пула: {:?}", pool_address);

        Ok(())
    }

}
