use std::{sync::{atomic::{AtomicU64, Ordering}, Arc}, time::Duration};

use colored::Colorize;
use dashmap::{DashMap, DashSet};

use ethers::{abi::RawLog, utils::keccak256};
use ethers::{contract::EthLogDecode, providers::Provider, types::{Address, BlockNumber, Filter, H256, I256, U256, U512}};
use ethers_providers::{Middleware, Ws, Http};
use ethers::contract::EthEvent;

use anyhow::Result;

use futures::{future::join_all, StreamExt};
use log::{info, warn};
use tokio::{sync::watch, time::sleep};

use crate::{uniswap_graph::UniversalGraph, uniswap_v3::{process_pool_data, tick_to_sqrt_price, UniswapV3Pool}};
     



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
#[ethevent(name = "Swap", abi = "Swap(address,address,int256,int256,uint160,uint128,int24)")]
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
#[ethevent(name = "Mint", abi = "Mint(address,address,int24,int24,uint128,uint256,uint256)")]
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
#[ethevent(name = "Burn", abi = "Burn(address,int24,int24,uint128,uint256,uint256)")]
pub struct BurnEvent {
    pub owner: Address,        // address
    pub tick_lower: i32,       // int24
    pub tick_upper: i32,       // int24
    pub liquidity: U256,       // uint128 (можно U256 — безопасно)
    pub amount0: U256,         // uint256
    pub amount1: U256, 
}



// Структуры данных
#[derive(Debug, Default,Clone)]
pub struct EventPoolUpdate {
    pub liquidity: U512,
    pub sqrt_price_x96: U256,
    pub current_tick: i32,
    pub tick_map: DashMap<i32, (i128, U512)>,
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

    ///создает новую подписку
    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        info!("[UNISWAP_EVENT] Создаем подписку на события");
        Self {
            provider,
            subscribed_pools: DashSet::new(),
            last_processed_block: Arc::new(AtomicU64::new(0)),
        }
    }


        /// Добавляет новые пулы в опросник блоков и инициализирует их буферы 
    pub async fn add_pools_to_subscription(&self, pool_address: Address) -> Result<(), Box<dyn std::error::Error>> {
        self.subscribed_pools.insert(pool_address);

        // Логируем добавление пула и текущий размер списка подписанных пулов
        info!(
            "{} Пул с адресом {:?} добавлен в список подписки. Всего подписанных пулов: {}",
            "INFO".bright_yellow().blink(),
            pool_address,
            self.subscribed_pools.len()
        );

        Ok(())
    }


    /// Подписка на новые блоки с переподключением без подсчёта попыток
    pub async fn subscribe_to_new_blocks(
            provider_ws: &Arc<Provider<Ws>>,
            block_sender: watch::Sender<u64>
        ) -> anyhow::Result<()> {

        info!("[BLOCKS] Запускаем подписку на новые блоки...");

        const RECONNECT_DELAY: Duration = Duration::from_secs(1);

        // Храним последний отправленный блок для предотвращения отправки одинаковых блоков
        let mut last_sent_block: u64 = 0;
        
        loop {
            match provider_ws.subscribe_blocks().await {
                Ok(mut stream) => {
                    info!("[BLOCKS] Подписка на блоки активна");

                    while let Some(block) = stream.next().await {
                        if let Some(number) = block.number {
                            let n = number.as_u64();

                            // Отправляем новый номер блока в канал только если он отличается от последнего отправленного
                            if n != last_sent_block {
                                last_sent_block = n;
                                let _ = block_sender.send(n); // Отправляем в канал
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
            H256::from_slice(&keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)")),
            H256::from_slice(&keccak256(b"Mint(address,address,int24,int24,uint128,uint256,uint256)")),
            H256::from_slice(&keccak256(b"Burn(address,int24,int24,uint128,uint256,uint256)")),
        ]
    }
    
    ///список хешей для фильтра событий
   pub async fn fetch_events(&self, from_block: u64, to_block: u64) -> anyhow::Result<Vec<PoolEventInfo>> {

        if from_block > to_block {
            warn!(
                "Ошибка: from_block ({}) больше to_block ({})",
                from_block,
                to_block
            );
        }

        let subscribed_pools = self.subscribed_pools.iter().collect::<Vec<_>>();

        if subscribed_pools.is_empty() {
            return Ok(vec![]);
        }

        let filter = Filter::new()
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .address(subscribed_pools.iter().map(|r| *r.key()).collect::<Vec<_>>())
            .topic0(Self::get_event_topics());

        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(e) => {
                log::warn!("[UNISWAP_EVENT_EVENT] RPC error: {}", e);
                Vec::new()
            }
        };

        let mut pool_event_info_list = Vec::new();

        let mut swap_count = 0;
        let mut mint_count = 0;
        let mut burn_count = 0;

        for log in logs {
            let address = log.address;
            let mut pool_event_info = PoolEventInfo {
                address,
                tick_updates: DashSet::new(),
                current_tick: 0,
            };

            match log.topics.first().map(|t| t.as_bytes()) {
                Some(b) if b == SwapEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(swap) = <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                        pool_event_info.current_tick = swap.tick;
                        swap_count += 1;
                    }
                },
                Some(b) if b == MintEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(mint) = <MintEvent as EthLogDecode>::decode_log(&raw_log) {
                        pool_event_info.tick_updates.extend([mint.tick_lower, mint.tick_upper]);
                        mint_count += 1;
                    }
                },
                Some(b) if b == BurnEvent::signature().as_bytes() => {
                    let raw_log = RawLog {
                        topics: log.topics,
                        data: log.data.to_vec(),
                    };
                    if let Ok(burn) = <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                        pool_event_info.tick_updates.extend([burn.tick_lower, burn.tick_upper]);
                        burn_count += 1;
                    }
                },
                _ => {},
            }

            if !pool_event_info.tick_updates.is_empty() || pool_event_info.current_tick != 0 {
                pool_event_info_list.push(pool_event_info);
            }
        }

        self.last_processed_block.store(to_block, Ordering::Release);

      if swap_count > 0 || mint_count > 0 || burn_count > 0 {
            info!(
                "[UNISWAP_EVENT_FETCH_EVENT] Обработано {} событий (Swap: {}, Mint: {}, Burn: {})",
                pool_event_info_list.len(),
                swap_count,
                mint_count,
                burn_count
            );
        }
        Ok(pool_event_info_list)    

    } 
    
    
    /// Получает данные из пула и сохраняет их в EventPoolUpdate   
    pub async fn fetch_tick_data(
            &self,
            pool_event_info: &PoolEventInfo,
            pool_address: Address,     
            provider: Arc<Provider<Ws>>,
        ) -> anyhow::Result<EventPoolUpdate> {
                // Получаем контракт пула
                let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
                
                sleep(Duration::from_millis(400)).await;
                
                // Получаем базовые данные (liquidity, slot0, и т.д.)
                let (liquidity, slot0, _, _, _) = process_pool_data(pool_address, pool_contract.clone().into())
                .await
                .ok_or_else(|| anyhow::anyhow!("[UNISWAP_EVENT] Ошибка получения данных из fetch_tick_data"))?;
            
            sleep(Duration::from_millis(310)).await;
            
            // Принудительно обновляем current_tick из slot0
            let current_tick = slot0.1;
            
            // Собираем список тикетов для асинхронных запросов
        let tick_indices: Vec<i32> = pool_event_info.tick_updates.iter()
            .map(|tick| *tick)  
            .collect();

        // Составляем список асинхронных запросов
        let tick_futures: Vec<_> = tick_indices.iter().map(|&tick| {
            let contract = pool_contract.clone();
            async move {
                let tick_data = contract.ticks(tick).call().await;
                match tick_data {
                    Ok(data) => Some((tick, data)),
                    Err(_) => None,
                }
            }
        }).collect();

        // Выполняем все запросы параллельно
        let tick_results = join_all(tick_futures).await;

        // Собираем данные в карту
        let tick_map: DashMap<i32, (i128, U512)> = DashMap::new();
        for result in tick_results {
            if let Some((tick, data)) = result {
                let sqrt_price = tick_to_sqrt_price(tick).map_err(anyhow::Error::msg)?;
                tick_map.insert(tick, (data.1, sqrt_price));
            }
        }

        // Вычисляем текущую цену
        let current_price: U512 = tick_to_sqrt_price(current_tick)
        .map_err(anyhow::Error::msg)?;

        info!(
            "[[UNISWAP_EVENT] Обновление] Пул {:?}, ликвидность: {:?}, текущий тик: {:?}",
            pool_address, liquidity, current_tick
        );

        Ok(EventPoolUpdate {
            liquidity: liquidity.into(),
            sqrt_price_x96: slot0.0.into(),
            current_tick,
            tick_map,
            current_price,
        })
    }



    ///обновляем граф данными
    pub async fn update_graph_from_event(
        &self,
        pool_event_info: &PoolEventInfo,
        graph: Arc<UniversalGraph>,  
        pool_address: Address,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        // 1. Получаем начальный блок и текущий блок
        
        let pool_update = self.fetch_tick_data(pool_event_info, pool_address, provider.clone()).await?;
        
        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;
            pool.tick_map = pool_update.tick_map;
            pool.uniswap_current_price = pool_update.current_price;
            
            info!(
                "[UNISWAP_EVENT_GRAPH_UPDATE] Обновлен пул {:?} ( Ликвидность: {} | Текущая цена (sqrt): {} | Текущий тик: {}",
                pool_address,
                pool.uniswap_liquidity,
                pool.uniswap_sqrt_price,
                pool.uniswap_tick_current,
            );
        }
        
        Ok(())
    }

    pub async fn polling_event(
        &self,
        graph: Arc<UniversalGraph>,
        provider_ws: Arc<Provider<Ws>>,
        block_receiver: watch::Receiver<u64>, // Канал для получения блока
    ) -> anyhow::Result<()> {
        
            //номер блока запуска
        let mut block_from: u64 = *block_receiver.borrow();
        
        loop {        
            // Получаем адрес пула из subscribed_pools (если есть)
            let subscribed_pools = self.subscribed_pools.clone();
            if subscribed_pools.is_empty() {
                log::warn!("[UNISWAP_EVENT_POLLING_EVENT] Нет подписанных пулов для обработки");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
            
            // Получаем номер блока из subscribe_to_new_blocks
            let block_to : u64 = *block_receiver.borrow();
                    
            // Запрашиваем события для пула
            for pool_address in subscribed_pools {
                // Получаем события (вектор PoolEventInfo для разных пулов)
                let pool_events_result = self.fetch_events( block_from, block_to).await;
                
                let pool_events = match pool_events_result {
                    Ok(events) => events,
                    Err(e) => {
                        log::warn!(
                            "[UNISWAP_EVENT_POLLING_EVENT] Ошибка получения событий для пула {:?}: {}",
                            pool_address, e
                        );
                        continue;
                    }
                };
                
                // Обрабатываем каждое событие отдельно
                for pool_event_info in pool_events {
                    let pool_address = pool_event_info.address;
                    
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Err(e) = self
                    .update_graph_from_event(&pool_event_info, graph.clone(), pool_address, provider_ws.clone())
                    .await
                    {
                        log::error!(
                            "[UNISWAP_EVENT_POLLING_EVENT_ERROR] Ошибка обновления графа для пула {:?}: {}",
                            pool_address, e
                        );
                        continue;
                    }
                }          
            }        
            // 6. Обновляем блоки для следующей итерации
            info!("[UNISWAP_EVENT_POLLING_EVENT] Обработаны события для блока от {} до {} всего {} блоков", block_from, block_to, block_to.saturating_sub(block_from) + 1);
            
            block_from = block_to + 1;
            // 7. Сдвигаем block_from на следующий блок
            tokio::time::sleep(Duration::from_secs(1)).await; // Задержка перед следующим циклом
        }
    }

}

















    
