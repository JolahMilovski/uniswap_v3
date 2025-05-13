use std::{sync::{atomic::{AtomicU64, Ordering}, Arc}, time::Duration};

use dashmap::{DashMap, DashSet};

use ethers::abi::RawLog;
use ethers::{contract::{abigen, EthLogDecode}, providers::Provider, types::{Address, BlockNumber, Filter, H256, I256, U256, U512}};
use ethers_providers::{Middleware, Ws, Http};
use ethers::contract::EthEvent;
use ethers::utils::keccak256;

use anyhow::anyhow;
use anyhow::Result;

use futures::{future::join_all, StreamExt};
use log::info;
use tokio::{sync::watch, time::sleep};

use crate::{uniswap_graph::UniversalGraph, uniswap_v3::{self, fetch_tick_spacing, tick_to_sqrt_price, UniswapV3Pool}};
     


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
pub struct BufferUniswapEventSubscriber {
    pub pool_address: Address,
    pub tick_updates: DashSet<i32>,
    pub current_tick: i32,
    pub tick_spacing: i32,
    pub block_from: u64,    
}


#[derive(Debug, Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Http>>,
    buffers: Arc<DashMap<Address, BufferUniswapEventSubscriber>>,
    subscribed_pools: Arc<DashSet<Address>>,
    last_processed_block: Arc<AtomicU64>,
}

impl UniswapEventSubscriber {

    ///создает новую подписку
    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        info!("[UNISWAP_EVENT] Создаем подписку на события");
        Self {
            provider,
            buffers: Arc::new(DashMap::new()),
            subscribed_pools: Arc::new(DashSet::new()),
            last_processed_block: Arc::new(AtomicU64::new(0)),
        }
    }


        /// Добавляет новые пулы в опросник блоков и инициализирует их буферы 
    pub async fn add_pools_to_subscription(
        &self,
        pool_address: Address,
        ws_provider: Arc<Provider<Ws>>,
    ) -> Result<()> {
        // Проверка уже подписанного пула — теперь DashSet
        if self.subscribed_pools.contains(&pool_address) {
            info!("[UNISWAP_EVENT] Пул {:?} уже подписан", pool_address);
            return Ok(());
        }

        // Получение данных с повторами
        let (tick_spacing, block_from) = tokio::try_join!(
            async {
                for _ in 0..3 {
                    if let Some(spacing) = fetch_tick_spacing(pool_address, ws_provider.clone()).await {
                        return Ok(spacing);
                    }
                    sleep(Duration::from_secs(1)).await;
                }
                Err(anyhow!("Не удалось получить tick_spacing"))
            },
            async {
                for _ in 0..3 {
                    if let Ok(n) = ws_provider.get_block_number().await {
                        return Ok(n.as_u64());
                    }
                    sleep(Duration::from_secs(1)).await;
                }
                Err(anyhow!("Не удалось получить номер блока"))
            }
        )?;

        self.subscribed_pools.insert(pool_address);
        self.buffers.insert(
            pool_address,
            BufferUniswapEventSubscriber {
                pool_address,
                tick_updates: DashSet::new(),
                current_tick: 0,
                tick_spacing,
                block_from,
            },
        );

        Ok(())
    }

    
    
    ///список хешей для фильтра событий
    fn get_event_topics() -> Vec<H256> {
        vec![
            H256::from_slice(&keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)")),
            H256::from_slice(&keccak256(b"Mint(address,address,int24,int24,uint128,uint256,uint256)")),
            H256::from_slice(&keccak256(b"Burn(address,int24,int24,uint128,uint256,uint256)")),
            ]
        }

        
        
    pub async fn fetch_events(&self, from_block: u64, to_block: u64) -> anyhow::Result<()> {
    
        sleep(Duration::from_millis(400)).await;
        let subscribed_pools = self.subscribed_pools.iter().map(|addr| *addr).collect::<Vec<_>>();
    
        // 2. Ранний выход с обновлением блока
        if subscribed_pools.is_empty() {
            return Ok(());
        }
    
        // 3. Оптимизированное создание фильтра
        let filter = Filter::new()
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .address(subscribed_pools)
            .topic0(Self::get_event_topics());
    

        // 4. Запрос с агрессивным таймаутом
        let logs = match self.provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(e) => {
                log::warn!("[UNISWAP_EVENT] RPC error: {}", e);
                Vec::new()
            }
        };
    
        let logs_count = logs.len();
    
        // 5. Максимально быстрый парсинг событий
        let mut swap_count = 0;
        let mut mint_count = 0;
        let mut burn_count = 0;
    
        for log in logs {
            let address = log.address;
            if let Some(mut buffer) = self.buffers.get_mut(&address) {
                match log.topics.first().map(|t| t.as_bytes()) {
                    Some(b) if b == SwapEvent::signature().as_bytes() => {
                        let raw_log = RawLog {
                            topics: log.topics,
                            data: log.data.to_vec(),
                        };
                        if let Ok(swap) = <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                            buffer.current_tick = swap.tick;
                            swap_count += 1;
                        }
                    },
                    Some(b) if b == MintEvent::signature().as_bytes() => {
                        let raw_log = RawLog {
                            topics: log.topics,
                            data: log.data.to_vec(),
                        };
                        if let Ok(mint) =<MintEvent as EthLogDecode>::decode_log(&raw_log) {
                            buffer.tick_updates.insert(mint.tick_lower);
                            buffer.tick_updates.insert(mint.tick_upper);
                            mint_count += 1;
                        }
                    },
                    Some(b) if b == BurnEvent::signature().as_bytes() => {
                        let raw_log = RawLog {
                            topics: log.topics,
                            data: log.data.to_vec(),
                        };
                        if let Ok(burn) = <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                            buffer.tick_updates.insert(burn.tick_lower);
                            buffer.tick_updates.insert(burn.tick_upper);
                            burn_count += 1;
                        }
                    },
                    _ => {},
                }
            }
        }
    
        // 6. Атомарное обновление прогресса
        self.last_processed_block.store(to_block, Ordering::Release);
        info!(
            "[UNISWAP_EVENT] Обработано {} событий (Swap: {}, Mint: {}, Burn: {})",
            logs_count, swap_count, mint_count, burn_count
        );
    
        Ok(())
    }
        
        

 
    /// Получает данные из пула и сохраняет их в EventPoolUpdate   
    pub async fn fetch_tick_data(
        &self,
        pool_address: Address,
        mut buffer: BufferUniswapEventSubscriber,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<EventPoolUpdate> {
        // Получаем контракт пула
        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

        sleep(Duration::from_millis(400)).await;

        // Получаем базовые данные (liquidity, slot0, и т.д.)
        let (liquidity, slot0, _, _, _) = uniswap_v3::process_pool_data(pool_address, pool_contract.clone().into())
            .await
            .ok_or_else(|| anyhow::anyhow!("[UNISWAP_EVENT] Ошибка получения данных из fetch_tick_data"))?;

        sleep(Duration::from_millis(310)).await;

        // Принудительно обновляем current_tick из slot0
        let current_tick = slot0.1;
        buffer.current_tick = current_tick;

        // Собираем список тикетов для асинхронных запросов
        let tick_indices: Vec<i32> = buffer.tick_updates.iter()
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
        let current_price: U512 = uniswap_v3::tick_to_sqrt_price(current_tick)
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
        graph: Arc<UniversalGraph>,  
        pool_address: Address,
        ws_provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        // 1. Получаем начальный блок и текущий блок
        sleep(Duration::from_millis(400)).await;
        let (block_from, block_to) = {
            if let Some(buffer) = self.buffers.get(&pool_address) {
                let block_to = ws_provider.get_block_number().await?.as_u64();
                (buffer.block_from, block_to)
            } else {
                return Ok(()); // Пул не подписан
            }
        };
        sleep(Duration::from_millis(400)).await;
        // 2. Обрабатываем события и проверяем нужно ли обновлять буфер
        let need_update = {
            if let Some(buffer) = self.buffers.get_mut(&pool_address) {
                self.fetch_events(block_from, block_to).await?;
                !buffer.tick_updates.is_empty() || buffer.current_tick != 0
            } else {
                false
            }
        };

        if !need_update {
            info!(
                "[UNISWAP_EVENT_GRAPH_NOT_UPDATE] Нет событий для обновления графа для пула {:?}",
                pool_address
            );
            return Ok(());
        }

        sleep(Duration::from_millis(300)).await;

        // 3. Обновляем граф на основе обработанных данных
        let buffer = self.buffers.get(&pool_address).unwrap().value().clone();
        let pool_update = self.fetch_tick_data(pool_address, buffer, ws_provider).await?;

        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;
            pool.tick_map = pool_update.tick_map;
            pool.uniswap_current_price = pool_update.current_price;

            info!(
                "[UNISWAP_EVENT_GRAPH_UPDATE] Обновлен пул {:?} (блоки: {}..{}). Ликвидность: {} | Текущая цена (sqrt): {} | Текущий тик: {}",
                pool_address,
                block_from,
                block_to,
                pool.uniswap_liquidity,
                pool.uniswap_sqrt_price,
                pool.uniswap_tick_current,
            );
        }

        Ok(())
    }
}

/// Подписка на новые блоки с переподключением без подсчёта попыток
pub async fn subscribe_to_new_blocks(
    ws_provider: Arc<Provider<Ws>>,
    mut block_sender: watch::Sender<u64>
) -> anyhow::Result<()> {
    info!("[BLOCKS] Запускаем подписку на новые блоки...");
    const RECONNECT_DELAY: Duration = Duration::from_secs(1);

    // Храним последний отправленный блок для предотвращения отправки одинаковых блоков
    let mut last_sent_block: u64 = 0;
    
    loop {
        match ws_provider.subscribe_blocks().await {
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
 
     
