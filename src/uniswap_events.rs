use std::{collections::{HashMap, HashSet}, sync::{atomic::{AtomicU64, Ordering}, Arc}, time::Duration};

use ethers::{contract::{abigen, EthLogDecode}, providers::{Provider, StreamExt}, types::{Address, BlockNumber, Filter, H256, I256, U256, U512}};
use ethers_providers::{Middleware, Ws, Http};
use ethers::contract::EthEvent;
use ethers::utils::keccak256;

use log::{info, warn};
use tokio::{sync::Mutex, time::sleep};

use crate::{uniswap_graph::UniversalGraph, uniswap_v3::{self, fetch_tick_spacing, UniswapV3Pool}};
     


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
    pub tick_map: HashMap<i32, (i128, U512)>,
    pub current_price: U512,
}

#[derive(Debug, Clone)]
pub struct BufferUniswapEventSubscriber {
    pub pool_address: Address,
    pub tick_updates: HashSet<i32>,
    pub current_tick: Option<i32>,
    pub tick_spacing: i32,
}

#[derive(Debug,Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Http>>,   
    buffers: Arc<Mutex<HashMap<Address, BufferUniswapEventSubscriber>>>,
    subscribed_pools: Arc<Mutex<HashSet<Address>>>,
    last_processed_block: Arc<AtomicU64>, 
}

impl UniswapEventSubscriber {

    ///создает новую подписку
    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        info!("[UNISWAP_EVENT] Создаем подписку на события");
        Self {            
            provider,     
            buffers: Arc::new(Mutex::new(HashMap::new())),
            subscribed_pools: Arc::new(Mutex::new(HashSet::new())),
            last_processed_block: Arc::new(AtomicU64::new(0)),
        }
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

        // 1. Атомарное копирование адресов с минимальной блокировкой
        let subscribed_pools = {
            let guard = self.subscribed_pools.lock().await;
            guard.iter().copied().collect::<Vec<Address>>()
        };
    
        // 2. Ранний выход с обновлением блока
        if subscribed_pools.is_empty() {
            self.last_processed_block.store(to_block, Ordering::Release);
            return Ok(());
        }
    
        // 3. Оптимизированное создание фильтра
        let filter = Filter::new()
            .from_block(BlockNumber::Number(from_block.into()))
            .to_block(BlockNumber::Number(to_block.into()))
            .address(subscribed_pools)
            .topic0(Self::get_event_topics());
    
        // 4. Запрос с агрессивным таймаутом
        let logs = match tokio::time::timeout(
            Duration::from_millis(100),
            self.provider.get_logs(&filter)
        ).await {
            Ok(Ok(logs)) => logs,
            Ok(Err(e)) => {
                log::warn!("RPC error: {}", e);
                Vec::new()
            },
            Err(_) => {
                log::warn!("Timeout fetching blocks {}-{}", from_block, to_block);
                Vec::new()
            }
        };
    
        // 5. Максимально быстрый парсинг событий
        if !logs.is_empty() {
            let mut buffers = self.buffers.lock().await;
            
            for log in logs {
                let address = log.address;
                if let Some(buffer) = buffers.get_mut(&address) {
                    match log.topics.first().map(|t| t.as_bytes()) {
                        Some(b) if b == SwapEvent::signature().as_bytes() => {
                            if let Ok(swap) = <SwapEvent as EthLogDecode>::decode_log(&log.into()) {
                                buffer.current_tick = Some(swap.tick);
                            }
                        },
                        Some(b) if b == MintEvent::signature().as_bytes() => {
                            if let Ok(mint) = <MintEvent as EthLogDecode>::decode_log(&log.into()) {
                                buffer.tick_updates.reserve(2);
                                buffer.tick_updates.insert(mint.tick_lower);
                                buffer.tick_updates.insert(mint.tick_upper);
                            }
                        },
                        Some(b) if b == BurnEvent::signature().as_bytes() => {
                            if let Ok(burn) = <BurnEvent as EthLogDecode>::decode_log(&log.into()) {
                                buffer.tick_updates.reserve(2);
                                buffer.tick_updates.insert(burn.tick_lower);
                                buffer.tick_updates.insert(burn.tick_upper);
                            }
                        },
                        _ => {}
                    }
                }
            }
        }
    
        // 6. Атомарное обновление прогресса
        self.last_processed_block.store(to_block, Ordering::Release);
        Ok(())
    }


        
    /// Добавляет новые пулы в опросник блоков и инициализирует их буферы 
    pub async fn add_pools_to_subscription(
        &self,
        pools: Vec<Address>,
        ws_provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        use futures::stream::{self, StreamExt};

        info!("[ДОБАВЛЕНИЕ ПУЛОВ] НАЧИНАЕМ добавление {:?} пулов в подписку", pools.len());

        let subscribed_pools = self.subscribed_pools.lock().await;

        // Отбираем только те пулы, которых ещё нет в подписке
        let new_pools: Vec<_> = pools
            .iter()
            .filter(|addr| !subscribed_pools.contains(addr))
            .cloned()
            .collect();

        drop(subscribed_pools); // Отпускаем мьютекс до начала асинхронной части

        let provider_clone = ws_provider.clone();
        let subscribed_pools = self.subscribed_pools.clone();
        let buffers = self.buffers.clone();

        // Параллельный запрос tick_spacing и инициализация буфера
        stream::iter(new_pools)
            .for_each_concurrent(10, move |pool_address| {
                let provider = provider_clone.clone();
                let subscribed_pools = subscribed_pools.clone();
                let buffers = buffers.clone();

                async move {
                    let tick_spacing = fetch_tick_spacing(pool_address, provider)
                        .await
                        .unwrap_or_else(|| {
                            warn!("Не удалось получить tick_spacing для пула {:?}", pool_address);
                            0
                        });

                    // Добавляем пул в список подписанных
                    let mut subscribed = subscribed_pools.lock().await;
                    subscribed.insert(pool_address);

                    // Инициализируем буфер с полученным tick_spacing
                    let mut buf = buffers.lock().await;

                    buf.insert(pool_address, BufferUniswapEventSubscriber {
                        pool_address,
                        tick_updates: HashSet::new(),
                        current_tick: None,
                        tick_spacing,
                    });
                }
            })
            .await;

        Ok(())

    }

    
 

    /// Удаляет пул из подписки и удаляет его буфер
    pub async fn remove_pool_from_subscription(&self, pool_address: Address) -> anyhow::Result<()> {
        let mut subscribed_pools = self.subscribed_pools.lock().await;
        let mut buffers = self.buffers.lock().await;
        
        subscribed_pools.remove(&pool_address);
        buffers.remove(&pool_address);
        
        Ok(())
    }  


    /// Получает данные из пула и сохраняет их в EventPoolUpdate   
    async fn fetch_tick_data(
        &self,
        pool_address: Address,
        mut buffer: BufferUniswapEventSubscriber, // делаем buffer изменяемым
        provider: Arc<Provider<Ws>>,
) -> anyhow::Result<EventPoolUpdate> {
        // Получаем контракт пула
        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
        sleep(std::time::Duration::from_millis(1)).await;

        // Получаем базовые данные (liquidity, slot0, и т.д.)
        let (liquidity, slot0, _, _, _) = uniswap_v3::process_pool_data(pool_address, &pool_contract)
            .await
            .ok_or_else(|| anyhow::anyhow!("Ошибка получения данных из fetch_tick_data"))?;
        sleep(std::time::Duration::from_millis(1)).await;

        // Принудительно обновляем current_tick из slot0
        let current_tick = slot0.1;
        buffer.current_tick = Some(current_tick);

        // Собираем карту тиков
        let mut tick_map: HashMap<i32, (i128, U512)> = HashMap::new();
        for tick in buffer.tick_updates {
            let tick_data = pool_contract.ticks(tick).call().await?;
            let sqrt_price = uniswap_v3::tick_to_sqrt_price(tick).map_err(anyhow::Error::msg)?;
            tick_map.insert(tick, (tick_data.1, sqrt_price));
        }
        sleep(std::time::Duration::from_millis(1)).await;

        // Вычисляем текущую цену
        let current_price: U512 = uniswap_v3::tick_to_sqrt_price(current_tick)
            .map_err(anyhow::Error::msg)?;

        info!(
            "[Обновление] Пул {:?}, ликвидность: {:?}, текущий тик: {:?}",
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


    /// Обновляет граф на основе данных из EventPoolUpdate по адресу пула
    pub async fn update_graph_from_event(
        &self,
        graph: Arc<Mutex<UniversalGraph>>,
        pool_address: Address,
        ws_provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<()> {
        let buffer = {
            let mut buffers = self.buffers.lock().await;
            if let Some(buffer) = buffers.get_mut(&pool_address) {
                let buffer_clone = buffer.clone();
                // Очищаем буфер, но не удаляем пул из подписки
                buffer.tick_updates.clear();
                buffer.current_tick = None;
                Some(buffer_clone)
            } else {
                None
            }
        };

        if let Some(buffer) = buffer {
            let pool_update = self.fetch_tick_data(pool_address, buffer, ws_provider).await?;
            
            let mut graph_lock = graph.lock().await;
            if let Some(pool) = graph_lock.edges.get_mut(&pool_address) {
                pool.uniswap_liquidity = pool_update.liquidity;
                pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
                pool.uniswap_tick_current = pool_update.current_tick;
                pool.tick_map = pool_update.tick_map;
                pool.uniswap_current_price = pool_update.current_price;
                
                info!(
                    "[GRAPH] Обновлены данные пула {:?} в графе",
                    pool_address
                );
            }
        }

        Ok(())
    }

     /// Подписка на новые блоки с переподключением без подсчёта попыток
    pub async fn subscribe_to_new_blocks(ws_provider: Arc<Provider<Ws>>) -> anyhow::Result<()> {
        info!("[BLOCKS] Запускаем подписку на новые блоки...");
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);
        loop {
            match ws_provider.subscribe_blocks().await {
                Ok(mut stream) => {
                    info!("[BLOCKS] Подписка на блоки активна");

                    while let Some(block) = stream.next().await {
                        if let Some(number) = block.number {
                            if number.as_u64() % 100 == 0 {
                                info!("[BLOCKS] Новый блок: {}", number);
                            }
                        }
                    }
                    // Если поток завершился — значит соединение разорвано
                    info!("[BLOCKS] Поток блоков завершился. Переподключение...");
                }
                Err(e) => {
                    info!("[BLOCKS] Ошибка подписки: {e}. Переподключение...");
                }
            }

            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }
     
}


