use std::{collections::{HashMap, HashSet}, sync::Arc};

use ethers::{contract::{abigen, EthLogDecode}, providers::{Provider, StreamExt}, types::{Address, Filter, I256, H256, U256, U512}};
use ethers_providers::{Middleware, Ws};
use ethers::contract::EthEvent;
use ethers::utils::keccak256;

use log::info;
use tokio::sync::Mutex;
use tokio::sync::{Notify, oneshot};

use crate::{uniswap_graph::UniversalGraph, uniswap_v3::{self, UniswapV3Pool}};
     


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
    pub start_block: u64,
    pub last_merged_block: u64,
    pub tick_updates: HashSet<i32>,
    pub current_tick: Option<i32>,
    pub tick_spacing: i32,
}

#[derive(Debug,Clone)]
pub struct UniswapEventSubscriber {
    provider: Arc<Provider<Ws>>,   
    buffers: Arc<Mutex<HashMap<Address, BufferUniswapEventSubscriber>>>,
    subscribed_pools: Arc<Mutex<HashSet<Address>>>,
    notify: Arc<Notify>,
}

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


impl UniswapEventSubscriber {

    ///создает новую подписку
    pub fn new(provider: Arc<Provider<Ws>>) -> Self {
        info!("[UNISWAP_EVENT] Создаем подписку на события");
        Self {            
            provider,     
            buffers: Arc::new(Mutex::new(HashMap::new())),
            subscribed_pools: Arc::new(Mutex::new(HashSet::new())),
            notify: Arc::new(Notify::new()),  // Инициализация Notify для синхронизации фильтров
        }
    }
    
    fn get_event_topics() -> Vec<H256> {
        vec![
            H256::from_slice(&keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)")),
            H256::from_slice(&keccak256(b"Mint(address,address,int24,int24,uint128,uint256,uint256)")),
            H256::from_slice(&keccak256(b"Burn(address,int24,int24,uint128,uint256,uint256)")),
        ]
    }

    pub async fn subscribe_to_pool_events(
        &self,        
        initial_pools: Vec<Address>,
        cancel_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        info!("[UNISWAP_EVENT] Запускаем подписку на события");
        
        let topics = Self::get_event_topics();
        
        let provider = self.provider.clone();
        let notify = self.notify.clone();
        let buffers = self.buffers.clone();
        let subscribed_pools_arc = self.subscribed_pools.clone();
    
        tokio::spawn(async move {
            if !initial_pools.is_empty() {
                let mut pools = subscribed_pools_arc.lock().await;
                pools.extend(initial_pools);
                drop(pools);
            }
            
            let worker = async {
                loop {
                    info!("[UNISWAP_EVENT] Ждем уведомления об обновлении пулов");
                    notify.notified().await;
    
                    info!("[UNISWAP_EVENT] Получаем текущий список пулов");
                    let subscribed_pools = subscribed_pools_arc.lock().await.clone();
                    if subscribed_pools.is_empty() {
                        continue;
                    }
    
                    let mut filter = Filter::new();
                    for pool_address in &subscribed_pools {
                        filter = filter.address(*pool_address);
                    }
                    
                    for topic in &topics {
                        filter = filter.topic0(*topic);
                    }
    
                    info!("[UNISWAP_EVENT] Подписываемся на события");
                    let mut stream = match provider.subscribe_logs(&filter).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("Failed to subscribe to logs: {:?}", e);
                            continue;
                        }
                    };
    
                    let next_notification = notify.notified();
                    tokio::pin!(next_notification);
    
                    loop {
                        tokio::select! {
                            Some(log) = stream.next() => {
                                info!("[UNISWAP_EVENT] Обрабатываем событие");
                                let pool_address = log.address;
                                
                                if !subscribed_pools.contains(&pool_address) {
                                    continue;
                                }
                                
                                let mut buffers = buffers.lock().await;
                                if let Some(buffer) = buffers.get_mut(&pool_address) {
                                    match log.topics.first().cloned().unwrap_or_default() {
                                        t if t == H256::from_slice(&keccak256(b"Swap(address,address,int256,int256,uint160,uint128,int24)")) => {
                                            if let Ok(swap) = <SwapEvent as EthLogDecode>::decode_log(&log.clone().into()) {
                                                buffer.current_tick = Some(swap.tick);
                                            }
                                        }
                                        t if t == H256::from_slice(&keccak256(b"Mint(address,address,int24,int24,uint128,uint256,uint256)")) => {
                                            if let Ok(mint) = <MintEvent as EthLogDecode>::decode_log(&log.clone().into()) {
                                                buffer.tick_updates.insert(mint.tick_lower);
                                                buffer.tick_updates.insert(mint.tick_upper);
                                            }
                                        }
                                        t if t == H256::from_slice(&keccak256(b"Burn(address,int24,int24,uint128,uint256,uint256)")) => {
                                            if let Ok(burn) = <BurnEvent as EthLogDecode>::decode_log(&log.clone().into()) {
                                                buffer.tick_updates.insert(burn.tick_lower);
                                                buffer.tick_updates.insert(burn.tick_upper);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            },
                            
                            _ = &mut next_notification => {
                                info!("[UNISWAP_EVENT] прерываем обработку");
                                break;
                            }
                        }
                    }
                }
            };
    
            tokio::select! {
                _ = worker => {
                    info!("[UNISWAP_EVENT] Подписка на события завершена");
                }
                _ = cancel_rx => {
                    info!("[UNISWAP_EVENT] Подписка отменена");
                }
            }
        });
    
        Ok(())
    }



    pub async fn add_pools_to_subscription(&self, pools: Vec<Address>) -> anyhow::Result<()> {

        info!("[ДОБАВЛЕНИЕ ПУЛОВ] НАЧИНАЕМ добавление {:?} пулов в подписку", pools.len());

        let mut subscribed_pools = self.subscribed_pools.lock().await;

        for pool_address in &pools {
            if !subscribed_pools.contains(&pool_address) {
                
                // Добавляем новый пул в множество подписанных пулов
                subscribed_pools.insert(pool_address.clone());

                // Инициализируем буфер для нового пула
                let mut buffers = self.buffers.lock().await;
                buffers.insert(pool_address.clone(), BufferUniswapEventSubscriber {
                    pool_address: pool_address.clone(),
                    start_block: 0,
                    last_merged_block: 0,
                    tick_updates: HashSet::new(),
                    current_tick: None,
                    tick_spacing: 0,
                });
            }
        }

        // Уведомляем подписчика об обновлении фильтра
        self.notify.notify_one();

        Ok(())
    }
 

    pub async fn remove_pool_from_subscription(
        &self,
        pool_address: Address,
    ) -> anyhow::Result<Option<EventPoolUpdate>> {

        // 1. Удаляем пул из подписки и забираем его буфер
        let (buffer, provider) = {
            let mut subscribed_pools = self.subscribed_pools.lock().await;
            let mut buffers = self.buffers.lock().await;
            
            subscribed_pools.remove(&pool_address);
            let buffer = buffers.remove(&pool_address);
            
            (buffer, self.provider.clone())
        };

        info!("[УДАЛЕНИЕ ПОДА] Удалили пул {:?} из подписки", pool_address);

        // 2. Перезапускаем подписку
        self.notify.notify_one();

        // 3. Если буфер был, собираем полные данные
        if let Some(buffer) = buffer {
            let pool_update = self.fetch_tick_data(pool_address, buffer, provider).await?;
            Ok(Some(pool_update))
        } else {
            Ok(None)
        }
    }
    
    async fn fetch_tick_data(
        &self,
        pool_address: Address,
        buffer: BufferUniswapEventSubscriber,
        provider: Arc<Provider<Ws>>,
    ) -> anyhow::Result<EventPoolUpdate> {

        // Получаем контракт пула
        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Получаем базовые данные (liquidity, slot0, и т.д.)
        let (liquidity, slot0, _, _, _) = uniswap_v3::process_pool_data(pool_address, &pool_contract)
            .await
            .ok_or_else(|| anyhow::anyhow!("Ошибка получения данных из fetch_tick_data"))?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Собираем карту тиков
        let mut tick_map: HashMap<i32, (i128, U512)> = HashMap::new();
        for tick in buffer.tick_updates {
            let tick_data = pool_contract.ticks(tick).call().await?;
            let sqrt_price = uniswap_v3::tick_to_sqrt_price(tick).map_err(anyhow::Error::msg)?;
            tick_map.insert(tick, (tick_data.1, sqrt_price));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Вычисляем текущую цену
        let current_price: U512 = match buffer.current_tick {
            Some(current_tick) => {
                uniswap_v3::tick_to_sqrt_price(current_tick).map_err(anyhow::Error::msg)?
            }
            None => slot0.0.into(),
        };
    
        info!("[Обновление] Пул {:?}, ликвидность: {:?}, текущий тик: {:?}", pool_address, liquidity, buffer.current_tick);
    
        // Возвращаем структуру
        Ok(EventPoolUpdate {
            liquidity: liquidity.into(),
            sqrt_price_x96: slot0.0.into(),
            current_tick: buffer.current_tick.unwrap_or(slot0.1),
            tick_map,
            current_price,
        })
    }
    
    pub async fn update_graph(
        &self,
        graph: Arc<Mutex<UniversalGraph>>,
        pool_address: Address,
    ) {
        if let Ok(Some(update)) = self.remove_pool_from_subscription(pool_address).await {
            let mut graph_lock = graph.lock().await;

            if let Some(pool) = graph_lock.edges.get_mut(&pool_address) {
                pool.uniswap_liquidity = update.liquidity;
                pool.uniswap_sqrt_price = update.sqrt_price_x96.into();
                pool.uniswap_tick_current = update.current_tick;
                pool.tick_map = update.tick_map;
                pool.uniswap_current_price = update.current_price;
                
                info!(
                    "[GRAPH] Мержим данные из пула {:?} в граф:\n\t\tliquidity: {:?}\n\t\tsqrt_price_x96: {:?}\n\t\tcurrent_tick: {:?}\n\t\ttick_map: {:?}\n\t\tcurrent_price: {:?}",
                    pool_address,
                    pool.uniswap_liquidity,
                    pool.uniswap_sqrt_price,
                    pool.uniswap_tick_current,
                    pool.tick_map,
                    pool.uniswap_current_price,
                );
            }
        }
    }  
}

