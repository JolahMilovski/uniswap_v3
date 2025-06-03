use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use colored::Colorize;
use dashmap::DashSet;
use ethers::contract::EthEvent;
use ethers::{abi::RawLog, utils::keccak256};
use ethers::{
    contract::EthLogDecode,
    providers::{Http, Middleware, Provider, Ws},
    types::{Address, BlockNumber, Filter, H256, I256, U256, U512},
};
use anyhow::{Context, Result};
use futures::StreamExt;
use im::OrdMap;
use log::{info, warn};
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

pub struct PoolEventInfo {
    pub address: Address,
    pub tick_updates: DashSet<i32>,
    pub current_tick: i32,
    pub block_number: u64,
}

impl UniswapEventSubscriber {
    pub fn new(provider: Arc<Provider<Http>>) -> Self {
        info!("[UNISWAP_EVENT] Создаём подписку на события");
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
            " [UNISWAP_EVENT] Ошибка: from_block ({}) больше to_block ({})",
            from_block, to_block
        );
    }

    let subscribed_pool_addresses: Vec<Address> = self
        .subscribed_pools
        .iter()
        .map(|entry| *entry.key())
        .collect();

    if subscribed_pool_addresses.is_empty() {
        info!("[UNISWAP_EVENT] Нет подписанных пулов");
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
            warn!("[UNISWAP_EVENT] Ошибка RPC: {}", e);
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
        let block_number = log.block_number.map_or("неизвестен".to_string(), |n| n.as_u64().to_string());

    let entry = event_map.entry(address).or_insert_with(|| {

    let block_number_u64 = log.block_number.map(|n| n.as_u64()).unwrap_or(0);

        PoolEventInfo {
            address,
            tick_updates: DashSet::new(),
            current_tick: 0,
            block_number: block_number_u64,
        }
    });


        // Сравнение темы события как H256
        match log.topics.first() {
            Some(topic) if *topic == SwapEvent::signature() => {
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                match <SwapEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(swap) => {
                        info!(
                            "[UNISWAP_EVENT] Swap: пул {:?}, блок {}, amount0: {}, amount1: {}, sqrtPriceX96: {}, ликвидность: {}, тик: {}",
                            address, block_number, swap.amount0, swap.amount1, swap.sqrt_price_x96, swap.liquidity, swap.tick
                        );
                        entry.current_tick = swap.tick;
                        swap_count += 1;
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT] Ошибка декодирования Swap: {:?}", e);
                    }
                }
            }
            Some(topic) if *topic == MintEvent::signature() => {
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                match <MintEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(mint) => {
                        info!(
                            "[UNISWAP_EVENT] Mint: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}",
                            address, block_number, mint.tick_lower, mint.tick_upper, mint.liquidity, mint.amount0, mint.amount1
                        );
                        entry.tick_updates.extend([mint.tick_lower, mint.tick_upper]);
                        mint_count += 1;
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT] Ошибка декодирования Mint: {:?}", e);
                    }
                }
            }
            Some(topic) if *topic == BurnEvent::signature() => {
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                match <BurnEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(burn) => {
                        info!(
                            "[UNISWAP_EVENT] Burn: пул {:?}, блок {}, tick_lower: {}, tick_upper: {}, ликвидность: {}, amount0: {}, amount1: {}",
                            address, block_number, burn.tick_lower, burn.tick_upper, burn.liquidity, burn.amount0, burn.amount1
                        );
                        entry.tick_updates.extend([burn.tick_lower, burn.tick_upper]);
                        burn_count += 1;
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT] Ошибка декодирования Burn: {:?}", e);
                    }
                }
            }
            Some(topic) if *topic == FlashEvent::signature() => {
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                match <FlashEvent as EthLogDecode>::decode_log(&raw_log) {
                    Ok(flash) => {
                        info!(
                            "[UNISWAP_EVENT] Flash: пул {:?}, блок {}, заимствовано {} token0, {} token1, уплачено {} token0, {} token1",
                            address, block_number, flash.amount0, flash.amount1, flash.paid0, flash.paid1
                        );
                        flash_count += 1;
                    }
                    Err(e) => {
                        warn!("[UNISWAP_EVENT] Ошибка декодирования Flash: {:?}", e);
                    }
                }
            }
            _ => {}
        }
    }

    self.last_processed_block.store(to_block, Ordering::Release);
    if swap_count > 0 || mint_count > 0 || burn_count > 0 || flash_count > 0 {
        let pool_addresses: Vec<String> = event_map.keys().map(|addr| format!("{:?}", addr)).collect();
        let pools_str = format!("{} пулов", pool_addresses.len());
        info!(
            "[{}][Блоки {}-{}] Обработано {} событий (Swap: {}, Mint: {}, Burn: {}, Flash: {}) для {}",
            "UNISWAP_EVENT".bright_blue(),
            from_block,
            to_block,
            swap_count + mint_count + burn_count + flash_count,
            swap_count,
            mint_count,
            burn_count,
            flash_count,
            pools_str
        );
    }
    Ok(event_map.into_values().collect())
}




    /// Получает данные о тиках пула Uniswap V3
    pub async fn fetch_tick_data(
        &self,




        pool_event_info: &PoolEventInfo,      // Информация о событиях пула
        pool_address: Address,                 // Адрес пула в сети
        provider: Arc<Provider<Ws>>,          // Web3 провайдер для взаимодействия с сетью
        graph: Arc<UniversalGraph>,           // Граф с информацией о пулах
    ) -> anyhow::Result<EventPoolUpdate> {
        // Создаем экземпляр контракта пула
        let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
        
        // Пауза для избежания превышения лимита запросов
        sleep(Duration::from_millis(200)).await;
        
        // Получаем основные данные пула: ликвидность и slot0
        let (liquidity, slot0, _, _, _) = process_pool_data(pool_address, pool_contract.clone().into())
            .await
            .context(format!(
                "[UNISWAP_EVENT] Не удалось получить данные пула для: {:?}", 
                pool_address
            ))?;
            
        // Извлекаем текущий тик из slot0
        let current_tick = slot0.1;
        
        // Получаем информацию о пуле из графа
        let pool_info = graph.edges.get(&pool_address)
            .ok_or_else(|| anyhow::anyhow!("Пул {:?} не найден в графе", pool_address))?;
            
        // Еще одна пауза перед следующей серией запросов
        sleep(Duration::from_millis(200)).await;
        
        // Преобразуем обновленные тики в вектор
        let tick_indices: Vec<i32> = pool_event_info
            .tick_updates
            .iter()
            .map(|tick| *tick)
            .collect();
            
        // Создаем футуры для параллельного запроса данных по каждому тику
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
            
        // Выполняем все запросы параллельно
        let tick_results = futures::future::join_all(tick_futures).await;
        
        // Создаем упорядоченную карту для хранения данных тиков
        let mut tick_map = OrdMap::new();
        
        // Заполняем карту данными полученных тиков
        for result in tick_results {
            if let Some((tick, data)) = result {
                tick_map.insert(tick, (data.1, U512::from(data.0)));
            }
        }

        // Преобразуем sqrt_price в U512
        let sqrt_price = U512::from(slot0.0);
        
        // Вычисляем текущую цену на основе sqrt_price и десятичных знаков токенов
        let current_price = calculate_current_price(sqrt_price, pool_info.uniswap_token_a_decimals, pool_info.uniswap_token_b_decimals)
            .map_err(anyhow::Error::msg)?;
            
        // Логируем информацию об обновлении пула
        info!(
            "[{}] Обновление пула: {:?}, Ликвидность: {}, Текущий тик: {}, Цена: {}",
            "UNISWAP_EVENT".green(),
            pool_address, liquidity, current_tick, current_price
        );
        
        // Возвращаем структуру с обновленными данными пула
        Ok(EventPoolUpdate {
            liquidity,
            sqrt_price_x96: slot0.0,
            current_tick,
            tick_map,
            current_price,
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
        // Загружаем свежие данные из Uniswap V3
        let pool_update = self
            .fetch_tick_data(pool_event_info, pool_address, provider.clone(), graph.clone())
            .await?;

        // Проверяем наличие пула в графе
        if let Some(mut pool) = graph.edges.get_mut(&pool_address) {
            info!(
                "[{}] Для пула: {:?}. Обновление графа начато.", "EVENT_UPDATE_GRAPH".red(), 
                pool_address
            );

            // Обновление данных
            pool.uniswap_liquidity = pool_update.liquidity;
            pool.uniswap_sqrt_price = pool_update.sqrt_price_x96.into();
            pool.uniswap_tick_current = pool_update.current_tick;
            pool.uniswap_current_price = pool_update.current_price;

            // Tick map объединяется
            pool.tick_map = pool.tick_map.clone().union(pool_update.tick_map);

            // Финальный лог об обновлении
            info!(
                "[{}] Обновлен пул {} данными блока {} : Ликвидность: {}, Цена (sqrt): {}, Текущий тик: {}, Цена: {}",
                "UNISWAP_EVENT".green(),
                pool_address,
                pool_event_info.block_number,
                pool.uniswap_liquidity,
                pool.uniswap_sqrt_price,
                pool.uniswap_tick_current,
                pool.uniswap_current_price
            );
        } else {
            // Пул не найден в графе — предупреждение
            warn!(
                "[EVENT_UPDATE_GRAPH] Пул {:?} не найден в графе. Обновление пропущено.",
                pool_address
            );
        }

        Ok(())
    }   

    /// Функция для обработки событий Uniswap V3
    pub async fn polling_event(
        &self,
        graph: Arc<UniversalGraph>,
        provider_ws: Arc<Provider<Ws>>,
        block_receiver: &watch::Receiver<u64>,
    ) -> anyhow::Result<()> {
        // Получаем начальный блок из приемника блоков
        let mut block_from = *block_receiver.borrow();
        // Максимальный размер чанка для запроса событий
        let max_chunk_size: u64 = 200;

        loop {
            // Получаем список подписанных пулов
            let subscribed_pools = self.subscribed_pools.clone();
            // Проверяем, есть ли пулы для обработки
            if subscribed_pools.is_empty() {
                warn!("[UNISWAP_EVENT] Нет пулов для обработки");
                sleep(Duration::from_secs(3)).await;
                continue;
            }

            // Получаем текущий последний блок
            let block_to = *block_receiver.borrow();
            // Проверяем корректность диапазона блоков
            if block_to < block_from {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Инициализируем переменные для обработки чанков
            let mut current_from = block_from;
            let mut all_events = Vec::new();

            // Обрабатываем блоки чанками
            while current_from <= block_to {
                // Вычисляем конец текущего чанка
                let current_to = current_from.min(block_to + max_chunk_size - 1);

                // Получаем события для текущего чанка
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

            // Создаем список уникальных адресов пулов из событий
            let unique_pools: Vec<_> = all_events
                .iter()
                .map(|e| format!("{:?}", e.address))
                .collect();

            // Обрабатываем каждое событие
            for pool_event in all_events {
                // Получаем адрес пула из события
                let pool_address = pool_event.address;

                // Обновляем граф на основе события
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

            // Логируем информацию об обработанных блоках
            info!(
                "[{}] Обработаны блоки {}–{}, пулов: {}",
                "UNISWAP_EVENT".bright_blue(),
                block_from,
                block_to,
                unique_pools.len()
            );

            // Обновляем начальный блок для следующей итерации
            block_from = block_to + 1;
            // Делаем паузу перед следующей итерацией
            sleep(Duration::from_secs(1)).await;
        }
    }
}