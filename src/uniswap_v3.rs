use crate::uniswap_events::UniswapEventSubscriber;
use crate::uniswap_graph::UniversalGraph;
use crate::uniswap_graph::UniswapPool;
use crate::token::TokenCache;
use crate::token::get_single_token_data;
use crate::uniswap_cache::UniswapPoolCache;
use crate::get_env_var;

use cfmms::dex::uniswap_v3::UniswapV3Dex;
use cfmms::{
    dex::Dex,
    pool::Pool as CfmmsPool,
    sync::sync_pairs,
};
use ethers::contract::abigen;
use ethers::providers::{Middleware, Provider, Ws,};
use ethers::types::BlockNumber;
use ethers::types::H160;
use ethers::types::{Address, U512};
use log::warn;
use tokio::time::sleep;

use std::collections::{HashSet, HashMap};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinSet;

use lazy_static::lazy_static;
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use futures::{stream, StreamExt};

abigen!(
    UniswapV3Pool,
    r#"[{
        "constant": true,
        "inputs": [],
        "name": "maxLiquidityPerTick",
        "outputs": [
            { "internalType": "uint128", "name": "", "type": "uint128" }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"name": "", "type": "uint128"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },  {
        "constant": true,
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"}
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }, {
        "constant": true,
        "inputs": [],
        "name": "protocol_fees",
        "outputs": [
            {"name": "token0", "type": "uint128"},
            {"name": "token1", "type": "uint128"}
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }, {
        "constant": true,
        "inputs": [],
        "name": "fee",
        "outputs": [{"name": "", "type": "uint24"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }, {
        "constant": true,
        "inputs": [],
        "name": "tickSpacing",
        "outputs": [{"name": "", "type": "int24"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }, {
    "inputs": [{"internalType":"int24","name":"","type":"int24"}],
        "name": "ticks",
        "outputs": [
            {"internalType":"uint128","name":"liquidityGross","type":"uint128"},
            {"internalType":"int128","name":"liquidityNet","type":"int128"},
            {"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},
            {"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},
            {"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},
            {"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},
            {"internalType":"uint32","name":"secondsOutside","type":"uint32"},
            {"internalType":"bool","name":"initialized","type":"bool"}
        ],
        "stateMutability": "view",
        "type": "function"
    }, {
        "constant": true,
        "inputs": [{"name": "word", "type": "int16"}],
        "name": "tickBitmap",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    
    },{
    "constant": true,
    "inputs": [],
    "name": "token0",
    "outputs": [
        { "internalType": "address", "name": "", "type": "address" }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
},{
    "constant": true,
    "inputs": [],
    "name": "token1",
    "outputs": [
        { "internalType": "address", "name": "", "type": "address" }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
}    
    
    ]"#
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
/// Функция для получения текущего блока
pub async fn get_current_block(provider: Arc<Provider<Ws>>) -> Result<u64, Box<dyn std::error::Error>> {
    let block_number = provider.get_block_number().await?;
    Ok(block_number.as_u64())
}

/// Функция для расчета текущей цены
pub fn calculate_current_price(
    sqrt_price: U512,
    token0_decimals: u8,
    token1_decimals: u8
) -> Result<U512, String> {
    // 1. Проверка входных данных
    if sqrt_price.is_zero() {
        return Err("sqrt_price_x96 is zero".into());
    }

    // 2. Вычисление с максимальной точностью
    let sqrt_price_squared = sqrt_price.checked_pow(U512::from(2))
        .ok_or("Overflow in squaring")?;
    
    // 3. Масштабирование перед делением
    let scale_factor = U512::from(10).pow(U512::from(token1_decimals.max(token0_decimals) + 18));
    let scaled_price = sqrt_price_squared.checked_mul(scale_factor)
        .ok_or("Overflow in scaling")? / (U512::one() << 192);
    
    // 4. Коррекция decimals
    let decimals_adjustment = i32::from(token1_decimals) - i32::from(token0_decimals);
    let final_price = if decimals_adjustment > 0 {
        scaled_price.checked_mul(
            U512::from(10).pow(U512::from(decimals_adjustment as u32)))
    } else {
        scaled_price.checked_div(
            U512::from(10).pow(U512::from((-decimals_adjustment) as u32)))
    }.ok_or("Overflow in decimals adjustment")?;
    
    Ok(final_price)
}

// Q96
lazy_static! {
    static ref Q96: U512 = U512::from(1u128) << 96;
 }


/// преобразует тик в sqrt_price_x96
pub fn tick_to_sqrt_price(tick: i32) -> Result<U512, String> {

    if tick < -887272 || tick > 887272 {
        return Err("Tick out of bounds".to_string());
    }

    let abs_tick = tick.unsigned_abs() as u32;
    let mut ratio = if abs_tick & 0x1 != 0 {
        U512::from_str("0xfffcb933bd6fad37aa2d162d1a594001").unwrap()
    } else {
        U512::from_str("0x100000000000000000000000000000000").unwrap()
    };

    if abs_tick & 0x2 != 0 {
        ratio = (ratio * U512::from_str("0xfff97272373d413259a46990580e213a").unwrap()) >> 128;
    }                                    
    if abs_tick & 0x4 != 0 {
        ratio = (ratio * U512::from_str("0xfff2e50f5f656932ef12357cf3c7fdcc").unwrap()) >> 128;
    }
    if abs_tick & 0x8 != 0 {
        ratio = (ratio * U512::from_str("0xffe5caca7e10e4e61c3624eaa0941cd0").unwrap()) >> 128;
    }
    if abs_tick & 0x10 != 0 {
        ratio = (ratio * U512::from_str("0xffcb9843d60f6159c9db58835c926644").unwrap()) >> 128;
    }
    if abs_tick & 0x20 != 0 {
        ratio = (ratio * U512::from_str("0xff973b41fa98c081472e6896dfb254c0").unwrap()) >> 128;
    }
    if abs_tick & 0x40 != 0 {
        ratio = (ratio * U512::from_str("0xff2ea16466c96a3843ec78b326b52861").unwrap()) >> 128;
    }
    if abs_tick & 0x80 != 0 {
        ratio = (ratio * U512::from_str("0xfe5dee046a99a2a811c461f1969c3053").unwrap()) >> 128;
    }
    if abs_tick & 0x100 != 0 {
        ratio = (ratio * U512::from_str("0xfcbe86c7900a88aedcffc83b479aa3a4").unwrap()) >> 128;
    }
    if abs_tick & 0x200 != 0 {
        ratio = (ratio * U512::from_str("0xf987a7253ac413176f2b074cf7815e54").unwrap()) >> 128;
    }
    if abs_tick & 0x400 != 0 {
        ratio = (ratio * U512::from_str("0xf3392b0822b70005940c7a398e4b70f3").unwrap()) >> 128;
    }
    if abs_tick & 0x800 != 0 {
        ratio = (ratio * U512::from_str("0xe7159475a2c29b7443b29c7fa6e889d9").unwrap()) >> 128;
    }
    if abs_tick & 0x1000 != 0 {
        ratio = (ratio * U512::from_str("0xd097f3bdfd2022b8845ad8f792aa5825").unwrap()) >> 128;
    }
    if abs_tick & 0x2000 != 0 {
        ratio = (ratio * U512::from_str("0xa9f746462d870fdf8a65dc1f90e061e5").unwrap()) >> 128;
    }
    if abs_tick & 0x4000 != 0 {
        ratio = (ratio * U512::from_str("0x70d869a156d2a1b890bb3df62baf32f7").unwrap()) >> 128;
    }
    if abs_tick & 0x8000 != 0 {
        ratio = (ratio * U512::from_str("0x31be135f97d08fd981231505542fcfa6").unwrap()) >> 128;
    }
    if abs_tick & 0x10000 != 0 {
        ratio = (ratio * U512::from_str("0x9aa508b5b7a84e1c677de54f3e99bc9").unwrap()) >> 128;
    }
    if abs_tick & 0x20000 != 0 {
        ratio = (ratio * U512::from_str("0x5d6af8dedb81196699c329225ee604").unwrap()) >> 128;
    }
    if abs_tick & 0x40000 != 0 {
        ratio = (ratio * U512::from_str("0x2216e584f5fa1ea926041bedfe98").unwrap()) >> 128;
    }
    if abs_tick & 0x80000 != 0 {
        ratio = (ratio * U512::from_str("0x48a170391f7dc42444e8fa2").unwrap()) >> 128;
    }                                    

    if tick > 0 {
        ratio = U512::MAX / ratio;
    }

    Ok(ratio >> 32)

    }
    

/// Получаем активные тики из пула Uniswap V3  
pub async fn fetch_active_ticks(
    pool_address: Address,
    client: Arc<Provider<Ws>>,
    current_tick: i32,
    fee: u32,
) -> Result<HashMap<i32, (i128, U512)>, anyhow::Error>  {

    let tick_lens_address: Address = env::var("UNISWAP_TICK_LENS_ADDRESS")?.parse()?;
    let tick_lens = Arc::new(TickLens::new(tick_lens_address, client.clone()));

    let current_word = current_tick / 256;

    // Параметры батчинга по fee
    let (total_batches, words_per_batch) = match fee {
        100 => (80, 20),
        500 => (50, 20),
        3000 => (10, 6),
        10_000 => (2, 20),
        _ => (10, 5), // дефолт
    };

    let left_active = Arc::new(AtomicUsize::new(0));
    let right_active = Arc::new(AtomicUsize::new(0));

    let mut set = JoinSet::new();

    // Центральное слово
    {
        let tick_lens = tick_lens.clone();
        let pool_address = pool_address;
        set.spawn(async move {
            let mut ticks = HashMap::new();
            if let Ok(list) = tick_lens.get_populated_ticks_in_word(pool_address, current_word.try_into().unwrap()).call().await {
                for tick in list {
                  sleep(std::time::Duration::from_millis(100)).await;
                    if let Ok(price) = tick_to_sqrt_price(tick.tick) {
                        ticks.insert(tick.tick, (tick.liquidity_net, price));
                    }
                }
            }
            ticks
        });
    }
    sleep(Duration::from_millis(100)).await;
    // Левая сторона
    for batch in 0..total_batches {
        let base_word = current_word - ((batch * words_per_batch) as i32);
        let tick_lens = tick_lens.clone();
        let pool_address = pool_address;
        let left_active = left_active.clone();
        set.spawn(async move {
            let mut ticks = HashMap::new();
            for i in 0..words_per_batch {
                sleep(std::time::Duration::from_millis(100)).await;
                let word = base_word - (i as i32);
                if let Ok(list) = tick_lens.get_populated_ticks_in_word(pool_address, word.try_into().unwrap()).call().await {
                    let mut count = 0;
                    for tick in list {                        
                        if let Ok(price) = tick_to_sqrt_price(tick.tick) {
                            ticks.insert(tick.tick, (tick.liquidity_net, price));
                            count += 1;
                        }
                    }
                    left_active.fetch_add(count, Ordering::Relaxed);
                }
            }
            ticks
        });
    }
    sleep(Duration::from_millis(100)).await;
    // Правая сторона
    for batch in 0..total_batches {
        let base_word = current_word + ((batch * words_per_batch) as i32);
        let tick_lens = tick_lens.clone();
        let pool_address = pool_address;
        let right_active = right_active.clone();
        set.spawn(async move {
            let mut ticks = HashMap::new();
            for i in 0..words_per_batch {
                sleep(std::time::Duration::from_millis(100)).await;
                let word = base_word + (i as i32);
                if let Ok(list) = tick_lens.get_populated_ticks_in_word(pool_address, word.try_into().unwrap()).call().await {
                    let mut count = 0;
                    for tick in list {
                        if let Ok(price) = tick_to_sqrt_price(tick.tick) {
                            ticks.insert(tick.tick, (tick.liquidity_net, price));
                            count += 1;
                        }
                    }
                    right_active.fetch_add(count, Ordering::Relaxed);
                }
            }
            ticks
        });
    }

    let mut all_ticks = HashMap::new();
    while let Some(Ok(partial)) = set.join_next().await {
        all_ticks.extend(partial);
    }

    let non_zero_prices: usize = all_ticks.values().filter(|(_, sqrt)| !sqrt.is_zero()).count();

    if !all_ticks.is_empty() && non_zero_prices > 0 {
        info!(
            "[[СИНХРОНИЗАЦИЯ]{:?}] Fee: {}, Батчи: {}×{}, Тики: {} (←{} →{}), С ценой ≠ 0: {}",
            pool_address,
            fee,
            total_batches,
            words_per_batch,
            left_active.load(Ordering::Relaxed) + right_active.load(Ordering::Relaxed),
            left_active.load(Ordering::Relaxed),
            right_active.load(Ordering::Relaxed),
            non_zero_prices
        );
    }

    Ok(all_ticks)



}

pub async fn sync_pools(
    graph: Arc<Mutex<UniversalGraph>>,
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
    pool_cache: Arc<Mutex<UniswapPoolCache>>,
    token_whitelist: &HashSet<Address>,
    start_block_from_env: u64,
    event_subscriber: Arc<UniswapEventSubscriber>,
) -> Result<(), Box<dyn std::error::Error>> {
    // === Фаза 1: обработка пулов из кэша ===
    let (original_addresses, original_count) = {
        let pool_cache_lock = pool_cache.lock().await;
        (pool_cache_lock.pool_addresses.clone(), pool_cache_lock.pool_addresses.len())
    };

    info!("[КЭШ] Начинаем обработку {} пулов из кэша", original_count);

    let phase1_active_count = Arc::new(AtomicUsize::new(0));
    let progress = ProgressBar::new(original_count as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.pink/red}] {pos}/{len} из кеша")
            .unwrap()
            .progress_chars("=>-"),
    );

    stream::iter(original_addresses)
    .for_each_concurrent(1, |addr| {
        let provider = provider.clone();
        let token_cache = Arc::clone(&token_cache);
        let graph = Arc::clone(&graph);
        let progress = progress.clone();
        let phase1_active_count = phase1_active_count.clone();
        let token_whitelist = token_whitelist.clone();

        let event_subscriber = Arc::clone(&event_subscriber);

        async move {
            let pool_contract = UniswapV3Pool::new(addr, provider.clone());

            let token0_call = pool_contract.token_0();
            let token1_call = pool_contract.token_1();

            match tokio::try_join!(
                token0_call.call(),
                token1_call.call()                                                                      
                ) {
                Ok((token0, token1)) if token_whitelist.contains(&token0) && token_whitelist.contains(&token1) => {
                    info!("[КЭШ] Пул {:?} проходит whitelist: {:?} ↔ {:?}", addr, token0, token1);

                    let _ = event_subscriber.add_pools_to_subscription(vec![addr]).await;
                   
                    if let Some(pool) = build_uniswap_v3_pool(
                        addr,
                        (token0, token1),
                        provider.clone(),
                        &token_cache,
                    ).await {
                        if pool.is_active {
                            graph.lock().await.upsert_pool(pool.clone());

                            event_subscriber.update_graph(graph.clone(), addr).await;

                            phase1_active_count.fetch_add(1, Ordering::SeqCst);                       
                        }
                    }
                },
                Ok(_) => info!("[КЭШ] Пул {:?} отфильтрован по whitelist", addr),

                Err(e) => warn!("[КЭШ] Ошибка проверки токенов пула {:?}: {:?}", addr, e),
            }
            progress.inc(1);
        }
    }).await;

    progress.finish_with_message("[КЭШ]✅ Пулы из кеша обработаны");

    // === Фаза 2: обработка новых пулов с фабрики ===========================================================================================================================

    let (factory_address, start_block) = {
        let pool_cache_lock = pool_cache.lock().await;
        let factory_address: Address = get_env_var("UNISWAP_V3_FACTORY").parse()?;
        let start_block_in = if pool_cache_lock.pool_addresses.is_empty() {
            start_block_from_env
        } else {
            pool_cache_lock.last_verified_block
        };
        (factory_address, start_block_in)
    };

    info!("[СИНХРОНИЗАЦИЯ] Сканируем новые пулы с блока {}", start_block);
    let dex = Dex::UniswapV3(UniswapV3Dex::new(factory_address, BlockNumber::Number(start_block.into())));
    let all_new = sync_pairs(vec![dex], provider.clone(), None).await?;

    let progress2 = ProgressBar::new(all_new.len() as u64);
    let phase2_active_count = Arc::new(AtomicUsize::new(0));

    stream::iter(all_new)
    .for_each_concurrent(10, |pool| {
        let provider = provider.clone();
        let token_cache = Arc::clone(&token_cache);
        let pool_cache = Arc::clone(&pool_cache);
        let graph = Arc::clone(&graph);
        let progress = progress2.clone();
        let phase2_active_count = phase2_active_count.clone();
        let token_whitelist = token_whitelist.clone();
        let event_subscriber = Arc::clone(&event_subscriber);

        async move {
            let addr = pool.address();
            let pool_contract = UniswapV3Pool::new(addr, provider.clone());
            
            let token0_call = pool_contract.token_0();
            let token1_call = pool_contract.token_1();
            
            match tokio::try_join!(
                token0_call.call(),
                token1_call.call()
            ) {
                Ok((token0, token1)) if token_whitelist.contains(&token0) && token_whitelist.contains(&token1) => {
                    info!("[СИНХРОНИЗАЦИЯ] Новый пул {:?} проходит whitelist: {:?} ↔ {:?}", addr, token0, token1);

                    let _ = event_subscriber.add_pools_to_subscription(vec![addr]).await;
                    
                    let mut pool_cache_look = pool_cache.lock().await;
                    if let Some(pool) = build_uniswap_v3_pool(
                        addr,
                        (token0, token1),
                        provider.clone(),
                        &token_cache,
                        ).await {
                            if pool.is_active {
                                pool_cache_look.add_pool_address(addr);
                                graph.lock().await.upsert_pool(pool.clone());
                                phase2_active_count.fetch_add(1, Ordering::SeqCst);
                                event_subscriber.update_graph(graph.clone(), addr).await;
                        }
                    }
                },
                Ok(_) => info!("[СИНХРОНИЗАЦИЯ] Пропуск нового пула {:?}: токены не в whitelist", addr),
                Err(e) => warn!("[СИНХРОНИЗАЦИЯ] Ошибка проверки токенов пула {:?}: {:?}", addr, e),
            }
            progress.inc(1);
        }
    }).await;

    progress2.finish_with_message("[СИНХРОНИЗАЦИЯ]✅ Новые пулы обработаны");

    // Итоговая статистика
    info!(
        "[ИТОГ] Обработано: {} пулов из кэша, {} новых пулов",
        phase1_active_count.load(Ordering::SeqCst),
        phase2_active_count.load(Ordering::SeqCst)
    );

    Ok(())
}



pub async fn build_uniswap_v3_pool(
    pool_address: Address,
    tokens: (Address, Address),
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
) -> Option<UniswapPool> {
    let (token_a, token_b) = tokens;

    // Получаем данные токенов
    let (token_a_info, token_b_info) = tokio::try_join!(
        get_single_token_data(token_a, provider.clone(), token_cache),
        get_single_token_data(token_b, provider.clone(), token_cache)
    ).ok()?;

    // Получаем данные пула
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
    let (
        liquidity,
        slot0_result,
        tick_spacing,
        max_liquidity_per_tick,
        fee,
    ) = process_pool_data(pool_address, &pool_contract).await?;

    let (sqrt_price_x96, tick, _, _, _, _, _) = slot0_result;
    let sqrt_price = U512::from_str(&sqrt_price_x96.to_string()).unwrap_or_default();

    let current_price = calculate_current_price(
        sqrt_price,
        token_a_info.decimals,
        token_b_info.decimals
    ).ok()?;

    let tick_map = fetch_active_ticks(pool_address, provider.clone(), slot0_result.1, fee).await.ok()?;

    Some(UniswapPool {
        uniswap_pool_address: pool_address,
        uniswap_dex: "uniswap_v3".to_string(),
        uniswap_token_a: token_a,
        uniswap_token_a_decimals: token_a_info.decimals,
        uniswap_token_a_symbol: token_a_info.symbol,
        uniswap_token_b: token_b,
        uniswap_token_b_decimals: token_b_info.decimals,
        uniswap_token_b_symbol: token_b_info.symbol,
        uniswap_liquidity: liquidity,
        uniswap_sqrt_price: sqrt_price,
        uniswap_current_price: current_price,
        uniswap_tick_current: tick,
        uniswap_tick_lower: tick - tick_spacing,
        uniswap_tick_upper: tick + tick_spacing,
        uniswap_tick_spacing: tick_spacing,
        uniswap_max_liquidity_per_tick: U512::from(max_liquidity_per_tick),
        uniswap_fee_tier: fee,
        tick_map,
        is_active: true,
    })
}

/// Источник пула, который может быть как адресом, так и уже готовым объектом пула
pub enum PoolSource {
    /// Адрес пула
    Address(H160),
    /// Готовый объект пула
    Pool(CfmmsPool)   
}

/// Асинхронно запрашивает и возвращает `tick_spacing` для данного пула
pub async fn fetch_tick_spacing(
    pool_address: H160,
    provider: Arc<Provider<Ws>>,
) -> Option<i32> {
    // Создаём client-контракт пула
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
    // Делаем вызов метода `tick_spacing` и возвращаем результат, если Ok
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pool_contract.tick_spacing().call().await.ok()
}
/// Асинхронно запрашивает и возвращает `liquidity` для данного пула
/// liquidity - это общее количество ликвидности, которое присвоено пулу
pub async fn fetch_pool_liquidity(pool_contract: &UniswapV3Pool<Provider<Ws>>,) -> Option<U512> {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pool_contract.liquidity().call().await.ok().map(U512::from)
}

/// Асинхронно запрашивает и возвращает `slot_0` для данного пула
/// slot_0 - это структурное поле, которое хранит текущие значения
/// sqrt_price, tick, observation_index, observation_cardinality, observation_cardinality_next, fee_protocol, unlocked
pub async fn fetch_pool_slot0(pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<(ethers::types::U256, i32, u16, u16, u16, u8, bool)> {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pool_contract.slot_0().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `tick_spacing` для данного пула
/// tick_spacing - это шаг, на который тик-интервалы (range) разделяются
pub async fn fetch_pool_tick_spacing(pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<i32> {
    pool_contract.tick_spacing().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `max_liquidity_per_tick` для данного пула
/// max_liquidity_per_tick - это максимальное значение ликвидности, которое может быть
/// присвоено отдельному тик-интервалу.
pub async fn fetch_pool_max_liquidity(pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<u128> {
    pool_contract.max_liquidity_per_tick().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `fee` для данного пула
///
/// `fee` - это комиссия, которая берется за обмен токенов в пуле.
/// Величина комиссии измеряется в 1/10000 от 1% (то есть 0.01%).
pub async fn fetch_pool_fee(pool_contract: &UniswapV3Pool<Provider<Ws>>,) -> Option<u32> {
    pool_contract.fee().call().await.ok()
}



/// Асинхронно обрабатывает данные о пуле
///
/// Получает: liquidity, slot0_result, tick_spacing, max_liquidity_per_tick, fee
///
/// slot0_result - это структурное поле, которое хранит текущие значения
/// sqrt_price, tick, observation_index, observation_cardinality, observation_cardinality_next, fee_protocol, unlocked
///
/// tick_spacing - это шаг, на который тик-интервалы (range) разделяются
///
/// max_liquidity_per_tick - это максимальное значение ликвидности, которое может быть
/// присвоено отдельному тик-интервалу.
///
/// fee - это комиссия, которая берется за обмен токенов в пуле.
/// Величина комиссии измеряется в 1/10000 от 1% (то есть 0.01%).
///
/// Если ликвидность пула меньше 10_000, то функция возвращает None.
///
pub async fn process_pool_data(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<(
    U512,                              // liquidity
    (ethers::types::U256, i32, u16, u16, u16, u8, bool), // slot0_result
    i32,                              // tick_spacing
    u128,                             // max_liquidity
    u32,                              // fee
)> {
    // Execute all calls in parallel
    let (
        liquidity_option,
        slot0_option,
        tick_spacing_option,
        max_liquidity_option,
        fee_option
    ) = tokio::join!(
        async { fetch_pool_liquidity(pool_contract) }, 
        async { fetch_pool_slot0(pool_contract) },     
        async { fetch_pool_tick_spacing(pool_contract) }, 
        async { fetch_pool_max_liquidity(pool_contract) }, 
        async { fetch_pool_fee(pool_contract) }        
    );

    let actual_liquidity = match liquidity_option.await {
        Some(liq) => {
            if liq < U512::from(10_000) {
                info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: низкая ликвидность ({:?})", pool_address, liq);
            return None;
        }
            liq
        }
        None => {
            info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: не удалось получить ликвидность", pool_address);
return None;
        }
    };

    let actual_slot0 = match slot0_option.await {
        Some(s0) => s0,
        None => {
            info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: не удалось получить slot0", pool_address);
            return None;
        }
    };

    let actual_tick_spacing = match tick_spacing_option.await {
        Some(ts) => ts,
        None => {
            info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: не удалось получить tick_spacing", pool_address);
            return None;
        }
    };

    let actual_max_liquidity = match max_liquidity_option.await {
        Some(ml) => ml,
        None => {
            info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: не удалось получить max_liquidity", pool_address);
            return None;
        }
    };

    let actual_fee = match fee_option.await {
        
        Some(feee) => feee,
        None => {
            info!("[СИНХРОНИЗАЦИЯ] Пул {:?} пропущен: не удалось получить fee", pool_address);
            return None;
        }
    };

    // Параллельное получение информации о тиках

    Some((
        actual_liquidity,
        actual_slot0,
        actual_tick_spacing,
        actual_max_liquidity,
        actual_fee,
    ))
    
}


