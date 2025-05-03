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
use log::debug;
use log::warn;

use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Mutex;
use tokio::task::JoinSet;

use lazy_static::lazy_static;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info};
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

pub async fn get_current_block(provider: Arc<Provider<Ws>>) -> Result<u64, Box<dyn std::error::Error>> {
    let block_number = provider.get_block_number().await?;
    Ok(block_number.as_u64())
}

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


lazy_static! {
    static ref Q96: U512 = U512::from(1u128) << 96;
 }



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
        100 => (100, 10),
        500 => (100, 3),
        3000 => (30, 2),
        10_000 => (2, 10),
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
                    if let Ok(price) = tick_to_sqrt_price(tick.tick) {
                        ticks.insert(tick.tick, (tick.liquidity_net, price));
                    }
                }
            }
            ticks
        });
    }

    // Левая сторона
    for batch in 0..total_batches {
        let base_word = current_word - ((batch * words_per_batch) as i32);
        let tick_lens = tick_lens.clone();
        let pool_address = pool_address;
        let left_active = left_active.clone();
        set.spawn(async move {
            let mut ticks = HashMap::new();
            for i in 0..words_per_batch {
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

    // Правая сторона
    for batch in 0..total_batches {
        let base_word = current_word + ((batch * words_per_batch) as i32);
        let tick_lens = tick_lens.clone();
        let pool_address = pool_address;
        let right_active = right_active.clone();
        set.spawn(async move {
            let mut ticks = HashMap::new();
            for i in 0..words_per_batch {
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
            "[{:?}] Fee: {}, Батчи: {}×{}, Тики: {} (←{} →{}), С ценой ≠ 0: {}",
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
) -> Result<(), Box<dyn std::error::Error>> {
    // Объект для сбора готовых UniswapPool
    let pools_to_process = Arc::new(Mutex::new(Vec::new()));

    // === Фаза 1: обрабатываем адреса из кэша без фильтрации по whitelist ===
    let original_addresses: Vec<Address> = {
        let pool_cache_lock = pool_cache.lock().await;
        pool_cache_lock.pool_addresses.iter().cloned().collect()
    };

    let valid_addresses = Arc::new(Mutex::new(HashSet::new()));
    let progress = ProgressBar::new(original_addresses.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/red}] {pos}/{len} из кеша")
            .unwrap()
            .progress_chars("=>-"),
    );

    stream::iter(original_addresses)
        .for_each_concurrent(20, |addr| {
            let provider = provider.clone();
            let token_cache = Arc::clone(&token_cache);
            let pool_cache = Arc::clone(&pool_cache);
            let pools_to_process = Arc::clone(&pools_to_process);
            let valid_addresses = Arc::clone(&valid_addresses);
            let progress = progress.clone();
            let whitelist = token_whitelist.clone();

            async move {
                match build_uniswap_v3_pool(
                    PoolSource::Address(addr),
                    provider.clone(),
                    &token_cache,
                    &mut *pool_cache.lock().await,
                    &whitelist,
                )
                .await
                {
                    Some(pool) => {
                        pools_to_process.lock().await.push(pool);
                        valid_addresses.lock().await.insert(addr);
                        info!("[CACHE] Пул из кеша добавлен: {:?}", addr);
                    }
                    None => {
                        warn!("[CACHE] Пул уделен из кеша : {:?}", addr);
                    }
                }
                
                progress.inc(1);
            }
        })
        .await;
    progress.finish_with_message("✅ Пулы из кеша обработаны");   

    // Сохраняем обновлённый кеш (бинарник + JSON)
    {
        let mut pool_cache_lock = pool_cache.lock().await;
        pool_cache_lock.pool_addresses = valid_addresses.lock().await.clone();
        if let Err(e) = pool_cache_lock.save_to_file() {
            error!("Ошибка сохранения pool_cache.bin: {:?}", e);
        }
        if let Err(e) = pool_cache_lock.save_to_json_debug("uniswap_pool_addresses_cache.json") {
            error!("Ошибка сохранения pool_cache.json: {:?}", e);
        } else {
            info!("🔍 pool_cache.json обновлён для отладки");
        }
        // и last_verified_block обновить:
        if let Err(e) = pool_cache_lock.update_last_verified_block(&provider).await {
            warn!("Не удалось обновить last_verified_block: {}", e);
        }
    }

    // === Фаза 2: сканируем новые пулы с фабрики и фильтруем по whitelist ===
    let (factory_address, start_block) = {
        let pc = pool_cache.lock().await;
        let fa: Address = get_env_var("UNISWAP_V3_FACTORY").parse()?;
        let sb = if pc.pool_addresses.is_empty() {
            start_block_from_env
        } else {
            pc.last_verified_block
        };
        (fa, sb)
    };

    info!("[SYNC] Новые пулы с блока {}", start_block);
    let dex = Dex::UniswapV3(UniswapV3Dex::new(factory_address, BlockNumber::Number(start_block.into())));
    let all_new = sync_pairs(vec![dex], provider.clone(), None).await?;
    let to_check: Vec<_> = {
        let pc = pool_cache.lock().await;
        all_new.into_iter()
            .filter(|p| !pc.pool_addresses.contains(&p.address()))
            .collect()
    };

    let progress2 = ProgressBar::new(to_check.len() as u64);
    progress2.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.magenta/green}] {pos}/{len} новых")
            .unwrap()
            .progress_chars("=>-"),
    );

    stream::iter(to_check)
        .for_each_concurrent(20, |pool| {
            let provider = provider.clone();
            let token_cache = Arc::clone(&token_cache);
            let pool_cache = Arc::clone(&pool_cache);
            let pools_to_process = Arc::clone(&pools_to_process);
            let progress = progress2.clone();
            let whitelist = token_whitelist.clone();

            async move {
                let addr = pool.address();
                let mut pc = pool_cache.lock().await;
                if let Some(p) = build_uniswap_v3_pool(
                    PoolSource::Pool(pool.clone()),
                    provider.clone(),
                    &token_cache,
                    &mut pc,
                    &whitelist,
                ).await {
                    // здесь уже внутри build_uniswap_v3_pool для PoolSource::Pool проверка по whitelist
                    pc.add_pool_address(addr);
                    pools_to_process.lock().await.push(p);
                    info!("[NEW] Добавлен пул: {:?}", addr);
                } else {
                    warn!("[NEW] Пул отброшен (не входит в whitelist или провалил build): {:?}", addr);
                }
                progress.inc(1);
            }
        })
        .await;

    progress2.finish_with_message("✅ Новые пулы обработаны");

    // Обновляем last_verified_block
    if let Some(cur) = get_current_block(provider.clone()).await.ok() {
        let mut pc = pool_cache.lock().await;
        pc.last_verified_block = cur;
        info!("✅ Завершили на блоке {}", cur);
    }

    // === Записываем всё в граф ===
    {
        let mut g = graph.lock().await;
        let list = pools_to_process.lock().await;
        for u in list.iter() {
            g.add_pool(
                u.uniswap_pool_address,
                u.uniswap_dex.clone(),
                u.uniswap_token_a,
                u.uniswap_token_a_decimals,
                u.uniswap_token_a_symbol.clone(),
                u.uniswap_token_b,
                u.uniswap_token_b_decimals,
                u.uniswap_token_b_symbol.clone(),
                u.uniswap_liquidity,
                u.uniswap_sqrt_price,
                u.uniswap_current_price,
                u.uniswap_tick_current,
                u.uniswap_tick_lower,
                u.uniswap_tick_upper,
                u.uniswap_tick_spacing,
                u.uniswap_max_liquidity_per_tick,
                u.uniswap_fee_tier,
                u.tick_map.clone(),
                u.is_active,
            );
        }
    }
    // Сохраняем граф
    {
        let g = graph.lock().await;
        let total = pools_to_process.lock().await.len();
        if let Err(e) = g.save_pool_to_file() {
            error!("Ошибка сохранения графа: {:?}", e);
        } else {
            info!("Граф сохранён, пулов в нём: {}", total);
        }
    }

    Ok(())
}




pub enum PoolSource {
    Address(H160),
    Pool(CfmmsPool)   
}

async fn process_pool_data(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
    provider: Arc<Provider<Ws>>,
) -> Option<(
    U512,                              // liquidity
    (ethers::types::U256, i32, u16, u16, u16, u8, bool), // slot0_result
    i32,                              // tick_spacing
    u128,                             // max_liquidity
    u32,                              // fee
    HashMap<i32, (i128, U512)>,       // tick_map
)> {
    // Store intermediate values
    let liquidity = pool_contract.liquidity();
    let slot0 = pool_contract.slot_0();
    let tick_spacing = pool_contract.tick_spacing();
    let max_liquidity = pool_contract.max_liquidity_per_tick();
    let fee = pool_contract.fee();

    // Create calls
    let liquidity_call = liquidity.call();
    let slot0_call = slot0.call();
    let tick_spacing_call = tick_spacing.call();
    let max_liquidity_call = max_liquidity.call();
    let fee_call = fee.call();

    // Execute all calls in parallel
    let (
        liquidity_result,
        slot0_result,
        tick_spacing_result,
        max_liquidity_result,
        fee_result
    ) = tokio::try_join!(
        liquidity_call,
        slot0_call,
        tick_spacing_call,
        max_liquidity_call,
        fee_call
    ).ok()?;

    let liquidity = U512::from(liquidity_result);
    if liquidity.is_zero()
    {
        info!("Пул {:?} пропущен: нулевая ликвидность", pool_address);
        return None;
    }
   

    // Параллельное получение информации о тиках
    let tick_map = fetch_active_ticks(pool_address, provider.clone(), slot0_result.1, fee_result).await.ok()?;

    Some((
        liquidity,
        slot0_result,
        tick_spacing_result,
        max_liquidity_result,
        fee_result,
        tick_map,
    ))
}

pub async fn build_uniswap_v3_pool(
    source: PoolSource,
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
    pool_cache: &mut UniswapPoolCache,
    token_whitelist: &HashSet<Address>,
) -> Option<UniswapPool> {
    // 1. Получаем адрес пула в зависимости от источника
    let pool_address = match source {
        PoolSource::Address(addr) => {
            info!("Проверяем пул из кэша: {:?}", addr);
            if !pool_cache.pool_addresses.contains(&addr) {
                error!("Адрес {:?} не найден в кэше", addr);
                return None;
            }
            addr
        },
        PoolSource::Pool(pool) => {
            info!("Проверяем новый пул: {:?}", pool.address());
            pool.address()
        }
    };

    // 2. Проверяем существование контракта
    let code = provider.get_code(pool_address, None).await.ok()?;
    if code.is_empty() {
        error!("Контракт по адресу {:?} пустой", pool_address);
        return None;
    }

    // 3. Создаем контракт и получаем адреса токенов
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
    
    let token_0_call = pool_contract.token_0();
    let token_1_call = pool_contract.token_1();

    let (token_a, token_b) = tokio::try_join!(
        token_0_call.call(),
        token_1_call.call()
    ).ok()?;

   // 4. Фильтрация по whitelist (ТОЛЬКО для новых пулов)
   match source {
    PoolSource::Pool(_) => {
        if !token_whitelist.contains(&token_a) || !token_whitelist.contains(&token_b) {
            info!(
                "⚠️ Пул {:?} отфильтрован: токены не в whitelist: token0 = {:?}, token1 = {:?}",
                pool_address, token_a, token_b
            );
            return None;
        }
    },
    PoolSource::Address(_) => {
        debug!(
            "✅ Пропускаем проверку whitelist для пула из кэша: {:?}",
            pool_address
        );
    }
}

    // 5. Получаем базовую информацию о токенах
    let (token_a_info, token_b_info) = tokio::try_join!(
        get_single_token_data(token_a, provider.clone(), token_cache),
        get_single_token_data(token_b, provider.clone(), token_cache)
    ).ok()?;


    // 6. Получаем остальные данные пула через process_pool_data
    let (
        liquidity,
        slot0_result,
        tick_spacing,
        max_liquidity_per_tick,
        fee,
        tick_map
    ) = process_pool_data(
        pool_address,
        &pool_contract,
        provider.clone(),
    ).await?;

    // 7. Извлекаем и обрабатываем данные из slot0
    let (sqrt_price_x96, tick, _, _, _, _, _) = slot0_result;
    let sqrt_price = U512::from_str(&sqrt_price_x96.to_string()).unwrap_or_default();

    // 8. Вычисляем текущую цену
    let current_price = calculate_current_price(
        sqrt_price,
        token_a_info.decimals,
        token_b_info.decimals
    ).ok()?;

    let is_active = !liquidity.is_zero() && liquidity > U512::from(10_000) && !sqrt_price.is_zero() && current_price > U512::from(10_000);


    // 9. Формируем финальный объект пула
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
        is_active: is_active,
    })
}

