use crate::tick_fetcher::fetch_active_ticks;
use crate::token::get_single_token_data;
use crate::token::TokenCache;
use crate::uniswap_cache::UniswapPoolCache;
use crate::uniswap_events::UniswapEventSubscriber;
use crate::uniswap_graph::UniswapPool;
use crate::uniswap_graph::UniversalGraph;

use arc_swap::ArcSwap;
use colored::Colorize;
use dashmap::DashSet;
use ethers::contract::abigen;
use ethers::providers::Provider;
use ethers::types::Address;
use ethers::types::H160;
use ethers::types::U256;
use ethers_contract::Multicall;
use ethers_providers::Http;
use im::OrdMap;
use lazy_static::lazy_static;
use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;
use tracing::error;
use tracing::{debug, info, warn};

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
}]"#
);

abigen!(
    ERC20,
    r#"[  
        {
            "constant": true,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "payable": false,
            "stateMutability": "view",
            "type": "function"
        },
        {
            "constant": true,
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "payable": false,
            "stateMutability": "view",
            "type": "function"
        },
        {
            "constant": true,
            "inputs": [{"name": "owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "payable": false,
            "stateMutability": "view",
            "type": "function"
        }
    ]"#
);

lazy_static! {
    static ref Q96: U256 = U256::from(1) << 96;
}

/// Вспомогательная функция для умножения двух U256 с разбиением на старшую и младшую части
fn full_multiply(a: U256, b: U256) -> (U256, U256) {
    // Разбиваем a и b на старшие и младшие 128 бит
    let a_high = a >> 128;
    let a_low = a & ((U256::from(1) << 128) - U256::from(1));
    let b_high = b >> 128;
    let b_low = b & ((U256::from(1) << 128) - U256::from(1));

    // Вычисляем части произведения: a * b = (a_high * 2^128 + a_low) * (b_high * 2^128 + b_low)
    let low_low = a_low
        .checked_mul(b_low)
        .expect("Переполнение при умножении младших частей");
    let high_low = a_high
        .checked_mul(b_low)
        .expect("Переполнение при умножении старшей и младшей частей");
    let low_high = a_low
        .checked_mul(b_high)
        .expect("Переполнение при умножении младшей и старшей частей");
    let high_high = a_high
        .checked_mul(b_high)
        .expect("Переполнение при умножении старших частей");

    // Суммируем части с учетом сдвигов
    let intermediate = low_low
        .checked_add(
            (high_low << 128)
                .checked_add(low_high << 128)
                .expect("Переполнение промежуточной суммы"),
        )
        .expect("Переполнение суммы младших частей");
    let high = high_high
        .checked_add(high_low >> 128)
        .expect("Переполнение старшей суммы")
        .checked_add(low_high >> 128)
        .expect("Переполнение старшей суммы");
    let low = intermediate;

    (high, low)
}

/// Преобразует тик в sqrt_price_x96, соответствующее TickMath.sol
pub fn tick_to_sqrt_price(tick: i32) -> Result<U256, String> {
    debug!(
        "[UNISWAP_V3_SQRT_PRICE_DEBUG] Начало преобразования тика в sqrt_price_x96, тик: {}",
        tick
    );

    // Проверка границ тика
    if tick < -887272 || tick > 887272 {
        warn!(
            "[UNISWAP_V3_SQRT_PRICE_WARN] Тик вне допустимого диапазона: {}, пропуск пула",
            tick
        );
        return Err("Тик вне допустимого диапазона".to_string());
    }

    let abs_tick = tick.unsigned_abs() as u32;
    let mut ratio = if abs_tick & 0x1 != 0 {
        U256::from_str("0xfffcb933bd6fad37aa2d162d1a594001").map_err(|e| e.to_string())?
    } else {
        U256::from_str("0x100000000000000000000000000000000").map_err(|e| e.to_string())?
    };

    // Константы из TickMath.sol
    let constants = [
        ("0xfff97272373d413259a46990580e213a", 0x2),
        ("0xfff2e50f5f656932ef12357cf3c7fdcc", 0x4),
        ("0xffe5caca7e10e4e61c3624eaa0941cd0", 0x8),
        ("0xffcb9843d60f6159c9db58835c926644", 0x10),
        ("0xff973b41fa98c081472e6896dfb254c0", 0x20),
        ("0xff2ea16466c96a3843ec78b326b52861", 0x40),
        ("0xfe5dee046a99a2a811c461f1969c3053", 0x80),
        ("0xfcbe86c7900a88aedcffc83b479aa3a4", 0x100),
        ("0xf987a7253ac413176f2b074cf7815e54", 0x200),
        ("0xf3392b0822b70005940c7a398e4b70f3", 0x400),
        ("0xe7159475a2c29b7443b29c7fa6e889d9", 0x800),
        ("0xd097f3bdfd2022b8845ad8f792aa5825", 0x1000),
        ("0xa9f746462d870fdf8a65dc1f90e061e5", 0x2000),
        ("0x70d869a156d2a1b890bb3df62baf32f7", 0x4000),
        ("0x31be135f97d08fd981231505542fcfa6", 0x8000),
        ("0x9aa508b5b7a84e1c677de54f3e99bc9", 0x10000),
        ("0x5d6af8dedb81196699c329225ee604", 0x20000),
        ("0x2216e584f5fa1ea926041bedfe98", 0x40000),
        ("0x48a170391f7dc42444e8fa2", 0x80000),
    ];

    for (constant, bit) in constants.iter() {
        if abs_tick & bit != 0 {
            let multiplier = U256::from_str(constant).map_err(|e| e.to_string())?;
            let (high, low) = full_multiply(ratio, multiplier);
            if high >= (U256::from(1) << 128) {
                warn!(
                    "[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при умножении ratio, бит: {}, пропуск пула",
                    bit
                );
                return Err("Переполнение при умножении ratio".to_string());
            }
            ratio = (high << 128).checked_add(low >> 128).ok_or_else(|| {
                warn!("[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при сдвиге ratio, пропуск пула");
                "Переполнение при сдвиге ratio".to_string()
            })?;
        }
    }

    // Инверсия только для положительных тиков
    if tick > 0 {
        ratio = U256::MAX.checked_div(ratio).ok_or_else(|| {
            warn!("[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при инверсии ratio, пропускk пула");
            "Переполнение при инверсии ratio".to_string()
        })?;
    } else {
        debug!("[UNISWAP_V3_SQRT_PRICE_DEBUG] Тик <= 0, инверсия ratio не требуется");
    }

    // Завершающее вычисление sqrt_price_x96
    let sqrt_price = (ratio >> 32)
        .checked_add(if ratio % (U256::from(1) << 32) == U256::zero() {
            U256::zero()
        } else {
            U256::one()
        })
        .ok_or_else(|| {
            warn!(
                "[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при финальном расчете sqrt_price, пропуск пула"
            );
            "Переполнение при финальном расчете sqrt_price".to_string()
        })?;
    debug!(
        "[UNISWAP_V3_SQRT_PRICE_DEBUG] Конец преобразования тика в sqrt_price_x96, результат: {}",
        sqrt_price
    );

    Ok(sqrt_price)
}

const Q96_U256: U256 = U256([0, 1 << (96 - 64), 0, 0]); // 2^96

/// Делит Q96^2 на sqrtPriceX96
fn q96_squared_div(sqrt_price_x96: U256) -> U256 {
    let numerator = Q96_U256.checked_mul(Q96_U256).unwrap_or(U256::MAX);
    numerator
        .checked_div(sqrt_price_x96)
        .unwrap_or(U256::zero())
}

/// Рассчитывает ликвидность токенов в пуле
pub fn calculate_token_liquidity(
    pool: &UniswapPool,
    tick_map: &OrdMap<i32, (i128, U256)>,
    current_tick: i32,
    sqrt_price_x96: U256,
) -> (U256, U256) {
    debug!(
        "[LIQUIDITY_CALC] 💧 Начало расчёта ликвидности для пула {:?} (токены: {}/{}), текущий тик: {}",
        pool.graph_pool_address,
        pool.uniswap_token_a_symbol,
        pool.uniswap_token_b_symbol,
        current_tick
    );

    // Проверка, что текущий тик находится в пределах диапазона пула
    if current_tick < pool.uniswap_tick_lower || current_tick >= pool.uniswap_tick_upper {
        debug!(
            "[LIQUIDITY_CALC] 💧 Текущий тик {} вне диапазона пула [{}, {}), возвращаем нули",
            current_tick, pool.uniswap_tick_lower, pool.uniswap_tick_upper
        );
        return (U256::zero(), U256::zero());
    }

    if tick_map.is_empty() {
        debug!(
            "[LIQUIDITY_CALC] 💧 tick_map пустой, возвращаем нули для пула {:?}",
            pool.graph_pool_address
        );
        return (U256::zero(), U256::zero());
    }

    let total_liquidity = U256::from(pool.uniswap_liquidity);
    info!(
        "[LIQUIDITY_CALC] 💧 Общая ликвидность пула: {}",
        total_liquidity
    );

    let next_active_tick = tick_map
        .iter()
        .filter(|(tick, _)| **tick > current_tick)
        .min_by_key(|(tick, _)| *tick)
        .map(|(tick, _)| *tick)
        .unwrap_or(pool.uniswap_tick_upper);

    let prev_active_tick = tick_map
        .iter()
        .filter(|(tick, _)| **tick <= current_tick)
        .max_by_key(|(tick, _)| *tick)
        .map(|(tick, _)| *tick)
        .unwrap_or(pool.uniswap_tick_lower);

    info!(
        "[LIQUIDITY_CALC] 💧💧 Активные тики: предыдущий={}, следующий={}",
        prev_active_tick, next_active_tick
    );

    let sqrt_price_upper = match tick_to_sqrt_price(next_active_tick) {
        Ok(price) => price,
        Err(e) => {
            warn!(
                "[LIQUIDITY_CALC] 💧💧 Ошибка конвертации верхнего тика {}: {}, используем текущую цену",
                next_active_tick, e
            );
            sqrt_price_x96
        }
    };

    let sqrt_price_lower = match tick_to_sqrt_price(prev_active_tick) {
        Ok(price) => price,
        Err(e) => {
            warn!(
                "[LIQUIDITY_CALC] 💧💧 Ошибка конвертации нижнего тика {}: {}, используем текущую цену",
                prev_active_tick, e
            );
            sqrt_price_x96
        }
    };

    info!(
        "[LIQUIDITY_CALC] 💧💧 Цены: текущая={}, верхняя={}, нижняя={}",
        sqrt_price_x96, sqrt_price_upper, sqrt_price_lower
    );

    let inv_sqrt_lower = q96_squared_div(sqrt_price_lower);
    let inv_sqrt_upper = q96_squared_div(sqrt_price_upper);

    info!(
        "[LIQUIDITY_CALC] 💧💧 Обратные цены: нижняя={}, верхняя={}",
        inv_sqrt_lower, inv_sqrt_upper
    );

    // Учет порядка токенов для корректного расчета
    let delta_inv_sqrt = if pool.uniswap_token_a < pool.uniswap_token_b {
        inv_sqrt_lower.saturating_sub(inv_sqrt_upper)
    } else {
        inv_sqrt_upper.saturating_sub(inv_sqrt_lower)
    };
    let amount_token0 = total_liquidity
        .saturating_mul(delta_inv_sqrt)
        .checked_div(Q96_U256)
        .unwrap_or(U256::zero());

    let delta_sqrt = if pool.uniswap_token_a < pool.uniswap_token_b {
        sqrt_price_upper.saturating_sub(sqrt_price_lower)
    } else {
        sqrt_price_lower.saturating_sub(sqrt_price_upper)
    };
    let amount_token1 = total_liquidity
        .saturating_mul(delta_sqrt)
        .checked_div(Q96_U256)
        .unwrap_or(U256::zero());

    info!(
        "[LIQUIDITY_CALC] 💧💧💧 Результаты: token0={}, token1={}",
        amount_token0, amount_token1
    );

    (amount_token0, amount_token1)
}

/// Создает пул Uniswap V3
pub async fn build_uniswap_v3_pool(
    pool_address: Address,
    tokens: (Address, Address),
    provider: Arc<Provider<Http>>,
    token_cache: &TokenCache,
) -> Option<UniswapPool> {
    debug!(
        "[UNISWAP_V3_BUILD_DEBUG] Начало build_uniswap_v3_pool, pool_address: {:?}",
        pool_address
    );

    let (token_a, token_b) = tokens;

    let (token_a_info, token_b_info) = tokio::try_join!(
        get_single_token_data(token_a, provider.clone(), token_cache),
        get_single_token_data(token_b, provider.clone(), token_cache)
    )
    .ok()?;
    info!(
        "[UNISWAP_V3_BUILD_DEBUG][{:?}] ДАННЫЕ ТОКЕНОВ ПУЛА: token_a: {}, token_b: {}",
        pool_address, token_a_info.symbol, token_b_info.symbol
    );

    if token_a_info.decimals == 0
        || token_a_info.decimals > 18
        || token_b_info.decimals == 0
        || token_b_info.decimals > 18
    {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Некорректные decimals: token_a_decimals={}, token_b_decimals={}",
            pool_address, token_a_info.decimals, token_b_info.decimals
        );
        return None;
    }

    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

    let (liquidity, slot0_result, tick_spacing, max_liquidity_per_tick, fee) =
        process_pool_data(pool_address, pool_contract.clone().into()).await?;

    debug!(
        "[UNISWAP_V3_BUILD_DEBUG][{:?}] Данные пула получены: liquidity: {}, tick: {}, fee: {}",
        pool_address, liquidity, slot0_result.1, fee
    );

    let (sqrt_price_x96, tick, _, _, _, _, _) = slot0_result;

    if liquidity.is_zero() {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Пропуск пула: нулевая ликвидность",
            pool_address
        );
        return None;
    }
    if sqrt_price_x96.is_zero() {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Пропуск пула: нулевая sqrt_price_x96",
            pool_address
        );
        return None;
    }
    if tick_spacing <= 0 {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Некорректный tick_spacing: {}",
            pool_address, tick_spacing
        );
        return None;
    }

    let tick_map = fetch_active_ticks(pool_address, provider.clone(), slot0_result.1, fee)
        .await
        .ok()?;
    info!(
        "[UNISWAP_V3_BUILD_DEBUG][{:?}] tick_map получен, размер: {}",
        pool_address,
        tick_map.len()
    );

    if tick_map.is_empty() {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Пропуск пула: пустая тиковая карта",
            pool_address
        );
        return None;
    }

    let pool = UniswapPool {
        graph_pool_address: pool_address.into(),
        uniswap_dex: "uniswap_v3".to_string().into(),
        uniswap_token_a_decimals: token_a_info.decimals,
        uniswap_token_b_decimals: token_b_info.decimals,
        uniswap_token_a: Arc::new(token_a),
        uniswap_token_a_symbol: Arc::new(token_a_info.symbol),
        uniswap_token_b: Arc::new(token_b),
        uniswap_token_b_symbol: Arc::new(token_b_info.symbol),
        uniswap_liquidity: liquidity,
        uniswap_sqrt_price: sqrt_price_x96,
        uniswap_tick_current: tick,
        uniswap_tick_lower: tick - tick_spacing,
        uniswap_tick_upper: tick + tick_spacing,
        uniswap_tick_spacing: tick_spacing,
        uniswap_max_liquidity_per_tick: U256::from(max_liquidity_per_tick),
        uniswap_fee_tier: fee,
        tick_map: tick_map.clone(),
        is_active: true,
        liquidity_token_a: U256::zero(),
        liquidity_token_b: U256::zero(),
    };

    let (liquidity_token0, liquidity_token1) =
        calculate_token_liquidity(&pool, &tick_map, tick, sqrt_price_x96);

    if liquidity_token0 > U256::from(u128::MAX) || liquidity_token1 > U256::from(u128::MAX) {
        warn!(
            "[UNISWAP_V3_BUILD_DEBUG][{:?}] Пропуск пула: ликвидность токенов превышает uint128: token0: {}, token1: {}",
            pool_address, liquidity_token0, liquidity_token1
        );
        return None;
    }

    let pool = UniswapPool {
        liquidity_token_a: liquidity_token0,
        liquidity_token_b: liquidity_token1,
        ..pool
    };

    debug!(
        "[UNISWAP_V3_BUILD_DEBUG] Конец build_uniswap_v3_pool, pool_address: {:?}",
        pool_address
    );
    Some(pool)
}

/// Синхронизирует пулы Uniswap V3 с кэша и обновляет граф
pub async fn sync_pools(
    graph: Arc<ArcSwap<UniversalGraph>>,
    provider: Arc<Provider<Http>>,
    token_cache: &TokenCache,
    pool_cache: Arc<UniswapPoolCache>,
    token_whitelist: &DashSet<Address>,
    event_subscriber: Arc<UniswapEventSubscriber>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("[UNISWAP_V3_SYNC_POOL_DEBUG] Начало sync_pools");

    let save_per_pool = env::var("SAVE_GRAPH_PER_POOL")
        .map(|v| v == "true")
        .unwrap_or(false);

    let (original_addresses, original_count) = (
        pool_cache.pool_addresses.clone(),
        pool_cache.pool_addresses.len(),
    );

    info!(
        "[UNISWAP_V3_SYNC_POOL] Начинаем обработку {} пулов из кэша",
        original_count
    );
    debug!(
        "[UNISWAP_V3_SYNC_POOL_DEBUG] original_count: {}",
        original_count
    );

    let phase1_active_count = Arc::new(AtomicUsize::new(0));
    let phase1_processed = Arc::new(AtomicUsize::new(0));

    for current_addresses in original_addresses {
        debug!(
            "[UNISWAP_V3_SYNC_POOL_DEBUG][{:?}] >>>>> Старт обработки пула",
            current_addresses
        );

        let pool_contract = UniswapV3Pool::new(current_addresses, provider.clone());

        let token0 = match pool_contract.token_0().call().await {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "[UNISWAP_V3_SYNC_POOL][{:?}] Ошибка получения token0: {:?}",
                    current_addresses, e
                );
                continue;
            }
        };

        let token1 = match pool_contract.token_1().call().await {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "[UNISWAP_V3_SYNC_POOL][{:?}] Ошибка получения token1: {:?}",
                    current_addresses, e
                );
                continue;
            }
        };

        if token_whitelist.contains(&token0) && token_whitelist.contains(&token1) {
            debug!(
                "[UNISWAP_V3_SYNC_POOL_DEBUG][{:?}] Токены в whitelist: token0: {:?}, token1: {:?}",
                current_addresses, token0, token1
            );

            match build_uniswap_v3_pool(
                current_addresses,
                (token0, token1),
                provider.clone(),
                token_cache,
            )
            .await
            {
                Some(pool) => {
                    debug!(
                        "[UNISWAP_V3_SYNC_POOL_DEBUG][{:?}] Пул построен: {:?}",
                        current_addresses, pool.graph_pool_address
                    );
                    if pool.is_active {
                        let graph_inner = graph.load().as_ref().clone();
                        match graph_inner.upsert_pool(pool.clone()).await {
                            Ok(()) => {
                                debug!("[UNISWAP_V3_SYNC_POOL_DEBUG][{:?}] Пул успешно добавлен в граф", current_addresses);
                                graph.store(Arc::new(graph_inner));
                                phase1_active_count.fetch_add(1, Ordering::SeqCst);
                                event_subscriber.subscribed_pools.insert(current_addresses);
                                info!(
                                    "{} Пул с адресом {:?} добавлен в список подписки. Всего подписанных пулов: {}",
                                    "INFO".bright_yellow().blink(),
                                    current_addresses,
                                    event_subscriber.subscribed_pools.len()
                                );

                                if save_per_pool {
                                    if let Err(e) =
                                        graph.load().save_graph_to_json("graph_final.json")
                                    {
                                        warn!(
                                            "[UNISWAP_V3_SYNC_POOL] Ошибка сохранения JSON графа для пула {:?}: {:?}",
                                            current_addresses, e
                                        );
                                    } else {
                                        info!("[UNISWAP_V3_SYNC_POOL] JSON граф сохранён для пула {:?}", current_addresses);
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "[UNISWAP_V3_SYNC_POOL_ERROR][{:?}] Ошибка при добавлении пула в граф: {}",
                                    current_addresses, e
                                );
                                continue;
                            }
                        }
                    }
                }
                None => {
                    warn!(
                        "[UNISWAP_V3_SYNC_POOL][{:?}] Пул не построен",
                        current_addresses
                    );
                }
            }
        } else {
            info!(
                "[UNISWAP_V3_SYNC_POOL_whitelist][{:?}] Пул отфильтрован по whitelist",
                current_addresses
            );
        }

        let processed = phase1_processed.fetch_add(1, Ordering::SeqCst) + 1;
        warn!(
            "[UNISWAP_V3_SYNC_POOL] Прогресс: {}/{} пулов из кэша обработано",
            processed, original_count
        );
        sleep(Duration::from_millis(30)).await;

        debug!(
            "[UNISWAP_V3_SYNC_POOL_DEBUG][{:?}] Конец обработки пула <<<<< ",
            current_addresses
        );
    }

    info!("[UNISWAP_V3_SYNC_POOL] ✅ Пулы из кэша обработаны");

    if let Err(e) = graph.load().save_graph_to_json("graph_final.json") {
        warn!(
            "[UNISWAP_V3_SYNC_POOL] Ошибка сохранения итогового JSON графа: {:?}",
            e
        );
    } else {
        info!("[UNISWAP_V3_SYNC_POOL] Граф успешно сохранён в файл graph_final.json");
    }

    info!(
        "[UNISWAP_V3_SYNC_POOL] Обработано: {} пулов из кэша",
        phase1_active_count.load(Ordering::SeqCst)
    );
    Ok(())
}

/// Получает данные пула Uniswap V3 с использованием мультиколла
async fn fetch_pool_data_multicall(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Http>>,
    provider: Arc<Provider<Http>>,
) -> Option<(U256, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)> {
    debug!(
        "[UNISWAP_V3_MULTICALL_DEBUG] Начало мультиколла для пула {:?}",
        pool_address
    );

    let mut multicall = Multicall::new(provider, None).await.ok()?;
    multicall
        .add_call(pool_contract.liquidity(), true)
        .add_call(pool_contract.slot_0(), true)
        .add_call(pool_contract.tick_spacing(), true)
        .add_call(pool_contract.max_liquidity_per_tick(), true)
        .add_call(pool_contract.fee(), true);

    let result = multicall
        .call::<(u128, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)>()
        .await
        .map_err(|e| {
            warn!(
                "[UNISWAP_V3_MULTICALL] Ошибка мультиколла для пула {:?}: {:?}",
                pool_address, e
            );
            e
        })
        .ok()?;

    debug!(
        "[UNISWAP_V3_MULTICALL_DEBUG] Успешный мультиколл для пула {:?}",
        pool_address
    );
    Some((U256::from(result.0), result.1, result.2, result.3, result.4))
}

/// Обрабатывает данные пула Uniswap V3
pub async fn process_pool_data(
    pool_address: H160,
    pool_contract: Arc<UniswapV3Pool<Provider<Http>>>,
) -> Option<(U256, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)> {
    debug!(
        "[UNISWAP_V3_PROC_DEBUG] Начало обработки данных пула {:?}",
        pool_address
    );

    let result =
        fetch_pool_data_multicall(pool_address, &pool_contract, pool_contract.client()).await?;

    debug!(
        "[UNISWAP_V3_PROC_DEBUG][{:?}] Данные получены: ликвидность: {}, тик: {}, комиссия: {}",
        pool_address, result.0, result.1 .1, result.4
    );

    debug!(
        "[UNISWAP_V3_PROC_DEBUG] Конец обработки данных пула {:?}",
        pool_address
    );
    Some(result)
}
