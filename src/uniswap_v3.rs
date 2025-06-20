use crate::tick_fetcher::fetch_active_ticks;
use crate::token::TokenCache;
use crate::token::get_single_token_data;
use crate::uniswap_cache::UniswapPoolCache;
use crate::uniswap_events::UniswapEventSubscriber;
use crate::uniswap_graph::UniswapPool;
use crate::uniswap_graph::UniversalGraph;

use colored::Colorize;
use dashmap::DashSet;
use ethers::contract::abigen;
use ethers::providers::Provider;
use ethers::types::H160;
use ethers::types::U256;
use ethers::types::{Address, U512};
use ethers_contract::Multicall;
use ethers_providers::Ws;
use log::{info, warn, debug};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Duration;
use tokio::time::sleep;
use lazy_static::lazy_static;

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
    let low_low = a_low.checked_mul(b_low).expect("Переполнение при умножении младших частей");
    let high_low = a_high.checked_mul(b_low).expect("Переполнение при умножении старшей и младшей частей");
    let low_high = a_low.checked_mul(b_high).expect("Переполнение при умножении младшей и старшей частей");
    let high_high = a_high.checked_mul(b_high).expect("Переполнение при умножении старших частей");

    // Суммируем части с учетом сдвигов
    let intermediate = low_low
        .checked_add((high_low << 128).checked_add(low_high << 128).expect("Переполнение промежуточной суммы"))
        .expect("Переполнение суммы младших частей");
    let high = high_high
        .checked_add(high_low >> 128)
        .expect("Переполнение старшей суммы")
        .checked_add(low_high >> 128)
        .expect("Переполнение старшей суммы");
    let low = intermediate;

    (high, low)
}

/// Функция для расчета текущей цены, соответствующая Uniswap V3
pub fn calculate_current_price(
    sqrt_price: U256,
    token0_decimals: u8,
    token1_decimals: u8,
) -> Result<U256, String> {
    debug!(
        "[UNISWAP_V3_CCP_DEBUG] Начало расчета текущей цены, sqrt_price: {}, token0_decimals: {}, token1_decimals: {}",
        sqrt_price, token0_decimals, token1_decimals
    );

    // Проверка входных данных
    if sqrt_price.is_zero() {
        warn!("[UNISWAP_V3_CCP_WARN] sqrt_price_x96 равен нулю, пропуск пула");
        return Err("sqrt_price_x96 равен нулю".into());
    }
    if token0_decimals > 18 || token1_decimals > 18 {
        warn!(
            "[UNISWAP_V3_CCP_WARN] Некорректные decimals, token0: {}, token1: {}, пропуск пула",
            token0_decimals, token1_decimals
        );
        return Err("Некорректные decimals".into());
    }

    // Вычисление цены: price = (sqrtPriceX96 * sqrtPriceX96) >> 192
    let (high, low) = full_multiply(sqrt_price, sqrt_price);
    let price = if high >= (U256::from(1) << 64) {
        warn!(
            "[UNISWAP_V3_CCP_WARN] Переполнение при возведении sqrt_price в квадрат, high: {}, пропуск пула",
            high
        );
        return Err("Переполнение при расчете цены".into());
    } else {
        // Сдвиг: (high << 256 + low) >> 192 = (high << 64) + (low >> 192)
        (high << 64)
            .checked_add(low >> 192)
            .ok_or_else(|| {
                warn!("[UNISWAP_V3_CCP_WARN] Переполнение при сдвиге цены, пропуск пула");
                "Переполнение при сдвиге цены".to_string()
            })?
    };
    debug!("[UNISWAP_V3_CCP_DEBUG] Цена (без корректировки): {}", price);

    // Коррекция decimals
    let decimals_adjustment = i32::from(token0_decimals) - i32::from(token1_decimals);
    let final_price = if decimals_adjustment >= 0 {
        let adjustment = U256::from(10)
            .checked_pow(U256::from(decimals_adjustment as u32))
            .ok_or_else(|| {
                warn!(
                    "[UNISWAP_V3_CCP_WARN] Переполнение при корректировке decimals, пропуск пула"
                );
                "Переполнение при корректировке decimals".to_string()
            })?;
        price.checked_mul(adjustment).ok_or_else(|| {
            warn!(
                "[UNISWAP_V3_CCP_WARN] Переполнение при корректировке цены, пропуск пула"
            );
            "Переполнение при корректировке цены".to_string()
        })?
    } else {
        let adjustment = U256::from(10)
            .checked_pow(U256::from((-decimals_adjustment) as u32))
            .ok_or_else(|| {
                warn!(
                    "[UNISWAP_V3_CCP_WARN] Переполнение при корректировке decimals, пропуск пула"
                );
                "Переполнение при корректировке decimals".to_string()
            })?;
        price.checked_div(adjustment).ok_or_else(|| {
            warn!(
                "[UNISWAP_V3_CCP_WARN] Недостаток при корректировке цены, пропуск пула"
            );
            "Недостаток при корректировке цены".to_string()
        })?
    };
    debug!(
        "[UNISWAP_V3_CCP_DEBUG] Конец расчета текущей цены, final_price: {}",
        final_price
    );

    Ok(final_price)
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
        ("0xfff2e50f5f656 sanz932ef12357cf3c7fdcc", 0x4),
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
            ratio = (high << 128)
                .checked_add(low >> 128)
                .ok_or_else(|| {
                    warn!(
                        "[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при сдвиге ratio, пропуск пула"
                    );
                    "Переполнение при сдвиге ratio".to_string()
                })?;
        }
    }

    // Инверсия только для положительных тиков
    if tick > 0 {
        ratio = U256::MAX.checked_div(ratio).ok_or_else(|| {
            warn!(
                "[UNISWAP_V3_SQRT_PRICE_WARN] Переполнение при инверсии ratio, пропускk пула"
            );
            "Переполнение при инверсии ratio".to_string()
        })?;
    } else {
        debug!(
            "[UNISWAP_V3_SQRT_PRICE_DEBUG] Тик <= 0, инверсия ratio не требуется"
        );
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


/*

pub async fn fetch_active_ticks(
    pool_address: Address,
    client: Arc<Provider<Ws>>,
    current_tick: i32,
    fee: u32,
) -> Result<OrdMap<i32, (i128, U512)>, anyhow::Error> {
    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] Начало fetch_active_ticks, pool_address: {:?}, current_tick: {}, fee: {}", pool_address, current_tick, fee);

    let tick_lens_address: Address = env::var("UNISWAP_TICK_LENS_ADDRESS")?.parse()?;
    let tick_lens = Arc::new(TickLens::new(tick_lens_address, client.clone()));
    debug!("[ UNISWAP_V3_FETH_ACTIVE_BUILD_DEBUG ] TickLens создан, адрес: {:?}", tick_lens_address);

    let tick_spacing = match fee {
        100 => 1,
        500 => 10,
        3000 => 60,
        10_000 => 200,
        _ => 0,
    };
    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] tick_spacing: {}", tick_spacing);

    let current_word = (current_tick >> 8) as i32;
    let mut total_batches = match fee {
        100 => 5,
        500 => 5,
       3000 => 5,
     10_000 => 5,
        _ => 10,
    };
    let words_per_batch = match fee {
        100 => 5,
        500 => 5,
       3000 => 5,
     10_000 => 5,
        _ => 5,
    };
    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] current_word: {}, total_batches: {}, words_per_batch: {}", current_word, total_batches, words_per_batch);

    let max_attempts = 1;
    let mut attempt = 0;
    let mut all_ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
    let mut min_word = current_word - (total_batches * words_per_batch) as i32;
    let mut max_word = current_word + (total_batches * words_per_batch) as i32;
    let min_tick_word = -887272 >> 8;
    let max_tick_word = 887272 >> 8;
    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] min_word: {}, max_word: {}, min_tick_word: {}, max_tick_word: {}", min_word, max_word, min_tick_word, max_tick_word);

    loop {
        attempt += 1;
        info!("[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Попытка {}, Tick spacing: {}, Current word: {}, Range: {} to {}",
            pool_address, attempt, tick_spacing, current_word, min_word, max_word
        );
        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] Начало попытки {}, pool_address: {:?}", attempt, pool_address);

        let left_active = Arc::new(AtomicUsize::new(0));
        let right_active = Arc::new(AtomicUsize::new(0));
        let center_active = Arc::new(AtomicUsize::new(0));
        let mut set = JoinSet::new();
        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] JoinSet создан для обработки слов");

        // Запрос центрального слова
        {
            let tick_lens = tick_lens.clone();
            let pool_address = pool_address;
            let center_active = center_active.clone();
            let tick_spacing = tick_spacing;
            set.spawn(async move {
                let mut ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
                debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Запрос центрального слова {}", pool_address, current_word);
                match tick_lens
                    .get_populated_ticks_in_word(pool_address, current_word.try_into().unwrap())
                    .call()
                    .await
                {
                    Ok(list) => {
                        let count = list.len();
                        info!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Центральное слово {}: получено {} тиков", pool_address, current_word, count);
                        for tick in &list {
                            if tick.tick % tick_spacing == 0 {
                                ticks.insert(
                                    tick.tick,
                                    (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                                );
                            } else {
                                info!("[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Пропущен тик {} в центральном слове {} (не кратен tick_spacing: {})",
                                    pool_address, tick.tick, current_word, tick_spacing
                                );
                            }
                        }
                        center_active.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!(
                            "[ UNISWAP_V3_FETH_ACTIVE_WARN! ][{:?}] Ошибка для центрального слова {}: {}",
                            pool_address, current_word, e
                        );
                        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Ошибка при запросе центрального слова {}: {}", pool_address, current_word, e);
                    }
                }
                debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Завершение обработки центрального слова, ticks: {}", pool_address, ticks.len());
                ticks
            });
            sleep(Duration::from_millis(100)).await;
        }

        // Запрос слов слева
        let left_active_clone = left_active.clone();
        for batch in 0..total_batches {
            let base_word = current_word - ((batch * words_per_batch) as i32);
            let tick_lens = tick_lens.clone();
            let pool_address = pool_address;
            let left_active = left_active.clone();
            let tick_spacing = tick_spacing;
            set.spawn(async move {
                let mut ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
                for i in 0..words_per_batch {
                    let word = base_word - (i as i32);
                    if word < min_word || word >= current_word || word < min_tick_word {
                        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Пропуск левого слова {}: вне диапазона", pool_address, word);
                        continue;
                    }
                    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Запрос левого слова {}", pool_address, word);
                    match tick_lens
                        .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                        .call()
                        .await
                    {
                        Ok(list) => {
                            let count = list.len();
                            debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Слово {}: получено {} тиков", pool_address, word, count);
                            for tick in &list {
                                if tick.tick % tick_spacing == 0 {
                                    ticks.insert(
                                        tick.tick,
                                        (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                                    );
                                } else {
                                    info!(
                                        "[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Пропущен тик {} в левом слове {} (не кратен tick_spacing: {})",
                                        pool_address, tick.tick, word, tick_spacing
                                    );
                                }
                            }
                            left_active.fetch_add(count, Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!(
                                "[ UNISWAP_V3_FETH_ACTIVE_WARN! ][{:?}] Ошибка для левого слова {}: {}",
                                pool_address, word, e
                            );
                            debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Ошибка при запросе левого слова {}: {}", pool_address, word, e);
                        }
                    }
                    sleep(Duration::from_millis(300)).await;
                }
                debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Завершение обработки левого батча, ticks: {}", pool_address, ticks.len());
                ticks
            });
            sleep(Duration::from_millis(300)).await;
        }

        // Запрос слов справа
        let right_active_clone = right_active.clone();
        for batch in 0..total_batches {
            let base_word = current_word + ((batch * words_per_batch) as i32);
            let tick_lens = tick_lens.clone();
            let pool_address = pool_address;
            let right_active = right_active.clone();
            let tick_spacing = tick_spacing;
            set.spawn(async move {
                let mut ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
                for i in 0..words_per_batch {
                    let word = base_word + (i as i32);
                    if word > max_word || word <= current_word || word > max_tick_word {
                        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Пропуск правого слова {}: вне диапазона", pool_address, word);
                        continue;
                    }
                    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Запрос правого слова {}", pool_address, word);
                    match tick_lens
                        .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                        .call()
                        .await
                    {
                        Ok(list) => {
                            let count = list.len();
                            debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Слово {}: получено {} тиков", pool_address, word, count);
                            for tick in &list {
                                if tick.tick % tick_spacing == 0 {
                                    ticks.insert(
                                        tick.tick,
                                        (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                                    );
                                } else {
                                    info!(
                                        "[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Пропущен тик {} в правом слове {} (не кратен tick_spacing: {})",
                                        pool_address, tick.tick, word, tick_spacing
                                    );
                                }
                            }
                            right_active.fetch_add(count, Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!(
                                "[ UNISWAP_V3_FETH_ACTIVE_WARN! ][{:?}] Ошибка для правого слова {}: {}",
                                pool_address, word, e
                            );
                            debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Ошибка при запросе правого слова {}: {}", pool_address, word, e);
                        }
                    }
                    sleep(Duration::from_millis(300)).await;
                }
                debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Завершение обработки правого батча, ticks: {}", pool_address, ticks.len());
                ticks
            });
            sleep(Duration::from_millis(300)).await;
        }

        // Собираем все тики
        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Ожидание завершения JoinSet", pool_address);
        while let Some(Ok(partial)) = set.join_next().await {
            all_ticks = all_ticks.union(partial);
            debug!("[ UNISWAP_V3_FETH_ACTIVED_DEBUG ][{:?}] Добавлено тиков из батча, текущий размер all_ticks: {}", pool_address, all_ticks.len());
        }

        // Подсчёт тиков с ненулевой ликвидностью
        let non_zero_liquidity: usize = all_ticks
            .iter()
            .filter(|(_, (liquidity_net, liquidity_gross))| {
                *liquidity_net != 0 || !liquidity_gross.is_zero()
            })
            .count();
        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] non_zero_liquidity: {}", pool_address, non_zero_liquidity);

        // Проверяем, пуста ли тиковая карта
        if all_ticks.is_empty() && attempt < max_attempts {
            info!(
                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Пустая тиковая карта, расширяем диапазон (попытка {})",
                pool_address, attempt
            );
            total_batches = match fee {
                   100 => 1 * (attempt + 1) as usize,
                   500 => 1 * (attempt + 1) as usize,
                  3000 => 1 * (attempt + 1) as usize,
                10_000 => 1 * (attempt + 1) as usize,
                _ => 10,
            };
            min_word = min_word - (total_batches * words_per_batch) as i32;
            max_word = max_word + (total_batches * words_per_batch) as i32;
            min_word = min_word.max(min_tick_word);
            max_word = max_word.min(max_tick_word);
            debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Пустая тиковая карта, новый диапазон: {} to {}", pool_address, min_word, max_word);
            continue;
        }

        if all_ticks.is_empty() {
            warn!(
                "[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Пустая тиковая карта после {} попыток: fee: {}, current_tick: {}, word_range: {} to {}",
                pool_address, attempt, fee, current_tick, min_word, max_word
            );
        } else {
            info!(
                "[ UNISWAP_V3_FETH_ACTIVE ][{:?}] Тиковая карта заполнена после {} попыток: fee: {}, current_tick: {}, word_range: {} to {}, total_ticks: {}, non_zero_liquidity: {}, left_ticks: {}, right_ticks: {}, center_ticks: {}",
                pool_address, attempt, fee, current_tick, min_word, max_word, all_ticks.len(), non_zero_liquidity,
                left_active_clone.load(Ordering::Relaxed), right_active_clone.load(Ordering::Relaxed), center_active.load(Ordering::Relaxed)
            );
        }
        debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ][{:?}] Конец попытки {}, all_ticks: {}", pool_address, attempt, all_ticks.len());
        break;
    }

    debug!("[ UNISWAP_V3_FETH_ACTIVE_DEBUG ] Конец fetch_active_ticks, pool_address: {:?}, all_ticks: {}", pool_address, all_ticks.len());
    Ok(all_ticks)
}
 */
 
pub async fn build_uniswap_v3_pool(
    pool_address: Address,
    tokens: (Address, Address),
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
) -> Option<UniswapPool> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] Начало build_uniswap_v3_pool, pool_address: {:?}", pool_address);

    let (token_a, token_b) = tokens;

    // Получаем данные токенов
    let (token_a_info, token_b_info) = tokio::try_join!(
        get_single_token_data(token_a, provider.clone(), token_cache),
        get_single_token_data(token_b, provider.clone(), token_cache)
    )
    .ok()?;
    info!("[ UNISWAP_V3_BUILD_DEBUG ][{:?}] Данные токенов получены: token_a: {}, token_b: {}", pool_address, token_a_info.symbol, token_b_info.symbol);

    // Получаем данные пула
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

    let (liquidity, slot0_result, tick_spacing, max_liquidity_per_tick, fee) =
        process_pool_data(pool_address, pool_contract.into()).await?;

    debug!("[ UNISWAP_V3_BUILD_DEBUG ][{:?}] Данные пула получены: liquidity: {}, tick: {}, fee: {}", pool_address, liquidity, slot0_result.1, fee);

    let (sqrt_price_x96, tick, _, _, _, _, _) = slot0_result;

    let sqrt_price: U256 = U256::from_str(&sqrt_price_x96.to_string()).unwrap_or_default();

    debug!("[ UNISWAP_V3_BUILD_DEBUG ][{:?}] sqrt_price: {}", pool_address, sqrt_price);

    let current_price =
        calculate_current_price(sqrt_price, token_a_info.decimals, token_b_info.decimals).ok()?;
    debug!("[ UNISWAP_V3_BUILD_DEBUG ][{:?}] current_price: {}", pool_address, current_price);

    let tick_map = fetch_active_ticks(pool_address, provider.clone(), slot0_result.1, fee)
        .await
        .ok()?;
    info!("[ UNISWAP_V3_BUILD_DEBUG ][{:?}] tick_map получен, размер: {}", pool_address, tick_map.len());

    // Проверяем, что тиковая карта не пуста
    if tick_map.is_empty() {
        warn!(
            "[UNISWAP_V3_GRAPH_BUILDER][{:?}] Пропуск пула: пустая тиковая карта ))",
            pool_address
        );
        return None;
    }
    

    let pool = UniswapPool {
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
        uniswap_max_liquidity_per_tick: U256::from(max_liquidity_per_tick),
        uniswap_fee_tier: fee,
        tick_map,
        is_active: true,
    };
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] Конец build_uniswap_v3_pool, pool_address: {:?}", pool_address);

    Some(pool)
}




/// Функция для синхронизации пулов Uniswap V3 с кэша и обновления графа
pub async fn sync_pools(
    graph: Arc<UniversalGraph>,
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
    pool_cache: Arc<UniswapPoolCache>,
    token_whitelist: &DashSet<Address>,
    event_subscriber: Arc<UniswapEventSubscriber>,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ] Начало sync_pools");

    let save_per_pool = env::var("SAVE_GRAPH_PER_POOL")
        .map(|v| v == "true")
        .unwrap_or(false);

    let (original_addresses, original_count) = (
        pool_cache.pool_addresses.clone(),
        pool_cache.pool_addresses.len(),
    );
    info!("[ UNISWAP_V3_SYNC_POOL ] Начинаем обработку {} пулов из кэша", original_count);
    debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ] original_count: {}", original_count);

    let phase1_active_count = Arc::new(AtomicUsize::new(0));
    let phase1_processed = Arc::new(AtomicUsize::new(0));

    for current_addresses in original_addresses {
        debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ][{:?}] >>>>> Старт обработки пула ", current_addresses);

        let pool_contract = UniswapV3Pool::new(current_addresses, provider.clone());

                // Проверка валидности пула
      

        let token0 = match pool_contract.token_0().call().await {
            Ok(t) => t,
            Err(e) => {
                warn!("[ UNISWAP_V3_SYNC_POOL ][{:?}] Ошибка получения token0: {:?}", current_addresses, e);
                continue;
            }
        };

        sleep(Duration::from_millis(100)).await;

        let token1 = match pool_contract.token_1().call().await {
            Ok(t) => t,
            Err(e) => {
                warn!("[ UNISWAP_V3_SYNC_POOL ][{:?}] Ошибка получения token1: {:?}", current_addresses, e);
                continue;
            }
        };

        if token_whitelist.contains(&token0) && token_whitelist.contains(&token1) {
            debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ][{:?}] Токены в whitelist: token0: {:?}, token1: {:?}", current_addresses, token0, token1);

            match build_uniswap_v3_pool(
                current_addresses,
                (token0, token1),
                provider.clone(),
                &token_cache,
            ).await {
                Some(pool) => {
                    debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ][{:?}] Пул построен: {:?}", current_addresses, pool.uniswap_pool_address);
                    sleep(Duration::from_millis(222)).await;

                    if pool.is_active {
                        graph.upsert_pool(pool.clone());
                        phase1_active_count.fetch_add(1, Ordering::SeqCst);
                        event_subscriber.subscribed_pools.insert(current_addresses);
                        info!(
                            "{} Пул с адресом {:?} добавлен в список подписки. Всего подписанных пулов: {}",
                            "INFO".bright_yellow().blink(),
                            current_addresses,
                            event_subscriber.subscribed_pools.len()
                        );

                        if save_per_pool {
                            if let Err(e) = graph.save_graph_to_json("graph_final.json") {
                                warn!("[ UNISWAP_V3_SYNC_POOL ] Ошибка сохранения JSON графа для пула {:?}: {:?}", current_addresses, e);
                            } else {
                                info!("[ UNISWAP_V3_SYNC_POOL ] JSON граф сохранён для пула {:?}", current_addresses);
                            }
                        }
                    }
                }
                None => {
                    warn!("[ UNISWAP_V3_SYNC_POOL ][{:?}] Пул не построен", current_addresses);
                }
            }
        } else {
            info!("[ UNISWAP_V3_SYNC_POOL_whitelist ][{:?}] Пул отфильтрован по whitelist", current_addresses);
        }

        let processed = phase1_processed.fetch_add(1, Ordering::SeqCst) + 1;
        info!("[ UNISWAP_V3_SYNC_POOL ] Прогресс: {}/{} пулов из кэша обработано", processed, original_count);
        sleep(Duration::from_millis(30)).await;

        debug!("[ UNISWAP_V3_SYNC_POOL_DEBUG ][{:?}] Конец обработки пула <<<<< ", current_addresses);
    }

    info!("[ UNISWAP_V3_SYNC_POOL ] ✅ Пулы из кэша обработаны");

    if let Err(e) = graph.save_graph_to_json("graph_final.json") {
        warn!("[ UNISWAP_V3_SYNC_POOL ] Ошибка сохранения итогового JSON графа: {:?}", e);
    } else {
        info!("[ UNISWAP_V3_SYNC_POOL ] Граф успешно сохранён в файл graph_final.json");
    }

    info!("[ UNISWAP_V3_SYNC_POOL ] Обработано: {} пулов из кэша", phase1_active_count.load(Ordering::SeqCst));
    Ok(())
}

/*

pub async fn fetch_tick_spacing(
    pool_address: H160,
    provider: Arc<Provider<Ws>>) -> Option<i32> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] Начало fetch_tick_spacing, pool_address: {:?}", pool_address);

    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());

    let result = pool_contract.tick_spacing().call().await.ok();
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] Конец fetch_tick_spacing, pool_address: {:?}, result: {:?}", pool_address, result);

    result
}
*/

// Универсальная retry-обёртка
pub async fn retry_async<T, F, Fut>(mut f: F, retries: usize, delay_ms: u64) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    for attempt in 0..retries {
        match f().await {
            Some(result) => return Some(result),
            None => {
                debug!("[ RETRY ] Попытка {} не удалась, повтор через {} мс", attempt + 1, delay_ms);
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
    warn!("[ RETRY ] Все {} попыток не увенчались успехом", retries);
    None
}

// Обёртка для liquidity
pub async fn fetch_pool_liquidity(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<U512> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] fetch_pool_liquidity: {:?}", pool_address);

    retry_async(
        || async {
            pool_contract.liquidity().call().await.ok().map(U512::from)
        },
        5,
        300,
    ).await
}

// Обёртка для slot0
pub async fn fetch_pool_slot0(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<(U256, i32, u16, u16, u16, u8, bool)> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] fetch_pool_slot0: {:?}", pool_address);

    retry_async(
        || async {
            pool_contract.slot_0().call().await.ok()
        },
        5,
        300,
    ).await
}

// Обёртка для tick_spacing
pub async fn fetch_pool_tick_spacing(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<i32> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] fetch_pool_tick_spacing: {:?}", pool_address);

    retry_async(
        || async {
            pool_contract.tick_spacing().call().await.ok()
        },
        5,
        300,
    ).await
}

// Обёртка для max_liquidity_per_tick
pub async fn fetch_pool_max_liquidity(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<u128> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] fetch_pool_max_liquidity: {:?}", pool_address);

    retry_async(
        || async {
            pool_contract.max_liquidity_per_tick().call().await.ok()
        },
        5,
        300,
    ).await
}

// Обёртка для fee()
pub async fn fetch_pool_fee(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<u32> {
    debug!("[ UNISWAP_V3_BUILD_DEBUG ] fetch_pool_fee: {:?}", pool_address);

    retry_async(
        || async {
            pool_contract.fee().call().await.ok()
        },
        5,
        300,
    ).await
}



 
/// Получает данные пула Uniswap V3 с использованием мультиколла
///
/// # Описание
/// Выполняет одновременный запрос к контракту пула для получения ликвидности, slot0, tick_spacing,
/// max_liquidity_per_tick и fee, минимизируя количество сетевых вызовов.
///
/// # Параметры
/// * `pool_address` - Адрес пула
/// * `pool_contract` - Контракт пула Uniswap V3
/// * `provider` - WebSocket-провайдер
///
/// # Возвращаемое значение
/// * `Option<(U512, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)>` - Данные пула или None при ошибке
async fn fetch_pool_data_multicall(
    pool_address: H160,
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
    provider: Arc<Provider<Ws>>,
) -> Option<(
    U256,
    (U256, i32, u16, u16, u16, u8, bool),
    i32,
    u128,
    u32,
)> {
    // Логируем начало мультиколла
    debug!("[UNISWAP_V3_MULTICALL_DEBUG] Начало мультиколла для пула {:?}", pool_address);

    // Инициализируем мультиколл
    let mut multicall = Multicall::new(provider, None).await.ok()?;
    // Добавляем вызовы функций контракта
    multicall
        .add_call(pool_contract.liquidity(), true) // Ликвидность пула
        .add_call(pool_contract.slot_0(), true) // Данные slot0 (sqrtPriceX96, tick, etc.)
        .add_call(pool_contract.tick_spacing(), true) // Интервал тиков
        .add_call(pool_contract.max_liquidity_per_tick(), true) // Максимальная ликвидность на тик
        .add_call(pool_contract.fee(), true); // Комиссия пула

    // Выполняем мультиколл и обрабатываем результат
    let result = multicall
        .call::<(u128, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)>()
        .await
        .map_err(|e| {
            warn!("[UNISWAP_V3_MULTICALL] Ошибка мультиколла для пула {:?}: {:?}", pool_address, e);
            e
        })
        .ok()?;

    // Логируем успешное выполнение
    debug!("[UNISWAP_V3_MULTICALL_DEBUG] Успешный мультиколл для пула {:?}", pool_address);
    Some((
        U256::from(result.0), // Конвертируем ликвидность в U256
        result.1, // Данные slot0
        result.2, // tick_spacing
        result.3, // max_liquidity_per_tick
        result.4, // fee
    ))
}

/// Обрабатывает данные пула Uniswap V3
///
/// # Описание
/// Вызывает мультиколл для получения всех необходимых данных пула (ликвидность, slot0, tick_spacing,
/// max_liquidity_per_tick, fee) и возвращает их в структурированном виде.
///
/// # Параметры
/// * `pool_address` - Адрес пула
/// * `pool_contract` - Контракт пула Uniswap V3
///
/// # Возвращаемое значение
/// * `Option<(U512, (U256, i32, u16, u16, u16, u8, bool), i32, u128, u32)>` - Данные пула или None при ошибке
pub async fn process_pool_data(
    pool_address: H160,
    pool_contract: Arc<UniswapV3Pool<Provider<Ws>>>,
) -> Option<(
    U256,
    (ethers::types::U256, i32, u16, u16, u16, u8, bool),
    i32,
    u128,
    u32,
)> {
    // Логируем начало обработки
    debug!("[UNISWAP_V3_PROC_DEBUG] Начало обработки данных пула {:?}", pool_address);

    // Вызываем мультиколл для получения всех данных
    let result = fetch_pool_data_multicall(pool_address, &pool_contract, pool_contract.client()).await?;

    // Логируем успешное получение данных
    debug!("[UNISWAP_V3_PROC_DEBUG][{:?}] Данные получены: ликвидность: {}, тик: {}, комиссия: {}", 
        pool_address, result.0, result.1.1, result.4);

    // Возвращаем результат
    debug!("[UNISWAP_V3_PROC_DEBUG] Конец обработки данных пула {:?}", pool_address);
    Some(result)
}



