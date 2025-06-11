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
use ethers::types::{Address, U512};
use ethers_providers::Ws;
use futures::stream;
use im::OrdMap;
use log::info;
use log::warn;

use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::stream::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
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

/// Функция для расчета текущей цены
pub fn calculate_current_price(
    sqrt_price: U512,
    token0_decimals: u8,
    token1_decimals: u8,
) -> Result<U512, String> {
    // 1. Проверка входных данных
    if sqrt_price.is_zero() {
        return Err("sqrt_price_x96 is zero".into());
    }

    // 2. Вычисление с максимальной точностью
    let sqrt_price_squared = sqrt_price
        .checked_pow(U512::from(2))
        .ok_or("Overflow in squaring")?;

    // 3. Масштабирование перед делением
    let scale_factor = U512::from(10).pow(U512::from(token1_decimals.max(token0_decimals) + 18));
    let scaled_price = sqrt_price_squared
        .checked_mul(scale_factor)
        .ok_or("Overflow in scaling")?
        / (U512::one() << 192);

    // 4. Коррекция decimals
    let decimals_adjustment = i32::from(token1_decimals) - i32::from(token0_decimals);
    let final_price = if decimals_adjustment > 0 {
        scaled_price.checked_mul(U512::from(10).pow(U512::from(decimals_adjustment as u32)))
    } else {
        scaled_price.checked_div(U512::from(10).pow(U512::from((-decimals_adjustment) as u32)))
    }
    .ok_or("Overflow in decimals adjustment")?;

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



pub async fn fetch_active_ticks(
    pool_address: Address,
    client: Arc<Provider<Ws>>,
    current_tick: i32,
    fee: u32,
) -> Result<OrdMap<i32, (i128, U512)>, anyhow::Error> {
    let tick_lens_address: Address = env::var("UNISWAP_TICK_LENS_ADDRESS")?.parse()?;
    let tick_lens = Arc::new(TickLens::new(tick_lens_address, client.clone()));

    let tick_spacing = match fee {
        100 => 1,
        500 => 10,
        3000 => 60,
        10_000 => 200,
        _ => 0,
    };

    // Вычисляем центральное слово для текущего тика
    let current_word = (current_tick >> 8) as i32;

    let mut total_batches = match fee {
           100 => 1,
           500 => 1,
          3000 => 1,
        10_000 => 1,
        _ => 10,
    };
    let words_per_batch = match fee {
           100 => 1,
           500 => 1,
          3000 => 1,
        10_000 => 1,
        _ => 5,
    };

    let max_attempts = 1; // Максимальное количество попыток
    let mut attempt = 0;
    let mut all_ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
    let mut min_word = current_word - (total_batches * words_per_batch) as i32;
    let mut max_word = current_word + (total_batches * words_per_batch) as i32;
    let min_tick_word = -887272 >> 8; // Минимальное слово для Uniswap V3
    let max_tick_word = 887272 >> 8;  // Максимальное слово для Uniswap V3

    loop {
        attempt += 1;
        info!(
            "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Попытка {}, Tick spacing: {}, Current word: {}, Range: {} to {}",
            pool_address, attempt, tick_spacing, current_word, min_word, max_word
        );

        let left_active = Arc::new(AtomicUsize::new(0));
        let right_active = Arc::new(AtomicUsize::new(0));
        let mut set = JoinSet::new();
              
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
                        continue; // Пропускаем уже обработанные слова или за пределами
                    }
                    match tick_lens
                        .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                        .call()
                        .await
                    {
                        Ok(list) => {
                            let count = list.len();
                            for tick in &list {
                                if tick.tick % tick_spacing == 0 {
                                    ticks.insert(
                                        tick.tick,
                                        (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                                    );
                                } else {
                                    info!(
                                        "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Пропущен тик {} в левом слове {} (не кратен tick_spacing: {})",
                                        pool_address, tick.tick, word, tick_spacing
                                    );
                                }
                            }
                            left_active.fetch_add(count, Ordering::Relaxed);
                           
                        }
                        Err(e) => {
                            warn!(
                                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Ошибка для левого слова {}: {}",
                                pool_address, word, e
                            );
                        }
                    }
                   sleep(Duration::from_millis(100)).await;
                }
                ticks
            });
            sleep(Duration::from_millis(200)).await;
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
                        continue; // Пропускаем уже обработанные слова или за пределами
                    }
                    match tick_lens
                        .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                        .call()
                        .await
                    {
                        Ok(list) => {
                            let count = list.len();
                            for tick in &list {
                                if tick.tick % tick_spacing == 0 {
                                    ticks.insert(
                                        tick.tick,
                                        (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                                    );
                                } else {
                                    info!(
                                        "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Пропущен тик {} в правом слове {} (не кратен tick_spacing: {})",
                                        pool_address, tick.tick, word, tick_spacing
                                    );
                                }
                            }
                            right_active.fetch_add(count, Ordering::Relaxed);
                         
                        }
                        Err(e) => {
                            warn!(
                                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Ошибка для правого слова {}: {}",
                                pool_address, word, e
                            );
                        }
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                ticks
            });
            sleep(Duration::from_millis(200)).await;
        }

        // Собираем все тики
        while let Some(Ok(partial)) = set.join_next().await {
            all_ticks = all_ticks.union(partial);
        }

        // Подсчёт тиков с ненулевой ликвидностью
        let non_zero_liquidity: usize = all_ticks
            .iter()
            .filter(|(_, (liquidity_net, liquidity_gross))| {
                *liquidity_net != 0 || !liquidity_gross.is_zero()
            })
            .count();

        // Проверяем, пуста ли тиковая карта
        if all_ticks.is_empty() && attempt < max_attempts {
            info!(
                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Пустая тиковая карта, расширяем диапазон (попытка {})",
                pool_address, attempt
            );
            // Увеличиваем total_batches на 50 с каждой попыткой
            total_batches = match fee {
                   100 => 1 * (attempt + 1) as usize,
                   500 => 1 * (attempt + 1) as usize,
                  3000 => 1 * (attempt + 1) as usize,
                10_000 => 1 * (attempt + 1) as usize,
                _ => 10,
            };
            // Обновляем границы диапазона
            min_word = min_word - (total_batches * words_per_batch) as i32;
            max_word = max_word + (total_batches * words_per_batch) as i32;
            // Ограничиваем границы
            min_word = min_word.max(min_tick_word);
            max_word = max_word.min(max_tick_word);
            continue; // Повторяем цикл с новым диапазоном
        }

        // Логирование результата
        if all_ticks.is_empty() {
            warn!(
                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Пустая тиковая карта после {} попыток: fee: {}, current_tick: {}, word_range: {} to {}",
                pool_address,
                attempt,
                fee,
                current_tick,
                min_word,
                max_word
            );
        } else {
            info!(
                "[UNISWAP_V3_СИНХРОНИЗАЦИЯ][{:?}] Тиковая карта заполнена после {} попыток: fee: {}, current_tick: {}, word_range: {} to {}, total_ticks: {}, non_zero_liquidity: {}, left_ticks: {}, right_ticks: {}",
                pool_address,
                attempt,
                fee,
                current_tick,
                min_word,
                max_word,
                all_ticks.len(),
                non_zero_liquidity,
                left_active_clone.load(Ordering::Relaxed),
                right_active_clone.load(Ordering::Relaxed)
            );
        }
        break; // Выходим из цикла
    }

    Ok(all_ticks)
}



pub async fn sync_pools(
    graph: Arc<UniversalGraph>,
    provider: Arc<Provider<Ws>>,
    token_cache: &TokenCache,
    pool_cache: Arc<Mutex<UniswapPoolCache>>,
    token_whitelist: &DashSet<Address>,
    event_subscriber: Arc<UniswapEventSubscriber>,
) -> Result<(), Box<dyn std::error::Error>> {
    let save_per_pool = env::var("SAVE_GRAPH_PER_POOL")
        .map(|v| v == "true")
        .unwrap_or(false);

    // === Фаза 1: обработка пулов из кэша ===
    let (original_addresses, original_count) = {
        let pool_cache_lock = pool_cache.lock().await;
        (
            pool_cache_lock.pool_addresses.clone(),
            pool_cache_lock.pool_addresses.len(),
        )
    };
    info!("[UNISWAP_V3_КЭШ] Начинаем обработку {} пулов из кэша", original_count);
    let phase1_active_count = Arc::new(AtomicUsize::new(0));
    let phase1_processed = Arc::new(AtomicUsize::new(0));
    stream::iter(original_addresses)
        .for_each_concurrent(2, |current_addresses| {
            let provider = provider.clone();
            let token_cache = Arc::clone(&token_cache);
            let graph = Arc::clone(&graph);
            let phase1_active_count = phase1_active_count.clone();
            let phase1_processed = phase1_processed.clone();
            let token_whitelist = token_whitelist.clone();
            let event_subscriber = Arc::clone(&event_subscriber);
            async move {
                let pool_contract = UniswapV3Pool::new(current_addresses, provider.clone());
                let token0_call = pool_contract.token_0();
                let token1_call = pool_contract.token_1();
                match tokio::try_join!(token0_call.call(), token1_call.call()) {
                    Ok((token0, token1))
                        if token_whitelist.contains(&token0) && token_whitelist.contains(&token1) =>
                    {
                        sleep(Duration::from_millis(222)).await;
                        if let Some(pool) = build_uniswap_v3_pool(
                            current_addresses,
                            (token0, token1),
                            provider.clone(),
                            &token_cache,
                        )
                        .await
                        {
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
                                        warn!("[UNISWAP_V3_КЭШ] Ошибка сохранения JSON графа для пула {:?}: {:?}", current_addresses, e);
                                    } else {
                                        info!("[UNISWAP_V3_КЭШ] JSON граф сохранён для пула {:?}", current_addresses);
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => info!(
                        "[UNISWAP_V3_КЭШ_whitelist] Пул {:?} отфильтрован по whitelist",
                        current_addresses
                    ),
                    Err(e) => warn!(
                        "[UNISWAP_V3_КЭШ] Ошибка проверки токенов пула {:?}: {:?}",
                        current_addresses, e
                    ),
                }
                let processed = phase1_processed.fetch_add(1, Ordering::SeqCst) + 1;
                info!(
                    "[UNISWAP_V3_КЭШ] Прогресс: {}/{} пулов из кэша обработано",
                    processed, original_count
                );
                sleep(Duration::from_millis(800)).await;
            }
        })
        .await;
    info!("[UNISWAP_V3_КЭШ] ✅ Пулы из кэша обработаны");

    // Сохранение графа после обработки пулов
    if let Err(e) = graph.save_graph_to_json("graph_final.json") {
        warn!(
            "[UNISWAP_V3_СИНХРОНИЗАЦИЯ] Ошибка сохранения итогового JSON графа: {:?}",
            e
        );
    } else {
        info!("[UNISWAP_V3_СИНХРОНИЗАЦИЯ] Граф успешно сохранён в файл graph_final.json");
    }

    info!(
        "[UNISWAP_V3_ИТОГ] Обработано: {} пулов из кэша",
        phase1_active_count.load(Ordering::SeqCst)
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
    )
    .ok()?;

    // Получаем данные пула
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
    let (liquidity, slot0_result, tick_spacing, max_liquidity_per_tick, fee) =
        process_pool_data(pool_address, pool_contract.into()).await?;

    let (sqrt_price_x96, tick, _, _, _, _, _) = slot0_result;

    let sqrt_price = U512::from_str(&sqrt_price_x96.to_string()).unwrap_or_default();

    let current_price =
        calculate_current_price(sqrt_price, token_a_info.decimals, token_b_info.decimals).ok()?;

    let tick_map = fetch_active_ticks(pool_address, provider.clone(), slot0_result.1, fee)
        .await
        .ok()?;
/*
if tick_map.is_empty() {
    warn!(
        "[ UNISWAP_V3_GRAPH_BUILDER ][{:?}] Пропуск пула: пустая тиковая карта ))",
        pool_address
    );
    return None;
}
*/

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

/// Асинхронно запрашивает и возвращает `tick_spacing` для данного пула
pub async fn fetch_tick_spacing(pool_address: H160, provider: Arc<Provider<Ws>>) -> Option<i32> {
    // Создаём client-контракт пула
    let pool_contract = UniswapV3Pool::new(pool_address, provider.clone());
    sleep(Duration::from_millis(265)).await;
    // Делаем вызов метода `tick_spacing` и возвращаем результат, если Ok

    pool_contract.tick_spacing().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `liquidity` для данного пула
/// liquidity - это общее количество ликвидности, которое присвоено пулу
pub async fn fetch_pool_liquidity(pool_contract: &UniswapV3Pool<Provider<Ws>>) -> Option<U512> {
    sleep(Duration::from_millis(202)).await;
    pool_contract.liquidity().call().await.ok().map(U512::from)
}

/// Асинхронно запрашивает и возвращает `slot_0` для данного пула
/// slot_0 - это структурное поле, которое хранит текущие значения
/// sqrt_price, tick, observation_index, observation_cardinality, observation_cardinality_next, fee_protocol, unlocked
pub async fn fetch_pool_slot0(
    pool_contract: &UniswapV3Pool<Provider<Ws>>,
) -> Option<(ethers::types::U256, i32, u16, u16, u16, u8, bool)> {
    sleep(Duration::from_millis(205)).await;
    pool_contract.slot_0().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `tick_spacing` для данного пула
/// tick_spacing - это шаг, на который тик-интервалы (range) разделяются
pub async fn fetch_pool_tick_spacing(pool_contract: &UniswapV3Pool<Provider<Ws>>) -> Option<i32> {
    sleep(Duration::from_millis(210)).await;
    pool_contract.tick_spacing().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `max_liquidity_per_tick` для данного пула
/// max_liquidity_per_tick - это максимальное значение ликвидности, которое может быть
/// присвоено отдельному тик-интервалу.
pub async fn fetch_pool_max_liquidity(pool_contract: &UniswapV3Pool<Provider<Ws>>) -> Option<u128> {
    sleep(Duration::from_millis(230)).await;
    pool_contract.max_liquidity_per_tick().call().await.ok()
}

/// Асинхронно запрашивает и возвращает `fee` для данного пула
///
/// `fee` - это комиссия, которая берется за обмен токенов в пуле.
/// Величина комиссии измеряется в 1/10000 от 1% (то есть 0.01%).
pub async fn fetch_pool_fee(pool_contract: &UniswapV3Pool<Provider<Ws>>) -> Option<u32> {
    sleep(Duration::from_millis(250)).await;
    pool_contract.fee().call().await.ok()
}

pub async fn process_pool_data(
    pool_address: H160,
    pool_contract: Arc<UniswapV3Pool<Provider<Ws>>>,
) -> Option<(
    U512,
    (ethers::types::U256, i32, u16, u16, u16, u8, bool),
    i32,
    u128,
    u32,
)> {
    let liquidity_fut = {
        let pool_contract = pool_contract.clone();
        let pool_address = pool_address.clone();
        async move {
            fetch_pool_liquidity(&pool_contract).await.map_or_else(
                || {
                    warn!(
                        "[UNISWAP_V3] Pool {:?} failed to get liquidity",
                        pool_address
                    );
                    None
                },
                Some,
            )
        }
    };

    let slot0_fut = {
        let pool_contract = pool_contract.clone();
        let pool_address = pool_address.clone();
        async move {
            fetch_pool_slot0(&pool_contract).await.map_or_else(
                || {
                    warn!("[UNISWAP_V3] Pool {:?} failed to get slot0", pool_address);
                    None
                },
                Some,
            )
        }
    };

    let tick_spacing_fut = {
        let pool_contract = pool_contract.clone();
        let pool_address = pool_address.clone();
        async move {
            fetch_pool_tick_spacing(&pool_contract).await.map_or_else(
                || {
                    warn!(
                        "[UNISWAP_V3] Pool {:?} failed to get tick spacing",
                        pool_address
                    );
                    None
                },
                Some,
            )
        }
    };

    let max_liquidity_fut = {
        let pool_contract = pool_contract.clone();
        let pool_address = pool_address.clone();
        async move {
            fetch_pool_max_liquidity(&pool_contract).await.map_or_else(
                || {
                    warn!(
                        "[UNISWAP_V3] Pool {:?} failed to get max liquidity",
                        pool_address
                    );
                    None
                },
                Some,
            )
        }
    };

    let fee_fut = {
        let pool_contract = pool_contract.clone();
        let pool_address = pool_address.clone();
        async move {
            fetch_pool_fee(&pool_contract).await.map_or_else(
                || {
                    warn!("[UNISWAP_V3] Pool {:?} failed to get fee", pool_address);
                    None
                },
                Some,
            )
        }
    };

    let (liquidity_option, slot0_option, tick_spacing_option, max_liquidity_option, fee_option) = tokio::join!(
        liquidity_fut,
        slot0_fut,
        tick_spacing_fut,
        max_liquidity_fut,
        fee_fut
    );

    let liquidity = liquidity_option?;
   

    let slot0 = slot0_option?;
    let tick_spacing = tick_spacing_option?;
    let max_liquidity = max_liquidity_option?;
    let fee = fee_option?;

    Some((liquidity, slot0, tick_spacing, max_liquidity, fee))
}
