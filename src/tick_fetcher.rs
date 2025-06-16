use num_traits::identities::Zero;
use ethers::{
    prelude::*,
    types::{Address, U512},
    abi::{self},
};
use im::OrdMap;
use std::sync::Arc;
use std::env;
use log::{debug, info, warn};
use std::result::Result;
use anyhow::Error;

// ABI для Multicall3 с переименованным Result
abigen!(
    Multicall3,
    r#"[{
        "inputs": [
            {
                "components": [
                    { "internalType": "address", "name": "target", "type": "address" },
                    { "internalType": "bool", "name": "allowFailure", "type": "bool" },
                    { "internalType": "bytes", "name": "callData", "type": "bytes" }
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    { "internalType": "bool", "name": "success", "type": "bool" },
                    { "internalType": "bytes", "name": "returnData", "type": "bytes" }
                ],
                "internalType": "struct Multicall3.Call3Result[]",
                "name": "",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }]"#
);

// ABI для TickLens
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

/// Асинхронная функция для получения активных тиков из пула Uniswap V3
pub async fn fetch_active_ticks(
    pool_address: Address,
    client: Arc<Provider<Ws>>,
    current_tick: i32,
    fee: u32,
) -> Result<OrdMap<i32, (i128, U512)>, Error> {
    // Логируем начало выполнения функции
    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] Начало fetch_active_ticks, pool_address: {:?}, current_tick: {}, fee: {}", pool_address, current_tick, fee);

    // Загружаем адреса контрактов Multicall3 и TickLens
    let multicall_address: Address = env::var("MULTICALL3_ADDRESS")?.parse()?;
    let tick_lens_address: Address = env::var("UNISWAP_TICK_LENS_ADDRESS")?.parse()?;
    let multicall = Arc::new(Multicall3::new(multicall_address, client.clone()));
    let tick_lens = Arc::new(TickLens::new(tick_lens_address, client.clone()));
    debug!("[UNISWAP_V3_FETH_ACTIVE_BUILD_DEBUG] Multicall3 создан, адрес: {:?}, TickLens создан, адрес: {:?}", multicall_address, tick_lens_address);

    // Определяем шаг тиков в зависимости от комиссии пула
    let tick_spacing = match fee {
        100 => 1,
        500 => 10,
        3000 => 60,
        10_000 => 200,
        _ => return Err(anyhow::anyhow!("Недопустимый уровень комиссии: {}", fee)),
    };
    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] tick_spacing: {}", tick_spacing);

    // Вычисляем диапазон слов
    let current_word = (current_tick >> 8) as i32;
    let total_batches = match fee {
        100 | 500 | 3000 | 10_000 => 5,
        _ => 10,
    };
    let words_per_batch = 5;
    let min_word = current_word - (total_batches * words_per_batch) as i32;
    let max_word = current_word + (total_batches * words_per_batch) as i32;
    let min_tick_word = -887272 >> 8;
    let max_tick_word = 887272 >> 8;
    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] current_word: {}, min_word: {}, max_word: {}, min_tick_word: {}, max_tick_word: {}", 
        current_word, min_word, max_word, min_tick_word, max_tick_word);

    let mut all_ticks: OrdMap<i32, (i128, U512)> = OrdMap::new();
    let mut non_zero_liquidity = 0;

    // Запрос центрального слова (одиночный вызов)
    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Запрос центрального слова {}", pool_address, current_word);
    match tick_lens
        .get_populated_ticks_in_word(pool_address, current_word.try_into().unwrap())
        .call()
        .await
    {
        Ok(ticks) => {
            debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Центральное слово {}: получено {} тиков", pool_address, current_word, ticks.len());
            for tick in ticks {
                if tick.tick % tick_spacing == 0 {
                    all_ticks.insert(
                        tick.tick,
                        (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                    );
                    if tick.liquidity_net != 0 || !tick.liquidity_gross !=0 {
                        non_zero_liquidity += 1;
                    }
                } else {
                    info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пропущен тик {} в центральном слове {} (не кратен tick_spacing: {})",
                        pool_address, tick.tick, current_word, tick_spacing);
                }
            }
        }
        Err(e) => {
            warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Ошибка для центрального слова {}: {}", pool_address, current_word, e);
        }
    }

    // Подготавливаем вызовы для левой части (один мультиколл)
    let mut left_calls = Vec::new();
    for batch in 0..total_batches {
        let base_word = current_word - ((batch * words_per_batch) as i32);
        for i in 0..words_per_batch {
            let word = base_word - (i as i32);
            if word < min_word || word >= current_word || word < min_tick_word {
                debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Пропуск левого слова {}: вне диапазона", pool_address, word);
                continue;
            }
            left_calls.push((
                tick_lens_address,
                tick_lens
                    .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                    .calldata()
                    .unwrap(),
            ));
        }
    }

 // Выполняем мультиколл для левой части
    if !left_calls.is_empty() {
        debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Выполнение мультиколла для левой части с {} вызовами", pool_address, left_calls.len());
        let left_results = multicall
            .aggregate_3(left_calls.iter().map(|(target, data)| Call3 {
                target: *target,
                call_data: data.clone(),
                allow_failure: true,
            }).collect())
            .call()
            .await?;

        // Обрабатываем результаты левой части
        for (i, result) in left_results.into_iter().enumerate() {
            let word = current_word - ((i / words_per_batch * words_per_batch + i % words_per_batch + 1) as i32);
            if !result.success {
                warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Неудачный вызов для левого слова {}: {:?}", pool_address, word, result.return_data);
                continue;
            }
            match abi::decode(&[abi::ParamType::Array(Box::new(abi::ParamType::Tuple(vec![
                abi::ParamType::Int(24),  // tick
                abi::ParamType::Int(128), // liquidity_net
                abi::ParamType::Uint(128), // liquidity_gross
            ])))], &result.return_data) {
                Ok(decoded) => {
                    let ticks: Vec<tick_lens::PopulatedTick> = decoded[0]
                        .clone()
                        .into_array()
                        .unwrap()
                        .into_iter()
                        .map(|token| {
                            let tuple = token.into_tuple().unwrap();
                            tick_lens::PopulatedTick {
                                tick: I256::from_raw(tuple[0].clone().into_int().unwrap()).as_i32(),
                                liquidity_net: I256::from_raw(tuple[1].clone().into_int().unwrap()).as_i128(),
                                liquidity_gross: tuple[2].clone().into_uint().unwrap().as_u128(),
                            }
                        })
                        .collect();
                    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Левое слово {}: получено {} тиков", pool_address, word, ticks.len());
                    for tick in ticks {
                        if tick.tick % tick_spacing == 0 {
                            all_ticks.insert(
                                tick.tick,
                                (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                            );
                            if tick.liquidity_net != 0 || !tick.liquidity_gross.is_zero() {
                                non_zero_liquidity += 1;
                            }
                        } else {
                            info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пропущен тик {} в левом слове {} (не кратен tick_spacing: {})",
                                pool_address, tick.tick, word, tick_spacing);
                        }
                    }
                }
                Err(e) => {
                    warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Ошибка декодирования для левого слова {}: {}", pool_address, word, e);
                }
            }
        }
    }

    // Подготавливаем вызовы для правой части (один мультиколл)
    let mut right_calls = Vec::new();
    for batch in 0..total_batches {
        let base_word = current_word + ((batch * words_per_batch) as i32);
        for i in 0..words_per_batch {
            let word = base_word + (i as i32);
            if word > max_word || word <= current_word || word > max_tick_word {
                debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Пропуск правого слова {}: вне диапазона", pool_address, word);
                continue;
            }
            right_calls.push((
                tick_lens_address,
                tick_lens
                    .get_populated_ticks_in_word(pool_address, word.try_into().unwrap())
                    .calldata()
                    .unwrap(),
            ));
        }
    }

    // Выполняем мультиколл для правой части
    if !right_calls.is_empty() {
        debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Выполнение мультиколла для правой части с {} вызовами", pool_address, right_calls.len());
        let right_results = multicall
            .aggregate_3(right_calls.iter().map(|(target, data)| Call3 {
                target: *target,
                call_data: data.clone(),
                allow_failure: true,
            }).collect())
            .call()
            .await?;

        // Обрабатываем результаты правой части
        for (i, result) in right_results.into_iter().enumerate() {
            let word = current_word + ((i / words_per_batch * words_per_batch + i % words_per_batch + 1) as i32);
            if !result.success {
                warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Неудачный вызов для правого слова {}: {:?}", pool_address, word, result.return_data);
                continue;
            }
            match abi::decode(&[abi::ParamType::Array(Box::new(abi::ParamType::Tuple(vec![
                abi::ParamType::Int(24),
                abi::ParamType::Int(128),
                abi::ParamType::Uint(128),
            ])))], &result.return_data) {
                Ok(decoded) => {
                    let ticks: Vec<tick_lens::PopulatedTick> = decoded[0]
                        .clone()
                        .into_array()
                        .unwrap()
                        .into_iter()
                        .map(|token| {
                            let tuple = token.into_tuple().unwrap();
                            tick_lens::PopulatedTick {
                                tick: I256::from_raw(tuple[0].clone().into_int().unwrap()).as_i32(),
                                liquidity_net: I256::from_raw(tuple[1].clone().into_int().unwrap()).as_i128(),
                                liquidity_gross: tuple[2].clone().into_uint().unwrap().as_u128(),
                            }
                        })
                        .collect();
                    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Правое слово {}: получено {} тиков", pool_address, word, ticks.len());
                    for tick in ticks {
                        if tick.tick % tick_spacing == 0 {
                            all_ticks.insert(
                                tick.tick,
                                (tick.liquidity_net, U512::from(tick.liquidity_gross)),
                            );
                            if tick.liquidity_net != 0 || !tick.liquidity_gross !=0 {
                                non_zero_liquidity += 1;
                            }
                        } else {
                            info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пропущен тик {} в правом слове {} (не кратен tick_spacing: {})",
                                pool_address, tick.tick, word, tick_spacing);
                        }
                    }
                }
                Err(e) => {
                    warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Ошибка декодирования для правого слова {}: {}", pool_address, word, e);
                }
            }
        }
    }

    // Проверяем, пуста ли карта тиков
    if all_ticks.is_empty() {
        warn!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пустая карта тиков: fee: {}, current_tick: {}, word_range: {} to {}",
            pool_address, fee, current_tick, min_word, max_word);
    } else {
        info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Карта тиков заполнена: fee: {}, current_tick: {}, word_range: {} to {}, total_ticks: {}, non_zero_liquidity: {}",
            pool_address, fee, current_tick, min_word, max_word, all_ticks.len(), non_zero_liquidity);
    }

    // Логируем завершение функции
    debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] Конец fetch_active_ticks, pool_address: {:?}, all_ticks: {}", pool_address, all_ticks.len());
    Ok(all_ticks)
}