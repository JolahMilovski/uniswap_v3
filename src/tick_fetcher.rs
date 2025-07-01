use num_traits::identities::Zero;
use ethers::{
    prelude::*,
    types::{Address, I256, Bytes},
    abi::{self},
};
use im::OrdMap;
use std::sync::Arc;
use std::env;
use tracing::{info, warn};
use std::result::Result;
use anyhow::Error;
use tokio::time::Duration;

// ABI для Multicall3
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

/// Обработка мультиколлов для получения тиков из пула Uniswap V3
/// 
/// # Аргументы
/// * `calls` - Вектор вызовов (адрес контракта и данные вызова)
/// * `multicall` - Экземпляр контракта Multicall3
/// * `pool_address` - Адрес пула Uniswap V3
/// * `tick_spacing` - Шаг тиков в зависимости от комиссии пула
/// * `all_ticks` - Карта для хранения тиков и их ликвидности
/// * `non_zero_liquidity` - Счетчик тиков с ненулевой ликвидностью
/// * `word_offset` - Смещение слова для корректного логирования
///
/// # Возвращает
/// Результат выполнения, содержащий () в случае успеха или ошибку
async fn process_calls(
    calls: Vec<(Address, Bytes)>,
    multicall: &Arc<Multicall3<Provider<Ws>>>,
    pool_address: Address,
    tick_spacing: i32,
    all_ticks: &mut OrdMap<i32, (i128, U256)>,
    non_zero_liquidity: &mut u64,
    word_offset: i16, // Смещение для логирования слов
) -> Result<(), Error> {
    // Пропускаем обработку, если нет вызовов
    if calls.is_empty() {
        return Ok(());
    }
    //debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Выполнение мультиколла с {} вызовами", pool_address, calls.len());
    
    // Выполняем мультиколл с таймаутом 20 секунд
    let results = tokio::time::timeout(Duration::from_secs(30), multicall.aggregate_3(
        calls.iter().map(|(target, data)| Call3 {
            target: *target,
            call_data: data.clone(),
            allow_failure: true,
        }).collect()
    ).call()).await??;

    // Обрабатываем результаты вызовов
    for (i, result) in results.into_iter().enumerate() {
        let word = word_offset + i as i16; // Вычисляем номер слова
        if !result.success {
            warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Неудачный вызов для слова {}: {:?}", pool_address, word, result.return_data);
            continue;
        }
        // Декодируем возвращенные данные
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
                        let tick_value = I256::from_raw(tuple[0].clone().into_int().unwrap());
                        let liquidity_net_value = I256::from_raw(tuple[1].clone().into_int().unwrap());
                        // Проверка диапазона значений
                        if tick_value < I256::from(i32::MIN) || tick_value > I256::from(i32::MAX) {
                            warn!("[UNISWAP_V3_FETH_ACTIVE_WARN][{:?}] Тик вне диапазона i32: {}, пропускаем", pool_address, tick_value);
                            return None;
                        }
                        if liquidity_net_value < I256::from(i128::MIN) || liquidity_net_value > I256::from(i128::MAX) {
                            warn!("[UNISWAP_V3_FETH_ACTIVE_WARN][{:?}] LiquidityNet вне диапазона i128: {}, пропускаем", pool_address, liquidity_net_value);
                            return None;
                        }
                        Some(tick_lens::PopulatedTick {
                            tick: tick_value.as_i32(),
                            liquidity_net: liquidity_net_value.as_i128(),
                            liquidity_gross: tuple[2].clone().into_uint().unwrap().as_u128(),
                        })
                    })
                    .filter_map(|tick| tick) // Фильтруем None значения
                    .collect();
                // Логируем только если есть тики
                if !ticks.is_empty() {
                   // debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG][{:?}] Слово {}: получено {} тиков", pool_address, word, ticks.len());
                }
                // Добавляем тики в карту
                for tick in ticks {
                    if tick.tick % tick_spacing == 0 {
                        all_ticks.insert(
                            tick.tick,
                            (tick.liquidity_net, U256::from(tick.liquidity_gross)),
                        );
                        if tick.liquidity_net != 0 || !tick.liquidity_gross.is_zero() {
                            *non_zero_liquidity += 1;
                        }
                    } else {
                        info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пропущен тик {} (не кратен tick_spacing: {})",
                            pool_address, tick.tick, tick_spacing);
                    }
                }
            }
            Err(e) => {
                warn!("[UNISWAP_V3_FETH_ACTIVE_WARN!][{:?}] Ошибка декодирования для слова {}: {}", pool_address, word, e);
            }
        }
    }
    Ok(())
}

/// Асинхронная функция для получения всех активных тиков из пула Uniswap V3
///
/// # Аргументы
/// * `pool_address` - Адрес пула Uniswap V3
/// * `client` - Провайдер для взаимодействия с блокчейном
/// * `current_tick` - Текущий тик пула
/// * `fee` - Комиссия пула
///
/// # Возвращает
/// Карту тиков с их ликвидностью или ошибку
pub async fn fetch_active_ticks(
    pool_address: Address,
    client: Arc<Provider<Ws>>,
    current_tick: i32,
    fee: u32,
) -> Result<OrdMap<i32, (i128, U256)>, Error> {

    //debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] Начало fetch_active_ticks, pool_address: {:?}, current_tick: {}, fee: {}", pool_address, current_tick, fee);

    // Загружаем адреса контрактов
    let multicall_address: Address = env::var("MULTICALL3_ADDRESS")?.parse()?; // Адрес Multicall3
    let tick_lens_address: Address = env::var("UNISWAP_TICK_LENS_ADDRESS")?.parse()?; // Адрес TickLens
    let multicall = Arc::new(Multicall3::new(multicall_address, client.clone())); // Экземпляр Multicall3
    let tick_lens = Arc::new(TickLens::new(tick_lens_address, client.clone())); // Экземпляр TickLens

    //debug!("[UNISWAP_V3_FETH_ACTIVE_BUILD_DEBUG] Multicall3: {:?}, TickLens: {:?}", multicall_address, tick_lens_address);

    // Определяем шаг тиков в зависимости от комиссии
    let tick_spacing = match fee {
        100 => 1,
        500 => 10,
        3000 => 60,
        10_000 => 200,
        _ => return Err(anyhow::anyhow!("Недопустимый уровень комиссии: {}", fee)),
    };
    //debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] tick_spacing: {}", tick_spacing);

    // Вычисляем диапазон слов
    let min_tick_word = (-887272 >> 8) as i16; // Минимальное слово тиков
    let max_tick_word = (887272 >> 8) as i16; // Максимальное слово тиков
    let batch_size = 2000; // Размер батча для мультиколлов

    //debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] min_tick_word: {}, max_tick_word: {}, batch_size: {}",        min_tick_word, max_tick_word, batch_size);

    let mut all_ticks: OrdMap<i32, (i128, U256)> = OrdMap::new(); // Карта для хранения тиков
    let mut non_zero_liquidity = 0; // Счетчик тиков с ненулевой ликвидностью

    // Подготавливаем вызовы для всех слов
    let mut calls = Vec::new();
    for word in min_tick_word..=max_tick_word {
        calls.push((
            tick_lens_address,
            tick_lens.get_populated_ticks_in_word(pool_address, word).calldata().unwrap(),
        ));
    }

    // Обрабатываем все слова батчами по 1000
    for (batch_index, batch) in calls.chunks(batch_size).enumerate() {
        let word_offset = min_tick_word + (batch_index * batch_size) as i16; // Смещение для текущего батча
        process_calls(
            batch.to_vec(),
            &multicall,
            pool_address,
            tick_spacing,
            &mut all_ticks,
            &mut non_zero_liquidity,
            word_offset,
        ).await?;
    }

    // Логируем результат
    if all_ticks.is_empty() {
        warn!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Пустая карта тиков: fee: {}, current_tick: {}", 
            pool_address, fee, current_tick);
    } else {
        info!("[UNISWAP_V3_FETH_ACTIVE][{:?}] Карта тиков заполнена: fee: {}, current_tick: {}, total_ticks: {}, non_zero_liquidity: {}", 
            pool_address, fee, current_tick, all_ticks.len(), non_zero_liquidity);
    }

    //debug!("[UNISWAP_V3_FETH_ACTIVE_DEBUG] Конец fetch_active_ticks, pool_address: {:?}, all_ticks: {}", pool_address, all_ticks.len());
    Ok(all_ticks)
}