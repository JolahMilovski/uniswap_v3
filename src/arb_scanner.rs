use crate::aave_v3_flash_monitor::AaveTokenLiquidity;
use crate::uniswap_graph::UniswapPool;
use crate::uniswap_v3::tick_to_sqrt_price;

use ethers::{types::{Address, U256}, utils::hex};
use tracing::{debug, error, info, warn};
use lazy_static::lazy_static;
use std::collections::HashMap;

// Статическая карта минимальных порогов прибыли для различных токенов
// 
// Используется для определения минимальной прибыли, необходимой для выполнения арбитражной сделки
// с конкретным токеном. Значения указаны в базовых единицах (wei) соответствующих токенов.
// 
// # Структура данных
// * Ключ - адрес токена (Address)
// * Значение - минимальный порог прибыли (U256)
lazy_static! {
    pub static ref MIN_PROFIT_THRESHOLD_BY_TOKEN: HashMap<Address, U256> = {
        let mut min_profit_by_token = HashMap::new();

        // MAI 100 MAI
        min_profit_by_token.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // USDC - 100 USDC
        min_profit_by_token.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));

        // USDC.e - USD Coin (Bridged) 100 USDC
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));

        // ARB - Arbitrum Token (345 ARB)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(345_000_000_000_000_000_000u128));

        // RDNT - Radiant (0.038 RDNT)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(38_000_000_000_000_000u128));

        // WETH - Wrapped Ether (0.038 ETH)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(38_000_000_000_000_000u128));
                                                                                                                                   

        // USDT - 100 Tether USD 
        min_profit_by_token.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));

        // WBTC - Wrapped Bitcoin (0.0001 BTC)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(100_000u128));

        // LUSD - LUSD Token (100 LUSD)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // GMX - GMX Token (0.038 GMX)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(38_489_000_000_000_000u128));

        // LINK - Chainlink Token (8 LINK)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(8_000_000_000_000_000_000u128));

        // wstETH - Wrapped Staked Ether ??
        min_profit_by_token.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(33_000_000_000_000_000u128));

        // RDNT - Radiant ??
        min_profit_by_token.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(38_000_000_000_000_000u128));

        // MAGIC - MAGIC ??
        min_profit_by_token.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // PENDLE - Pendle Token ??
        min_profit_by_token.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // EQB - Equilibria Token ??
        min_profit_by_token.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));

        // DAI - Dai Stablecoin (100 DAI)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // RDNT - Radiant (0.038 RDNT)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(38_000_000_000_000_000u128));

        // AAVE - Aave Token (~0.353 AAVE)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ba5ddd1f9d7f570dc94a51479a000e3bce967196").unwrap()), U256::from(353_700_000_000_000_000u128));
        
        min_profit_by_token
    };
}



/// Проверяет входные данные для расчета арбитражной возможности
/// 
/// # Аргументы
/// * `pool_path` - Массив пулов Uniswap, через которые будет проходить арбитраж
/// * `start_token` - Адрес начального токена для арбитража
/// * `aave_liquidity` - Структура с информацией о ликвидности токенов в протоколе Aave
///
/// # Возвращаемое значение
/// * `Ok((U256, U256))` - Кортеж из доступной ликвидности и минимального порога прибыли
/// * `Err(String)` - Ошибка с описанием проблемы
fn validate_inputs(
    event_id: usize,
    pool_path: &[UniswapPool],
    start_token: Address,
    aave_liquidity: &AaveTokenLiquidity,
    path_index: usize,
) -> Result<(U256, U256), String> {
    let start_token_symbol = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(symbol, _)| symbol.as_str())
        .unwrap_or("UNKNOWN");
    info!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Проверка входных данных для токена {} ({:?})", event_id, path_index, start_token_symbol, start_token);

    if pool_path.is_empty() {
        warn!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Пустой путь пулов", event_id, path_index);
        return Err(format!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Пустой путь пулов", event_id, path_index));
    }

    let available_liquidity = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(_, liquidity)| *liquidity)
        .unwrap_or(U256::zero());
    
    debug!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Доступная ликвидность Aave для токена {} ({:?}): {}", 
        event_id, path_index, start_token_symbol, start_token, available_liquidity);

    if available_liquidity.is_zero() {
        warn!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Нет ликвидности Aave для токена {} ({:?})", event_id, path_index, start_token_symbol, start_token);
        return Err(format!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Нет ликвидности Aave для токена {} ({:?})", event_id, path_index, start_token_symbol, start_token));
    }

    let min_profit_threshold = MIN_PROFIT_THRESHOLD_BY_TOKEN
        .get(&start_token)
        .copied()
        .ok_or_else(|| {
            error!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Токен {} ({:?}) не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", event_id, path_index, start_token_symbol, start_token);
            format!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Токен {} ({:?}) не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", event_id, path_index, start_token_symbol, start_token)
        })?;

    debug!("[ UNISWAP_ARB_SCANNER_validate_inputs event:{} path_index:{} ] Минимальный порог прибыли: {}", event_id, path_index, min_profit_threshold);

    Ok((available_liquidity, min_profit_threshold))
}


/// Рассчитывает целевую сумму вывода с учетом минимального порога прибыли и комиссии
/// 
/// # Аргументы
/// * `min_profit_threshold` - Минимальный порог прибыли в базовых единицах токена
///
/// # Возвращаемое значение
/// * `Ok(U256)` - Рассчитанная целевая сумма вывода

fn calculate_target_amount_out (
    event_id: usize,
    min_profit_threshold: U256,
    path_index: usize,
) -> Result<U256, String> {
    let fee_denominator = U256::from(10000);
    let fee_numerator = U256::from(9);
    debug!(
        "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] Input: min_profit_threshold = {}, fee_denominator = {}, fee_numerator = {}",
        event_id, path_index, min_profit_threshold, fee_denominator, fee_numerator
    );

    let fee_adjustment = checked_op(
        || fee_denominator.checked_sub(fee_numerator),
        &format!(
            "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] Fee adjustment error",
            event_id, path_index
        ),
    )?;

    debug!(
        "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] Fee adjustment: {}",
        event_id, path_index, fee_adjustment
    );

    let target_amount_out = checked_op(
        || {
            min_profit_threshold
                .checked_mul(fee_denominator)
                .and_then(|x| x.checked_div(fee_adjustment))
        },
        &format!(
            "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] Overflow calculating target_amount_out: min_profit_threshold * {} / {}",
            event_id, path_index, fee_denominator, fee_adjustment
        ),
    )?;

    // Добавляем проверку на максимальное значение
    let max_amount_out = U256::from(10u128.pow(30)); // Ограничение на 10^30
    if target_amount_out > max_amount_out {
        debug!(
            "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] target_amount_out {} exceeds max_amount_out {}, capping",
            event_id, path_index, target_amount_out, max_amount_out
        );
        return Ok(max_amount_out);
    }

    debug!(
        "[UNISWAP_ARB_SCANNER_calculate_target_amount_out event:{} path_index:{}] Result: target_amount_out = {}",
        event_id, path_index, target_amount_out
    );

    Ok(target_amount_out)
}

/// Вспомогательная функция для обработки арифметических операций с проверкой на переполнение
/// 
/// # Аргументы
/// * `op` - Замыкание, выполняющее арифметическую операцию
/// * `error_msg` - Сообщение об ошибке в случае переполнения
///
/// # Возвращаемое значение
/// * `Ok(T)` - Результат операции
/// * `Err(String)` - Ошибка с указанным сообщением
fn checked_op<T, F: FnOnce() -> Option<T>>(op: F, error_msg: &str) -> Result<T, String> {
    op().ok_or_else(|| {
        warn!("[UNISWAP_ARB_SCANNER] {}", error_msg);
        error_msg.to_string()
    })
}

/// Обновляет ликвидность пула на основе net_liquidity и направления свопа
/// # Аргументы
/// * `current_liquidity` - Текущая ликвидность пула
/// * `net_liquidity` - Чистое изменение ликвидности для текущего тика
/// * `zero_for_one` - Направление свопа (true для token1 -> token0, false для token0 -> token1)
/// * `pool_index` - Индекс пула для логирования
/// # Возвращаемое значение
/// * `Ok(U256)` - Обновленная ликвидность
/// * `Err(String)` - Ошибка при преобразовании или переполнении
fn update_liquidity(
    event_id: usize,
    current_liquidity: U256,
    net_liquidity: i128,
    zero_for_one: bool,
    pool_index: usize,
    path_index: usize,
) -> Result<U256, String> {
    debug!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Пул {}: Входные параметры: current_liquidity = {}, net_liquidity = {}, zero_for_one = {}", 
        event_id, path_index, pool_index, current_liquidity, net_liquidity, zero_for_one);

    let net_liquidity_abs = net_liquidity.abs();
    let net_liquidity_u256 = U256::try_from(net_liquidity_abs).map_err(|e| {
        let msg = format!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Ошибка преобразования net_liquidity {} в пуле {}: {}", event_id, path_index, net_liquidity, pool_index, e);
        warn!("{}", msg);
        msg
    })?;

    debug!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Промежуточный результат: net_liquidity_u256 = {}", event_id, path_index, net_liquidity_u256);

    let updated_liquidity = if zero_for_one {
        if net_liquidity >= 0 {
            checked_op(
                || current_liquidity.checked_sub(net_liquidity_u256),
                &format!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Переполнение при вычитании ликвидности: current_liquidity = {}, net_liquidity_u256 = {} в пуле {}", event_id, path_index, current_liquidity, net_liquidity_u256, pool_index)
            )
        } else {
            checked_op(
                || current_liquidity.checked_add(net_liquidity_u256),
                &format!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Переполнение при добавлении ликвидности: current_liquidity = {}, net_liquidity_u256 = {} в пуле {}", event_id, path_index, current_liquidity, net_liquidity_u256, pool_index)
            )
        }
    } else {
        if net_liquidity >= 0 {
            checked_op(
                || current_liquidity.checked_add(net_liquidity_u256),
                &format!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Переполнение при добавлении ликвидности: current_liquidity = {}, net_liquidity_u256 = {} в пуле {}", event_id, path_index, current_liquidity, net_liquidity_u256, pool_index)
            )
        } else {
            checked_op(
                || current_liquidity.checked_sub(net_liquidity_u256),
                &format!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Переполнение при вычитании ликвидности: current_liquidity = {}, net_liquidity_u256 = {} в пуле {}", event_id, path_index, current_liquidity, net_liquidity_u256, pool_index)
            )
        }
    }?;
    debug!("[ UNISWAP_ARB_SCANNER_update_liquidity event:{} path_index:{} ] Пул {}: Обновленная ликвидность = {}", event_id, path_index, pool_index, updated_liquidity);

    Ok(updated_liquidity)
}

/// Вычисляет параметры свопа для одного тика
/// 
/// # Аргументы
/// * `current_liquidity` - Текущая ликвидность пула
/// * `current_sqrt_price` - Текущая цена пула (sqrt_price_x96)
/// * `target_sqrt_price` - Целевая цена для текущего тика
/// * `remaining_amount_out` - Оставшийся требуемый выходной объем
/// * `fee_pips` - Комиссия пула в pips (1/1,000,000)
/// * `zero_for_one` - Направление свопа (true для token1 -> token0, false для token0 -> token1)
/// * `two_pow_96` - Константа 2^96 для вычислений
/// * `pool_index` - Индекс пула для логирования
/// * `tick_idx` - Индекс текущего тика для логирования
///
/// # Возвращаемое значение
/// * `Ok((U256, U256, U256, U256))` - (amount_in, amount_out, fee_amount, total_amount_in_step)
/// * `Err(String)` - Ошибка при переполнении или делении
fn compute_swap_for_tick(
    event_id: usize,
    current_liquidity: U256,
    current_sqrt_price: U256,
    target_sqrt_price: U256,
    mut remaining_amount_out: U256,
    fee_pips: u32,
    zero_for_one: bool,
    two_pow_96: U256,
    pool_index: usize,
    tick_idx: i32,
    path_index: usize,
) -> Result<(U256, U256, U256, U256), String> {
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Input: current_liquidity = {}, current_sqrt_price = {}, target_sqrt_price = {}, remaining_amount_out = {}, fee_pips = {}, zero_for_one = {}, two_pow_96 = {}, tick_idx = {}",
        event_id, path_index, pool_index, current_liquidity, current_sqrt_price, target_sqrt_price, remaining_amount_out, fee_pips, zero_for_one, two_pow_96, tick_idx
    );

    if current_liquidity.is_zero() {
        warn!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Zero liquidity for tick {}",
            event_id, path_index, pool_index, tick_idx
        );
        return Err(format!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Zero liquidity in pool {} for tick {}",
            event_id, path_index, pool_index, tick_idx
        ));
    }

    // Добавляем проверку на максимальное значение remaining_amount_out
    let max_remaining_amount_out = U256::from(10u128.pow(30)); // Ограничение на 10^30
    if remaining_amount_out > max_remaining_amount_out {
        debug!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] remaining_amount_out {} exceeds max_remaining_amount_out {}, capping",
            event_id, path_index, pool_index, remaining_amount_out, max_remaining_amount_out
        );
        remaining_amount_out = max_remaining_amount_out;
    }

    let delta_sqrt_price = checked_op(
        || Some(current_sqrt_price.abs_diff(target_sqrt_price)),
        &format!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating delta_sqrt_price: current_sqrt_price = {}, target_sqrt_price = {} in pool {}",
            event_id, path_index, current_sqrt_price, target_sqrt_price, pool_index
        ),
    )?;
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Delta sqrt price: {}",
        event_id, path_index, pool_index, delta_sqrt_price
    );

    let max_amount_out = if zero_for_one {
        checked_op(
            || current_liquidity.checked_mul(delta_sqrt_price).and_then(|x| x.checked_div(two_pow_96)),
            &format!(
                "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating max_amount_out (zero_for_one): current_liquidity = {}, delta_sqrt_price = {}, two_pow_96 = {} in pool {}",
                event_id, path_index, current_liquidity, delta_sqrt_price, two_pow_96, pool_index
            ),
        )?
    } else {
        checked_op(
            || current_liquidity.checked_mul(delta_sqrt_price).and_then(|x| x.checked_div(current_sqrt_price)),
            &format!(
                "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating max_amount_out (!zero_for_one): current_liquidity = {}, delta_sqrt_price = {}, current_sqrt_price = {} in pool {}",
                event_id, path_index, current_liquidity, delta_sqrt_price, current_sqrt_price, pool_index
            ),
        )?
    };
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Max amount out: {} for tick {}",
        event_id, path_index, pool_index, max_amount_out, tick_idx
    );

    let amount_out = remaining_amount_out.min(max_amount_out);
    if amount_out.is_zero() {
        warn!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Zero amount_out for tick {}, skipping",
            event_id, path_index, pool_index, tick_idx
        );
        return Ok((U256::zero(), U256::zero(), U256::zero(), U256::zero()));
    }
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Using amount_out: {} for tick {}",
        event_id, path_index, pool_index, amount_out, tick_idx
    );

    let amount_in = if zero_for_one {
        checked_op(
            || {
                current_liquidity
                    .checked_mul(delta_sqrt_price)
                    .and_then(|x| x.checked_mul(two_pow_96))
                    .and_then(|x| x.checked_div(target_sqrt_price))
            },
            &format!(
                "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating amount_in (zero_for_one): current_liquidity = {}, delta_sqrt_price = {}, two_pow_96 = {}, target_sqrt_price = {} in pool {}",
                event_id, path_index, current_liquidity, delta_sqrt_price, two_pow_96, target_sqrt_price, pool_index
            ),
        )?
    } else {
        checked_op(
            || current_liquidity.checked_mul(delta_sqrt_price).and_then(|x| x.checked_div(two_pow_96)),
            &format!(
                "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating amount_in (!zero_for_one): current_liquidity = {}, delta_sqrt_price = {}, two_pow_96 = {} in pool {}",
                event_id, path_index, current_liquidity, delta_sqrt_price, two_pow_96, pool_index
            ),
        )?
    };
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Amount in: {} for tick {}",
        event_id, path_index, pool_index, amount_in, tick_idx
    );

    let fee_denom = U256::from(1_000_000u32);
    let fee_pips_u256 = U256::from(fee_pips);
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Fee params: fee_pips_u256 = {}, fee_denom = {}",
        event_id, path_index, pool_index, fee_pips_u256, fee_denom
    );

    let fee_amount = checked_op(
        || amount_in.checked_mul(fee_pips_u256).and_then(|x| x.checked_div(fee_denom)),
        &format!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow calculating fee_amount: amount_in = {}, fee_pips_u256 = {}, fee_denom = {} in pool {}",
            event_id, path_index, amount_in, fee_pips_u256, fee_denom, pool_index
        ),
    )?;
    let total_amount_in_step = checked_op(
        || amount_in.checked_add(fee_amount),
        &format!(
            "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{}] Overflow adding amount_in and fee_amount: amount_in = {}, fee_amount = {} in pool {}",
            event_id, path_index, amount_in, fee_amount, pool_index
        ),
    )?;
    debug!(
        "[UNISWAP_ARB_SCANNER_compute_swap_for_tick event:{} path_index:{} pool_index:{}] Result: fee_amount = {}, total_amount_in_step = {}",
        event_id, path_index, pool_index, fee_amount, total_amount_in_step
    );

    Ok((amount_in, amount_out, fee_amount, total_amount_in_step))
}



/// Обрабатывает тики пула для вычисления входного объема
/// 
/// # Аргументы
/// * `tick_iter` - Итератор по тикам пула
/// * `current_liquidity` - Начальная ликвидность пула
/// * `current_sqrt_price` - Начальная цена пула (sqrt_price_x96)
/// * `remaining_amount_out` - Требуемый выходной объем
/// * `fee_pips` - Комиссия пула в pips (1/1,000,000)
/// * `zero_for_one` - Направление свопа (true для token1 -> token0, false для token0 -> token1)
/// * `two_pow_96` - Константа 2^96 для вычислений
/// * `pool_index` - Индекс пула для логирования
///
/// # Возвращаемое значение
/// * `Ok((U256, U256, U256, U256))` - (total_amount_in, remaining_amount_out, current_sqrt_price, current_liquidity)
/// * `Err(String)` - Ошибка при обработке тиков
fn process_ticks<'a>(
    event_id: usize,
    tick_iter: Box<dyn Iterator<Item = (&'a i32, &'a (i128, U256))> + 'a>,
    mut current_liquidity: U256,
    mut current_sqrt_price: U256,
    mut remaining_amount_out: U256,
    fee_pips: u32,
    zero_for_one: bool,
    two_pow_96: U256,
    pool_index: usize,
    path_index: usize,
) -> Result<(U256, U256, U256, U256), String> {
    info!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Начало обработки тиков, входные параметры: remaining_amount_out = {}, current_liquidity = {}, current_sqrt_price = {}, fee_pips = {}, zero_for_one = {}, two_pow_96 = {}", 
        event_id, path_index, pool_index, remaining_amount_out, current_liquidity, current_sqrt_price, fee_pips, zero_for_one, two_pow_96);
    
    if current_liquidity.is_zero() {
        warn!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Нулевая ликвидность, пропуск обработки тиков", event_id, path_index, pool_index);
        return Err(format!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Нулевая ликвидность в пуле {}", event_id, path_index, pool_index));
    }

    let mut total_amount_in = U256::zero();
    for (tick_idx, (net_liquidity, _)) in tick_iter {
        let target_sqrt_price = tick_to_sqrt_price(*tick_idx).map_err(|e| {
            let msg = format!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Ошибка преобразования тика {}: {}", event_id, path_index, tick_idx, e);
            warn!("{}", msg);
            msg
        })?;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Промежуточный результат: target_sqrt_price = {} для тика {}", event_id, path_index, pool_index, target_sqrt_price, tick_idx);

        let reached_target = if zero_for_one {
            current_sqrt_price <= target_sqrt_price
        } else {
            current_sqrt_price >= target_sqrt_price
        };
        if reached_target {
            debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Достигнута целевая цена для тика {}, завершаем итерацию", event_id, path_index, pool_index, tick_idx);
            break;
        }

        let (amount_in, amount_out, fee_amount, total_amount_in_step) = compute_swap_for_tick(
            event_id,
            current_liquidity,
            current_sqrt_price,
            target_sqrt_price,
            remaining_amount_out,
            fee_pips,
            zero_for_one,
            two_pow_96,
            pool_index,
            *tick_idx,
            path_index,
        )?;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Промежуточный результат: amount_in = {}, amount_out = {}, fee_amount = {}, total_amount_in_step = {} для тика {}", 
            event_id, path_index, pool_index, amount_in, amount_out, fee_amount, total_amount_in_step, tick_idx);

        total_amount_in = checked_op(
            || total_amount_in.checked_add(total_amount_in_step),
            &format!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Переполнение при накоплении total_amount_in: total_amount_in = {}, total_amount_in_step = {} в пуле {}", event_id, path_index, total_amount_in, total_amount_in_step, pool_index)
        )?;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Промежуточный результат: total_amount_in = {}", event_id, path_index, pool_index, total_amount_in);

        remaining_amount_out = checked_op(
            || remaining_amount_out.checked_sub(amount_out),
            &format!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Недостаточная remaining_amount_out: remaining_amount_out = {}, amount_out = {} в пуле {}", event_id, path_index, remaining_amount_out, amount_out, pool_index)
        )?;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Оставшаяся amount_out = {}", event_id, path_index, pool_index, remaining_amount_out);

        current_liquidity = update_liquidity(event_id, current_liquidity, *net_liquidity, zero_for_one, pool_index, path_index)?;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Обновленная ликвидность = {}", event_id, path_index, pool_index, current_liquidity);

        current_sqrt_price = target_sqrt_price;
        debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Новая current_sqrt_price = {}", event_id, path_index, pool_index, current_sqrt_price);

        if remaining_amount_out.is_zero() {
            debug!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: remaining_amount_out израсходована, завершаем обработку тиков", event_id, path_index, pool_index);
            break;
        }
    }

    info!("[ UNISWAP_ARB_SCANNER_process_ticks event:{} path_index:{} ] Пул {}: Завершение обработки тиков, total_amount_in = {}, remaining_amount_out = {}, current_sqrt_price = {}, current_liquidity = {}", 
        event_id, path_index, pool_index, total_amount_in, remaining_amount_out, current_sqrt_price, current_liquidity);
    Ok((total_amount_in, remaining_amount_out, current_sqrt_price, current_liquidity))
}




/// Обрабатывает пул Uniswap V3 для расчета входного объема, необходимого для получения заданного выходного объема
/// 
/// # Аргументы
/// * `pool` - Пул Uniswap V3
/// * `pool_index` - Индекс пула в пути для логирования
/// * `current_token` - Текущий токен (выходной для пула)
/// * `remaining_amount_out` - Требуемый выходной объем
/// * `two_pow_96` - Константа 2^96 для вычислений
///
/// # Возвращаемое значение
/// * `Ok((U256, Address))` - (total_amount_in, next_token) - входной объем и следующий токен
/// * `Err(String)` - Ошибка при обработке пула
fn process_pool(
    event_id: usize,
    pool: &UniswapPool,
    pool_index: usize,
    current_token: Address,
    remaining_amount_out: U256,
    two_pow_96: U256,
    path_index: usize,
) -> Result<(U256, Address), String> {
    info!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Обработка пула {}: {:?}", event_id, path_index, pool_index, pool.uniswap_pool_address);
    let zero_for_one = pool.uniswap_token_b == current_token;
    let token_out = if zero_for_one { pool.uniswap_token_a } else { pool.uniswap_token_b };
    let token_out_symbol = if zero_for_one { &pool.uniswap_token_a_symbol } else { &pool.uniswap_token_b_symbol };
    let current_token_symbol = if current_token == pool.uniswap_token_a { &pool.uniswap_token_a_symbol } else { &pool.uniswap_token_b_symbol };
    debug!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Входные параметры: zero_for_one = {}, token_in = {} ({:?}), token_out = {} ({:?})", 
        event_id, path_index, pool_index, zero_for_one, current_token_symbol, current_token, token_out_symbol, token_out);
    
    let current_sqrt_price = pool.uniswap_sqrt_price;
    let current_liquidity = pool.uniswap_liquidity;
    debug!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Промежуточные параметры: current_sqrt_price = {}, current_liquidity = {}", event_id, path_index, pool_index, current_sqrt_price, current_liquidity);

    if current_liquidity.is_zero() {
        warn!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Нулевая ликвидность для токена {} ({:?})", event_id, path_index, pool_index, token_out_symbol, token_out);
        return Err(format!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Нулевая ликвидность в пуле {} для токена {} ({:?})", event_id, path_index, pool_index, token_out_symbol, token_out));
    }

    let tick_iter: Box<dyn Iterator<Item = (&i32, &(i128, U256))> + '_> = if zero_for_one {
        Box::new(pool.tick_map.range(..=pool.uniswap_tick_current).rev())
    } else {
        Box::new(pool.tick_map.range(pool.uniswap_tick_current..))
    };

    let fee_pips = pool.uniswap_fee_tier;
    debug!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Промежуточные параметры: fee_pips = {}, tick_current = {}", event_id, path_index, pool_index, fee_pips, pool.uniswap_tick_current);

    let (total_amount_in, remaining_amount_out, _final_sqrt_price, _final_liquidity) = process_ticks(
        event_id,
        tick_iter,
        current_liquidity,
        current_sqrt_price,
        remaining_amount_out,
        fee_pips,
        zero_for_one,
        two_pow_96,
        pool_index,
        path_index,
    )?;

    if remaining_amount_out > U256::zero() {
        warn!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Недостаточная ликвидность для remaining_amount_out = {} токена {} ({:?})", event_id, path_index, pool_index, remaining_amount_out, token_out_symbol, token_out);
        return Err(format!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Недостаточная ликвидность в пуле {} для remaining_amount_out = {} токена {} ({:?})", event_id, path_index, pool_index, remaining_amount_out, token_out_symbol, token_out));
    }

    let next_token = if zero_for_one { pool.uniswap_token_a } else { pool.uniswap_token_b };
    let next_token_symbol = if zero_for_one { &pool.uniswap_token_a_symbol } else { &pool.uniswap_token_b_symbol };
    debug!("[ UNISWAP_ARB_SCANNER_process_pool event:{} path_index:{} ] Пул {}: Результат: total_amount_in = {}, next_token = {} ({:?})", event_id, path_index, pool_index, total_amount_in, next_token_symbol, next_token);
    Ok((total_amount_in, next_token))
}



/// Проверяет конечное состояние расчета и валидирует результат
/// 
/// # Аргументы
/// * `current_token` - Текущий токен в конце пути
/// * `start_token` - Начальный токен, с которого начался путь
/// * `start_amount` - Рассчитанная сумма для заимствования
/// * `available_liquidity` - Доступная ликвидность в протоколе Aave
///
/// # Возвращаемое значение
/// * `Ok(U256)` - Валидированная сумма заимствования
/// * `Err(String)` - Ошибка с описанием проблемы
fn validate_final_state(
    event_id: usize,
    current_token: Address,
    start_token: Address,
    start_amount: U256,
    available_liquidity: U256,
    aave_liquidity: &AaveTokenLiquidity,
    path_index: usize,
) -> Result<U256, String> {
    let start_token_symbol = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(symbol, _)| symbol.as_str())
        .unwrap_or("UNKNOWN");
    let current_token_symbol = aave_liquidity
        .token_info
        .get(&current_token)
        .map(|(symbol, _)| symbol.as_str())
        .unwrap_or("UNKNOWN");
    debug!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Входные параметры: current_token = {} ({:?}), start_token = {} ({:?}), start_amount = {}, available_liquidity = {}", 
        event_id, path_index, current_token_symbol, current_token, start_token_symbol, start_token, start_amount, available_liquidity);

    if current_token != start_token {
        warn!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Некорректный путь: конечный токен {} ({:?}) != стартовый {} ({:?})", 
            event_id, path_index, current_token_symbol, current_token, start_token_symbol, start_token);
        return Err(format!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Некорректный путь: конечный токен {} ({:?}) != стартовый {} ({:?})", 
            event_id, path_index, current_token_symbol, current_token, start_token_symbol, start_token));
    }
    debug!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Проверка соответствия токенов пройдена успешно", event_id, path_index);

    if start_amount > available_liquidity {
        warn!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Требуемая сумма {} превышает доступную ликвидность Aave {} для токена {} ({:?})", 
            event_id, path_index, start_amount, available_liquidity, start_token_symbol, start_token);
        return Err(format!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Требуемая сумма {} превышает доступную ликвидность Aave {} для токена {} ({:?})", 
            event_id, path_index, start_amount, available_liquidity, start_token_symbol, start_token));
    }
    debug!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Проверка ликвидности пройдена успешно", event_id, path_index);

    info!("[ UNISWAP_ARB_SCANNER_validate_final_state event:{} path_index:{} ] Успешно рассчитана сумма заимствования: {} для токена {} ({:?})", 
        event_id, path_index, start_amount, start_token_symbol, start_token);
    Ok(start_amount)
}




/// Рассчитывает оптимальную сумму заимствования в протоколе Aave для арбитража
/// 
/// # Аргументы
/// * `pool_path` - Массив пулов Uniswap V3, через которые будет проходить арбитраж
/// * `start_token` - Адрес токена, который будет заимствован из Aave
/// * `aave_liquidity` - Структура с информацией о ликвидности токенов в Aave
///
/// # Возвращаемое значение
/// * `Ok(U256)` - Оптимальная сумма заимствования в случае успеха
/// * `Err(String)` - Ошибка с описанием проблемы
pub fn calculate_aave_borrow_amount(
    event_id: usize,
    pool_path: &[UniswapPool],
    start_token: Address,
    aave_liquidity: &AaveTokenLiquidity,
    path_index: usize,
) -> Result<U256, String> {
    let start_token_symbol = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(symbol, _)| symbol.as_str())
        .unwrap_or("UNKNOWN");
    if start_token_symbol == "UNKNOWN" {
        error!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Токен {:?} не найден в AaveTokenLiquidity", event_id, path_index, start_token);
        return Err(format!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Токен {:?} не найден в AaveTokenLiquidity", event_id, path_index, start_token));
    }
    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Начало расчета суммы заимствования для токена {} ({:?})", event_id, path_index, start_token_symbol, start_token);
    let (available_liquidity, min_profit_threshold) = validate_inputs(event_id, pool_path, start_token, aave_liquidity, path_index)?;
    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Промежуточные результаты: available_liquidity = {}, min_profit_threshold = {}", event_id, path_index, available_liquidity, min_profit_threshold);

    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Расчет целевой суммы с учетом комиссии Aave", event_id, path_index);
    let mut remaining_amount_out = calculate_target_amount_out(event_id, min_profit_threshold, path_index)?;
    let mut current_token = start_token;
    let two_pow_96 = U256::from(1u128 << 96);
    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Промежуточные параметры: remaining_amount_out = {}, current_token = {} ({:?}), two_pow_96 = {}", 
        event_id, path_index, remaining_amount_out, start_token_symbol, current_token, two_pow_96);

    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Начало обратного прохода по пути пулов", event_id, path_index);
    for (i, pool) in pool_path.iter().enumerate().rev() {
        let current_token_symbol = if current_token == pool.uniswap_token_a {
            &pool.uniswap_token_a_symbol
        } else if current_token == pool.uniswap_token_b {
            &pool.uniswap_token_b_symbol
        } else {
            aave_liquidity
                .token_info
                .get(&current_token)
                .map(|(symbol, _)| symbol.as_str())
                .unwrap_or("UNKNOWN")
        };
        debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Обработка пула #{} с текущим токеном {} ({:?})", event_id, path_index, pool_path.len() - i, current_token_symbol, current_token);
        let (total_amount_in, next_token) = process_pool(event_id, pool, i, current_token, remaining_amount_out, two_pow_96, path_index)?;
        let next_token_symbol = if next_token == pool.uniswap_token_a {
            &pool.uniswap_token_a_symbol
        } else if next_token == pool.uniswap_token_b {
            &pool.uniswap_token_b_symbol
        } else {
            aave_liquidity
                .token_info
                .get(&next_token)
                .map(|(symbol, _)| symbol.as_str())
                .unwrap_or("UNKNOWN")
        };
        debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Пул {}: Промежуточный результат: total_amount_in = {}, next_token = {} ({:?})", 
            event_id, path_index, i, total_amount_in, next_token_symbol, next_token);
        remaining_amount_out = total_amount_in;
        current_token = next_token;
    }

    debug!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Проверка конечного состояния и валидация результата", event_id, path_index);
    let start_amount = validate_final_state(event_id, current_token, start_token, remaining_amount_out, available_liquidity, aave_liquidity, path_index)?;
    warn!("[ UNISWAP_ARB_SCANNER_calculate_aave_borrow_amount event:{} path_index:{} ] Расчет завершен успешно. Итоговая сумма заимствования: {} для токена {} ({:?})", 
        event_id, path_index, start_amount, start_token_symbol, start_token);
    Ok(start_amount)
}
/*

pub fn get_next_sqrt_price_from_input(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_in: U256,
    zero_for_one: bool,
) -> U256 {
    info!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Вычисление следующей sqrt цены. zero_for_one: {}, sqrt_price_x96: {}, liquidity: {}, amount_in: {}",
        zero_for_one, sqrt_price_x96, liquidity, amount_in
    );
    if zero_for_one {
        let numerator = liquidity << 96;
        let product = amount_in.checked_mul(sqrt_price_x96).unwrap_or(U256::MAX);
        let denominator = numerator.checked_add(product).unwrap_or(U256::MAX);
        let result = numerator
            .checked_mul(sqrt_price_x96)
            .unwrap_or(U256::MAX)
            .checked_div(denominator)
            .unwrap_or(U256::zero());
        result
    } else {
        let product = amount_in
            .checked_mul(U256::from(1u128 << 96))
            .unwrap_or(U256::MAX);
        let result = sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap_or(U256::zero()))
            .unwrap_or(U256::MAX);
        result
    }
}

 
/// Вычисляет параметры одного шага свопа в пуле Uniswap V3
pub fn compute_swap_step(
    sqrt_price_x96: U256,
    target_sqrt_price_x96: U256,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: u32,
    zero_for_one: bool,
) -> Result<(U256, U256, U256, U256), String> {
    info!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Начало шага свопа. sqrt_price_x96: {}, target_sqrt_price_x96: {}, liquidity: {}, amount_remaining: {}, fee_pips: {}, zero_for_one: {}", 
        sqrt_price_x96, target_sqrt_price_x96, liquidity, amount_remaining, fee_pips, zero_for_one
    );

    if liquidity.is_zero() {
        debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Нулевая ликвидность, пропуск шага");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    if zero_for_one && target_sqrt_price_x96 > sqrt_price_x96 {
        debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] zero_for_one: Целевая цена выше текущей, пропуск шага");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }
    if !zero_for_one && target_sqrt_price_x96 < sqrt_price_x96 {
        debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] !zero_for_one: Целевая цена ниже текущей, пропуск шага");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    let fee_denom = U256::from(1_000_000u32);
    let fee_pips_u256 = U256::from(fee_pips);
    let fee_amount = amount_remaining
        .checked_mul(fee_pips_u256)
        .and_then(|x| x.checked_div(fee_denom))
        .ok_or_else(|| {
            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете fee_amount");
            "Переполнение при расчете fee_amount".to_string()
        })?;
    let amount_remaining_less_fee = amount_remaining
        .checked_sub(fee_amount)
        .ok_or_else(|| {
            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Недостаточная amount_remaining для fee_amount");
            "Недостаточная amount_remaining для fee_amount".to_string()
        })?;
    debug!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] fee_amount = {}, amount_remaining_less_fee = {}", 
        fee_amount, amount_remaining_less_fee
    );

    let next_sqrt_price = if zero_for_one {
        let numerator = liquidity << 96;
        let product = amount_remaining_less_fee
            .checked_mul(sqrt_price_x96)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при умножении amount_remaining_less_fee * sqrt_price_x96");
                "Переполнение при умножении amount_remaining_less_fee * sqrt_price_x96".to_string()
            })?;
        let denominator = numerator
            .checked_add(product)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при сложении numerator + product");
                "Переполнение при сложении numerator + product".to_string()
            })?;
        numerator
            .checked_mul(sqrt_price_x96)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при умножении numerator * sqrt_price_x96");
                "Переполнение при умножении numerator * sqrt_price_x96".to_string()
            })?
            .checked_div(denominator)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при делении numerator * sqrt_price_x96 / denominator");
                "Переполнение при делении numerator * sqrt_price_x96 / denominator".to_string()
            })?
    } else {
        let product = amount_remaining_less_fee
            .checked_mul(U256::from(1u128 << 96))
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при умножении amount_remaining_less_fee * 2^96");
                "Переполнение при умножении amount_remaining_less_fee * 2^96".to_string()
            })?;
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при делении product / liquidity");
                "Переполнение при делении product / liquidity".to_string()
            })?)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при сложении sqrt_price_x96 + product / liquidity");
                "Переполнение при сложении sqrt_price_x96 + product / liquidity".to_string()
            })?
    };
    debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] next_sqrt_price = {}", next_sqrt_price);

    let reached_target = if zero_for_one {
        next_sqrt_price <= target_sqrt_price_x96
    } else {
        next_sqrt_price >= target_sqrt_price_x96
    };
    debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] reached_target = {}", reached_target);

    let (used_sqrt_price, delta) = if reached_target {
        (target_sqrt_price_x96, sqrt_price_x96.abs_diff(target_sqrt_price_x96))
    } else {
        (next_sqrt_price, sqrt_price_x96.abs_diff(next_sqrt_price))
    };
    debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] used_sqrt_price = {}, delta = {}", used_sqrt_price, delta);

    let two_pow_96 = U256::from(1u128 << 96);
    let (amount_in, amount_out) = if zero_for_one {
        let amount_in = (liquidity * delta + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете amount_in (zero_for_one)");
                "Переполнение при расчете amount_in (zero_for_one)".to_string()
            })?;
        let amount_out = (liquidity * delta)
            .checked_div(two_pow_96)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете amount_out (zero_for_one)");
                "Переполнение при расчете amount_out (zero_for_one)".to_string()
            })?;
        (amount_in, amount_out)
    } else {
        let amount_in = (liquidity * delta * two_pow_96 + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете amount_in (!zero_for_one)");
                "Переполнение при расчете amount_in (!zero_for_one)".to_string()
            })?;
        let amount_out = (liquidity * delta)
            .checked_div(used_sqrt_price)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете amount_out (!zero_for_one)");
                "Переполнение при расчете amount_out (!zero_for_one)".to_string()
            })?;
        (amount_in, amount_out)
    };
    debug!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] amount_in = {}, amount_out = {}", 
        amount_in, amount_out
    );

    Ok((next_sqrt_price, amount_in, amount_out, fee_amount))
}

/// Симулирует своп токенов в пуле Uniswap, перебирая все тики
pub fn simulate_swap_tick_by_tick(
    pool: &UniswapPool,
    amount_in: U256,
    zero_for_one: bool,
) -> Result<(U256, U256), String> {
    info!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Начало симуляции свопа по тикам для пула: {:?}", 
        pool.uniswap_pool_address
    );
    let mut sqrt_price_x96 = pool.uniswap_sqrt_price;
    let mut liquidity = pool.uniswap_liquidity;
    let mut amount_out = U256::zero();
    let fee_pips = pool.uniswap_fee_tier;
    let mut remaining_amount_in = amount_in;
    let start_tick = pool.uniswap_tick_current;

    let tick_iter: Box<dyn Iterator<Item = (&i32, &(i128, U256))>> = if zero_for_one {
        Box::new(pool.tick_map.range(..=start_tick).rev())
    } else {
        Box::new(pool.tick_map.range(start_tick..))
    };

    for (next_tick_idx, (net_liquidity, _)) in tick_iter {
        let target_sqrt_price = tick_to_sqrt_price(*next_tick_idx)?;
        debug!(
            "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Обработка тика {}, target_sqrt_price = {}", 
            next_tick_idx, target_sqrt_price
        );

        let reached_target = if zero_for_one {
            sqrt_price_x96 > target_sqrt_price
        } else {
            sqrt_price_x96 < target_sqrt_price
        };
        if !reached_target {
            debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Не достигнута целевая цена, завершаем итерацию");
            break;
        }

        while remaining_amount_in > U256::zero() && reached_target {
            let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
                compute_swap_step(
                    sqrt_price_x96,
                    target_sqrt_price,
                    liquidity,
                    remaining_amount_in,
                    fee_pips,
                    zero_for_one,
                )?;

            if zero_for_one {
                if *net_liquidity >= 0 {
                    liquidity = liquidity
                        .checked_sub(U256::try_from(*net_liquidity).map_err(|_| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Ошибка преобразования net_liquidity {}", net_liquidity);
                            format!("Ошибка преобразования net_liquidity {}", net_liquidity)
                        })?)
                        .ok_or_else(|| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при вычитании ликвидности");
                            "Переполнение при вычитании ликвидности".to_string()
                        })?;
                } else {
                    liquidity = liquidity
                        .checked_add(U256::try_from(net_liquidity.abs()).map_err(|_| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Ошибка преобразования net_liquidity {}", net_liquidity);
                            format!("Ошибка преобразования net_liquidity {}", net_liquidity)
                        })?)
                        .ok_or_else(|| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при добавлении ликвидности");
                            "Переполнение при добавлении ликвидности".to_string()
                        })?;
                }
            } else {
                if *net_liquidity >= 0 {
                    liquidity = liquidity
                        .checked_add(U256::try_from(*net_liquidity).map_err(|_| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Ошибка преобразования net_liquidity {}", net_liquidity);
                            format!("Ошибка преобразования net_liquidity {}", net_liquidity)
                        })?)
                        .ok_or_else(|| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при добавлении ликвидности");
                            "Переполнение при добавлении ликвидности".to_string()
                        })?;
                } else {
                    liquidity = liquidity
                        .checked_sub(U256::try_from(net_liquidity.abs()).map_err(|_| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Ошибка преобразования net_liquidity {}", net_liquidity);
                            format!("Ошибка преобразования net_liquidity {}", net_liquidity)
                        })?)
                        .ok_or_else(|| {
                            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при вычитании ликвидности");
                            "Переполнение при вычитании ликвидности".to_string()
                        })?;
                }
            }
            debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Обновленная ликвидность = {}", liquidity);

            sqrt_price_x96 = next_sqrt_price_x96;
            amount_out = amount_out
                .checked_add(produced_amount_out)
                .ok_or_else(|| {
                    warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при накоплении amount_out");
                    "Переполнение при накоплении amount_out".to_string()
                })?;
            let total_used = used_amount_in
                .checked_add(fee_amount)
                .ok_or_else(|| {
                    warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при сложении used_amount_in и fee_amount");
                    "Переполнение при сложении used_amount_in и fee_amount".to_string()
                })?;
            remaining_amount_in = remaining_amount_in
                .checked_sub(total_used)
                .ok_or_else(|| {
                    warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Недостаточная remaining_amount_in");
                    "Недостаточная remaining_amount_in".to_string()
                })?;
            debug!(
                "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] next_sqrt_price_x96 = {}, used_amount_in = {}, produced_amount_out = {}, fee_amount = {}, remaining_amount_in = {}", 
                next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount, remaining_amount_in
            );

            if sqrt_price_x96 == target_sqrt_price {
                debug!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Достигнута целевая цена, завершаем итерацию в тике");
                break;
            }
        }
    }

    if remaining_amount_in > U256::zero() {
        let target_price = if zero_for_one { U256::one() } else { U256::MAX };
        debug!(
            "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Обрабатываем остаток remaining_amount_in = {}, target_price = {}", 
            remaining_amount_in, target_price
        );
        let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
            compute_swap_step(
                sqrt_price_x96,
                target_price,
                liquidity,
                remaining_amount_in,
                fee_pips,
                zero_for_one,
            )?;

        sqrt_price_x96 = next_sqrt_price_x96;
        amount_out = amount_out
            .checked_add(produced_amount_out)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при накоплении amount_out (остаток)");
                "Переполнение при накоплении amount_out (остаток)".to_string()
            })?;
        let total_used = used_amount_in
            .checked_add(fee_amount)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при сложении used_amount_in и fee_amount (остаток)");
                "Переполнение при сложении used_amount_in и fee_amount (остаток)".to_string()
            })?;
        remaining_amount_in = remaining_amount_in
            .checked_sub(total_used)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Недостаточная remaining_amount_in (остаток)");
                "Недостаточная remaining_amount_in (остаток)".to_string()
            })?;
        debug!(
            "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Остаток: next_sqrt_price_x96 = {}, used_amount_in = {}, produced_amount_out = {}, fee_amount = {}, remaining_amount_in = {}", 
            next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount, remaining_amount_in
        );
    }

    info!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Завершение симуляции свопа: amount_out = {}, final_sqrt_price = {}", 
        amount_out, sqrt_price_x96
    );
    Ok((amount_out, sqrt_price_x96))
}

/// Симулирует своп токенов по заданному пути пулов Uniswap
pub fn simulate_path_swap(
    pool_path: &[UniswapPool],
    start_amount: U256,
    start_token: Address,
    aave_liquidity: &AaveTokenLiquidity,
) -> Result<Option<(U256, Vec<U256>)>, String> {
    info!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Начало симуляции свопа по пути. start_amount: {}, start_token: {:?}", 
        start_amount, start_token
    );

    let available_liquidity = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(_, liquidity)| *liquidity)
        .unwrap_or(U256::zero());
    debug!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Доступная ликвидность Aave: {}", 
        available_liquidity
    );
    if available_liquidity.is_zero() {
        info!(
            "[{}] Нет ликвидности для токена {:?}", 
            "ARB SKIP".black().on_red(), start_token
        );
        return Ok(None);
    }

    let start_amount = start_amount.min(available_liquidity);
    if start_amount.is_zero() {
        info!(
            "[{}] Нулевая сумма для токена {:?}", 
            "ARB SKIP".black().on_red(), start_token
        );
        return Ok(None);
    }

    let min_profit_threshold = match MIN_PROFIT_THRESHOLD_BY_TOKEN.get(&start_token) {
        Some(&threshold) => threshold,
        None => {
            info!(
                "[{}] Токен {:?} не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", 
                "ARB SKIP".black().on_red(), start_token
            );
            return Ok(None);
        }
    };
    debug!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Минимальный порог прибыли: {}", 
        min_profit_threshold
    );

    if pool_path.is_empty() {
        warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Путь пуст");
        return Err("Путь пуст".to_string());
    }

    let mut current_amount = start_amount;
    let mut current_token = start_token;
    let mut intermediate_outputs = Vec::with_capacity(pool_path.len());

    for (i, pool) in pool_path.iter().enumerate() {
        let zero_for_one = current_token == pool.uniswap_token_a;
        debug!(
            "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Пул {}: zero_for_one = {}, token_in = {:?}, token_out = {:?}", 
            i, zero_for_one, current_token, 
            if zero_for_one { pool.uniswap_token_b } else { pool.uniswap_token_a }
        );
        let (amount_out, next_sqrt_price) = simulate_swap_tick_by_tick(pool, current_amount, zero_for_one)?;
        debug!(
            "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Пул {}: amount_out = {}, next_sqrt_price = {}", 
            i, amount_out, next_sqrt_price
        );
        intermediate_outputs.push(amount_out);
        current_amount = amount_out;
        current_token = if zero_for_one {
            pool.uniswap_token_b
        } else {
            pool.uniswap_token_a
        };
    }

    if current_token != start_token {
        info!(
            "[{}] Некорректный путь (длина: {}): конечный токен {:?} != стартовый {:?}", 
            "ARB SKIP".black().on_red(), pool_path.len(), current_token, start_token
        );
        return Ok(None);
    }

    let final_amount_out = current_amount;
    let aave_fee = start_amount
        .checked_mul(U256::from(9))
        .and_then(|x| x.checked_div(U256::from(10000)))
        .ok_or_else(|| {
            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при расчете aave_fee");
            "Переполнение при расчете aave_fee".to_string()
        })?;
    let total_threshold = min_profit_threshold
        .checked_add(aave_fee)
        .ok_or_else(|| {
            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при вычислении total_threshold");
            "Переполнение при вычислении total_threshold".to_string()
        })?;
    let profit_threshold = start_amount
        .checked_add(total_threshold)
        .ok_or_else(|| {
            warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при вычислении profit_threshold");
            "Переполнение при вычислении profit_threshold".to_string()
        })?;
    debug!(
        "[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] final_amount_out = {}, aave_fee = {}, total_threshold = {}, profit_threshold = {}", 
        final_amount_out, aave_fee, total_threshold, profit_threshold
    );

    if final_amount_out > profit_threshold {
        let profit = final_amount_out
            .checked_sub(start_amount)
            .ok_or_else(|| {
                warn!("[UNISWAP_UNISWAP_ARB_SCANNER_DEBUG] Переполнение при вычислении profit");
                "Переполнение при вычислении profit".to_string()
            })?;
        info!(
            "[{}] Прибыль: {} {:?} для пути: {}", 
            "ARB SUCCESS".red(),
            profit,
            start_token,
            format!("{:?}", pool_path.iter().map(|p| p.uniswap_pool_address).collect::<Vec<_>>()).red()
        );
        Ok(Some((final_amount_out, intermediate_outputs)))
    } else {
        info!(
            "[{}] Прибыль ниже порога: {} <= {} для токена {:?}", 
            "ARB SKIP".black().on_red(), final_amount_out, profit_threshold, start_token
        );
        Ok(None)
    }
}
*/



