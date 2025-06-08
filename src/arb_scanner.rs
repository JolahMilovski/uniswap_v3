use crate::aave_v3_flash_monitor::AaveTokenLiquidity;
use crate::uniswap_graph::UniswapPool;
use crate::uniswap_v3::tick_to_sqrt_price;

use colored::Colorize;
use ethers::types::{Address, U256, U512};

use ethers::utils::hex;
use log::info;
use lazy_static::lazy_static;
use std::collections::HashMap;


lazy_static! {
    static ref MIN_PROFIT_THRESHOLD_BY_TOKEN: HashMap<Address, U256> = {
        let mut m = HashMap::new();

        // MAI: 100 MAI = 100 * 10^18
        m.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128),);
        // USDC (Arbitrum): 100 USDC = 100 * 10^6

        m.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));

        // USDC (Bridged): 100 USDC = 100 * 10^6
        m.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));

        // ARB: 0.03 ARB = 0.03 * 10^18
        m.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(30_000_000_000_000_000u128));

        // ezETH: 0.01 ezETH = 0.01 * 10^18
        m.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(10_000_000_000_000_000u128));

        // WETH: 0.03 WETH = 0.03 * 10^18
        m.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(30_000_000_000_000_000u128));
      
        // USDT: 100 USDT = 100 * 10^6
        m.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));

        // WBTC: 0.03 WBTC = 0.03 * 10^8
        m.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(3_000_000));

        // LUSD: 100 LUSD = 100 * 10^18
        m.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // weETH: 0.01 weETH = 0.01 * 10^18
        m.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(10_000_000_000_000_000u128));

        // LINK: 0.03 LINK = 0.03 * 10^18
        m.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(30_000_000_000_000_000u128));

        // wstETH: 0.01 wstETH = 0.01 * 10^18
        m.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(10_000_000_000_000_000u128));

        // rsETH: 0.01 rsETH = 0.01 * 10^18
        m.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(10_000_000_000_000_000u128));

        // GHO: 100 GHO = 100 * 10^18
        m.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // FRAX: 100 FRAX = 100 * 10^18
        m.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // EURS: 100 EURS = 100 * 10^2
        m.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));

        // DAI: 100 DAI = 100 * 10^18
        m.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // rETH: 0.01 rETH = 0.01 * 10^18
        m.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(10_000_000_000_000_000u128));
        
        // AAVE: 0.03 AAVE = 0.03 * 10^18
        m.insert(Address::from_slice(&hex::decode("ba5ddd1f9d7f570dc94a51479a000e3bce967196").unwrap()), U256::from(30_000_000_000_000_000u128));
        m
    };
}

/// Функция вычисляет следующую цену sqrt после ввода определенного количества токенов
/// 
/// # Аргументы
/// * `sqrt_price_x96` - Текущая цена в формате Q64.96 (квадратный корень из соотношения цен)
/// * `liquidity` - Текущая ликвидность в пуле
/// * `amount_in` - Количество входящих токенов
/// * `zero_for_one` - Направление свопа (true если token0 -> token1, false если token1 -> token0)
/// 
/// # Возвращаемое значение
/// * Новая цена sqrt в формате Q64.96 после свопа
pub fn get_next_sqrt_price_from_input(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_in: U256,
    zero_for_one: bool,
) -> U256 {
    if zero_for_one {
        // Для свопа token0 -> token1:
        // Вычисляем числитель (ликвидность * 2^96)
        let numerator = liquidity << 96;
        // Вычисляем произведение входящего количества на текущую цену
        let product = amount_in.checked_mul(sqrt_price_x96).unwrap();
        // Знаменатель - сумма числителя и произведения
        let denominator = numerator.checked_add(product).unwrap();
        // Итоговая формула: (L * sqrt(P)) / (L + amount_in * sqrt(P))
        numerator.checked_mul(sqrt_price_x96).unwrap() / denominator
    } else {
        // Для свопа token1 -> token0:
        // Вычисляем произведение входящего количества на 2^96
        let product = amount_in.checked_mul(U256::from(1u128 << 96)).unwrap();
        // Итоговая формула: sqrt(P) + (amount_in * 2^96) / L
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap())
            .unwrap()
    }
}


/// Функция вычисляет следующую цену sqrt после вывода определенного количества токенов
/// 
/// # Аргументы
/// * `sqrt_price_x96` - Текущая цена в формате Q64.96 (квадратный корень из соотношения цен)
/// * `liquidity` - Текущая ликвидность в пуле
/// * `amount_out` - Количество выводимых токенов
/// * `zero_for_one` - Направление свопа (true если token0 -> token1, false если token1 -> token0)
/// 
/// # Возвращаемое значение
/// * Новая цена sqrt в формате Q64.96 после свопа
pub fn get_next_sqrt_price_from_output(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_out: U256,
    zero_for_one: bool,
) -> U256 {
    if zero_for_one {
        // Для свопа token0 -> token1:
        // Вычисляем произведение выходящего количества на 2^96
        let product = amount_out.checked_mul(U256::from(1u128 << 96)).unwrap();
        // Итоговая формула: sqrt(P) + (amount_out * 2^96) / L
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap())
            .unwrap()
    } else {
        // Для свопа token1 -> token0:
        // Вычисляем числитель (ликвидность * 2^96)
        let numerator = liquidity << 96;
        // Вычисляем произведение выходящего количества на текущую цену
        let product = amount_out.checked_mul(sqrt_price_x96).unwrap();
        // Знаменатель - разность числителя и произведения
        let denominator = numerator.checked_sub(product).unwrap();
        // Итоговая формула: (L * sqrt(P)) / (L - amount_out * sqrt(P))
        numerator.checked_mul(sqrt_price_x96).unwrap() / denominator
    }
}

/// Функция вычисляет параметры одного шага свопа в пуле Uniswap V3
/// 
/// # Аргументы
/// * `sqrt_price_x96` - Текущая цена в формате Q64.96 (квадратный корень из соотношения цен)
/// * `target_sqrt_price_x96` - Целевая цена в формате Q64.96, к которой стремимся
/// * `liquidity` - Текущая ликвидность в пуле
/// * `amount_remaining` - Оставшееся количество токенов для свопа
/// * `fee_pips` - Комиссия пула в пипсах (1 пипс = 0.0001%)
/// * `zero_for_one` - Направление свопа (true если token0 -> token1, false если token1 -> token0)
/// 
/// # Возвращаемое значение
/// Результат, содержащий кортеж из четырех значений:
/// * Новая цена sqrt после свопа
/// * Количество использованных входящих токенов
/// * Количество полученных выходящих токенов
/// * Размер комиссии
/// 
/// # Ошибки
/// Возвращает `Err` в случае арифметического переполнения или деления на ноль
pub fn compute_swap_step(
    sqrt_price_x96: U256,
    target_sqrt_price_x96: U256,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: u32,
    zero_for_one: bool,
) -> Result<(U256, U256, U256, U256), String> {
    // Если ликвидность нулевая, возвращаем исходную цену и нулевые значения
    if liquidity.is_zero() {
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    // Проверяем корректность направления движения цены
    if zero_for_one && target_sqrt_price_x96 > sqrt_price_x96 {
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }
    if !zero_for_one && target_sqrt_price_x96 < sqrt_price_x96 {
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    // Вычисляем комиссию
    let fee_denom = U256::from(1_000_000u32);
    let fee_pips_u256 = U256::from(fee_pips);
    let fee_amount = amount_remaining
        .checked_mul(fee_pips_u256)
        .ok_or("Fee multiplication overflow")?
        .checked_div(fee_denom)
        .ok_or("Fee division overflow")?;

    // Вычитаем комиссию из входящего количества
    let amount_remaining_less_fee = amount_remaining
        .checked_sub(fee_amount)
        .ok_or("Fee subtraction underflow")?;

    // Вычисляем следующую цену sqrt в зависимости от направления свопа
    let next_sqrt_price_x96 = if zero_for_one {
        // Для свопа token0 -> token1
        let numerator = liquidity << 96;
        let product = amount_remaining_less_fee
            .checked_mul(sqrt_price_x96)
            .ok_or("Multiplication overflow")?;
        let denominator = numerator
            .checked_add(product)
            .ok_or("Addition overflow")?;
        numerator
            .checked_mul(sqrt_price_x96)
            .ok_or("Multiplication overflow")?
            .checked_div(denominator)
            .ok_or("Division overflow")?
    } else {
        // Для свопа token1 -> token0
        let product = amount_remaining_less_fee
            .checked_mul(U256::from(1u128 << 96))
            .ok_or("Multiplication overflow")?;
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).ok_or("Division overflow")?)
            .ok_or("Addition overflow")?
    };

    // Проверяем, достигли ли мы целевой цены
    let reached_target = if zero_for_one {
        next_sqrt_price_x96 <= target_sqrt_price_x96
    } else {
        next_sqrt_price_x96 >= target_sqrt_price_x96
    };

    // Определяем итоговую цену и дельту изменения
    let (used_sqrt_price, delta) = if reached_target {
        (target_sqrt_price_x96, sqrt_price_x96.abs_diff(target_sqrt_price_x96))
    } else {
        (next_sqrt_price_x96, sqrt_price_x96.abs_diff(next_sqrt_price_x96))
    };

    // Константа для сдвига на 96 бит (деление на 2^96)
    let two_pow_96 = U256::from(1u128 << 96);

    // Вычисляем количество входящих и выходящих токенов в зависимости от направления свопа
    let (amount_in, amount_out) = if zero_for_one {
        // Для свопа token0 -> token1
        let amount_in = (liquidity * delta + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or("Division overflow")?;
        let amount_out = (liquidity * delta)
            .checked_div(two_pow_96)
            .ok_or("Division by 2^96 overflow")?;
        (amount_in, amount_out)
    } else {
        // Для свопа token1 -> token0
        let amount_in = (liquidity * delta * two_pow_96 + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or("Division overflow")?;
        let amount_out = (liquidity * delta)
            .checked_div(used_sqrt_price)
            .ok_or("Division overflow")?;
        (amount_in, amount_out)
    };

    // Возвращаем результаты вычислений
    Ok((next_sqrt_price_x96, amount_in, amount_out, fee_amount))
}



/// Симулирует своп в пуле Uniswap V3, проходя по тикам и обновляя ликвидность.
/// 
/// # Аргументы
/// * `pool` - Пул Uniswap V3, содержащий данные о ликвидности, цене и тиках.
/// * `amount_in` - Количество входящих токенов для свопа.
/// * `zero_for_one` - Направление свопа: `true` для token0 -> token1, `false` для token1 -> token0.
/// 
/// # Возвращаемое значение
/// Возвращает `Result` с кортежем:
/// * `amount_out` - Количество выходящих токенов после свопа.
/// * `next_sqrt_price_x96` - Новая цена в формате Q64.96 после свопа.
/// 
/// # Ошибки
/// Возвращает `Err(String)` при:
/// * Ошибках преобразования типов (U512 -> U256, i128 -> U256).
/// * Арифметических переполнениях или недостатке ликвидности.
/// * Ошибках вычислений в `compute_swap_step`.
pub fn simulate_swap_tick_by_tick(
    pool: &UniswapPool,
    amount_in: U256,
    zero_for_one: bool,
) -> Result<(U256, U256), String> {
    // Инициализация текущей цены и ликвидности из пула
    let mut sqrt_price_x96 = U256::try_from(pool.uniswap_sqrt_price)
        .map_err(|_| "U512 to U256 conversion failed")?;
    let mut liquidity = U256::try_from(pool.uniswap_liquidity)
        .map_err(|_| "U512 to U256 conversion failed")?;
    let mut amount_out = U256::zero();
    let fee_pips = pool.uniswap_fee_tier;
    let mut remaining_amount_in = amount_in;

    // Определяем итератор по тикам в зависимости от направления свопа
    let tick_iter: Box<dyn Iterator<Item = (&i32, &(i128, U512))>> = if zero_for_one {
        // Для token0 -> token1: цена уменьшается, идем по тикам в порядке убывания
        Box::new(pool.tick_map.iter().rev())
    } else {
        // Для token1 -> token0: цена увеличивается, идем по тикам в порядке возрастания
        Box::new(pool.tick_map.iter())
    };

    // Итерируем по тикам, пока есть входящее количество токенов
    for (next_tick_idx, (net_liquidity, _)) in tick_iter {
        // Вычисляем целевую цену для следующего тика
        let target_sqrt_price = U256::try_from(tick_to_sqrt_price(*next_tick_idx)?)
            .map_err(|_| "U512 to U256 conversion failed")?;

        // Проверяем, достигли ли мы целевой тик
        let reached_target = if zero_for_one {
            sqrt_price_x96 > target_sqrt_price
        } else {
            sqrt_price_x96 < target_sqrt_price
        };

        if !reached_target {
            break; // Если целевой тик недостижим, выходим из цикла
        }

        // Выполняем шаг свопа до следующего тика или пока не израсходуем amount_in
        while remaining_amount_in > U256::zero() && reached_target {
            let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
                compute_swap_step(
                    sqrt_price_x96,
                    target_sqrt_price,
                    liquidity,
                    remaining_amount_in,
                    fee_pips,
                    zero_for_one,
                ).map_err(|e| format!("Swap step computation failed: {}", e))?;

            // Обновляем ликвидность на основе net_liquidity
            if zero_for_one {
                if *net_liquidity >= 0 {
                    liquidity = liquidity
                        .checked_sub(
                            U256::try_from(*net_liquidity)
                                .map_err(|_| "i128 to U256 conversion failed")?,
                        )
                        .ok_or("Liquidity underflow")?;
                } else {
                    liquidity = liquidity
                        .checked_add(
                            U256::try_from(net_liquidity.abs())
                                .map_err(|_| "i128 to U256 conversion failed")?,
                        )
                        .ok_or("Liquidity overflow")?;
                }
            } else {
                if *net_liquidity >= 0 {
                    liquidity = liquidity
                        .checked_add(
                            U256::try_from(*net_liquidity)
                                .map_err(|_| "i128 to U256 conversion failed")?,
                        )
                        .ok_or("Liquidity overflow")?;
                } else {
                    liquidity = liquidity
                        .checked_sub(
                            U256::try_from(net_liquidity.abs())
                                .map_err(|_| "i128 to U256 conversion failed")?,
                        )
                        .ok_or("Liquidity underflow")?;
                }
            }

            // Обновляем состояние
            sqrt_price_x96 = next_sqrt_price_x96;
            amount_out = amount_out
                .checked_add(produced_amount_out)
                .ok_or("Output overflow")?;
            let total_used = used_amount_in
                .checked_add(fee_amount)
                .ok_or("Arithmetic overflow")?;
            remaining_amount_in = remaining_amount_in
                .checked_sub(total_used)
                .ok_or("Insufficient input amount")?;

            // Проверяем, достигли ли мы целевой цены
            if sqrt_price_x96 == target_sqrt_price {
                break; // Переходим к следующему тику
            }
        }
    }

if remaining_amount_in > U256::zero() {
    let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
        compute_swap_step(
            sqrt_price_x96,
            if zero_for_one {
                U256::zero() // Минимальная цена
            } else {
                U256::MAX // Максимальная цена
            },
            liquidity,
            remaining_amount_in,
            fee_pips,
            zero_for_one,
        ).map_err(|e| format!("Swap step computation failed: {}", e))?;

    // Обновляем финальное состояние
    sqrt_price_x96 = next_sqrt_price_x96;
    amount_out = amount_out
        .checked_add(produced_amount_out)
        .ok_or("Output overflow")?;
    let total_used = used_amount_in
        .checked_add(fee_amount)
        .ok_or("Arithmetic overflow")?;
    // Проверяем вычитание без присваивания
    remaining_amount_in
        .checked_sub(total_used)
        .ok_or("Insufficient input amount")?;
}

Ok((amount_out, sqrt_price_x96))
}


/// Симулирует выполнение свопа по заданному пути через пулы Uniswap
/// # Аргументы
/// * `pool_path` - Массив пулов Uniswap, через которые будет выполняться своп
/// * `start_amount` - Начальное количество токенов для свопа
/// * `min_profit_threshold` - Минимальный порог прибыли, который должен быть достигнут
/// * `start_token` - Адрес начального токена
/// # Возвращаемое значение
/// * `Ok(Some((final_amount, outputs)))` - Если своп успешен и превышает порог прибыли:
///   - final_amount: итоговое количество полученных токенов
///   - outputs: промежуточные результаты по каждому пулу
/// * `Ok(None)` - Если своп возможен, но не превышает порог прибыли
/// * `Err(String)` - В случае ошибки
pub fn simulate_path_swap(
    pool_path: &[UniswapPool],
    start_amount: U256,
    start_token: Address,
    aave_liquidity: &AaveTokenLiquidity,
) -> Result<Option<(U256, Vec<U256>)>, String> {
    let available_liquidity = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(_, liquidity)| *liquidity)
        .unwrap_or(U256::zero());

    if available_liquidity.is_zero() {
        info!("[{}] Нет ликвидности для токена {:?}", "ARB SKIP".black().on_red(), start_token);
        return Ok(None);
    }

    let max_start_amount = available_liquidity;

    let start_amount = start_amount.min(max_start_amount);

    if start_amount.is_zero() {
        info!("[{}] Нулевая сумма для токена {:?}", "ARB SKIP".black().on_red(), start_token);
        return Ok(None);
    }

    let min_profit_threshold = match MIN_PROFIT_THRESHOLD_BY_TOKEN.get(&start_token) {
        Some(&threshold) => threshold,
        None => {
            info!("[{}] Токен {:?} не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", "ARB SKIP".black().on_red(), start_token);
            return Ok(None);
        }
    };

    if pool_path.is_empty() {
        return Err("Path is empty".to_string());
    }

    let mut current_amount = start_amount;
    let mut current_token = start_token;
    let mut intermediate_outputs = Vec::with_capacity(pool_path.len());

    for pool in pool_path.iter() {
        let zero_for_one = current_token == pool.uniswap_token_a;
        let (amount_out, _next_sqrt_price) = simulate_swap_tick_by_tick(pool, current_amount, zero_for_one)?;
        intermediate_outputs.push(amount_out);
        current_amount = amount_out;
        current_token = if zero_for_one {
            pool.uniswap_token_b
        } else {
            pool.uniswap_token_a
        };
    }

    if current_token != start_token {
        info!("[{}] Некорректный путь: конечный токен {:?} != стартовый {:?}", "ARB SKIP".black().on_red(), current_token, start_token);
        return Ok(None);
    }

    let final_amount_out = current_amount;

    let aave_fee = start_amount * U256::from(9) / U256::from(10000); // 0.09%
    let total_threshold = min_profit_threshold
        .checked_add(aave_fee)
        .ok_or("Arithmetic overflow in Aave fee")?;

    if final_amount_out > start_amount.checked_add(total_threshold).ok_or("Arithmetic overflow")? {
        info!(
            "[{}] Profit: {} for path: {}", 
            "ARB SUCCESS".red(),
            final_amount_out.checked_sub(start_amount).ok_or("Arithmetic underflow")?,
            format!("{:?}", pool_path.iter().map(|p| p.uniswap_pool_address).collect::<Vec<_>>()).red()
        );
        Ok(Some((final_amount_out, intermediate_outputs)))
    } else {
        Ok(None)
    }
}

