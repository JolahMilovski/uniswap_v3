use crate::aave_v3_flash_monitor::AaveTokenLiquidity;
use crate::uniswap_graph::UniswapPool;
use crate::uniswap_v3::tick_to_sqrt_price;

use colored::Colorize as _;
use ethers::types::{Address, U256};

use ethers::utils::hex;
use tracing::info;
use lazy_static::lazy_static;
use std::collections::HashMap;


lazy_static! {
    pub static ref MIN_PROFIT_THRESHOLD_BY_TOKEN: HashMap<Address, U256> = {
        let mut min_profit_by_token = HashMap::new();
                // MAI: ~1 USD
        min_profit_by_token.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // USDC (Arbitrum): 100 USDC = 100 * 10^6
        min_profit_by_token.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));
        // USDC (Bridged): 100 USDC = 100 * 10^6
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));

        // ARB: ~$0.29 → 345 ARB = 345 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(345_000_000_000_000_000_000u128));

        // ezETH: ~$2644 → 0.038 ezETH = 0.038 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(38_000_000_000_000_000u128));

        // WETH: ~$2411 → 0.042 WETH = 0.042 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(42_000_000_000_000_000u128));   

        // USDT: 100 USDT = 100 * 10^6
        min_profit_by_token.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));

        // WBTC: ~$103_000 → 0.001 WBTC = 0.001 * 10^8
        min_profit_by_token.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(100_000u128));

        // LUSD: 100 LUSD = 100 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // weETH: ~$2697 → 0.037 weETH = 0.037 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(37_000_000_000_000_000u128));

        // LINK: ~$12.5 → 8 LINK = 8 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(8_000_000_000_000_000_000u128));

        // wstETH: ~$3040 → 0.033 wstETH = 0.033 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(33_000_000_000_000_000u128));

        // rsETH: ~$2650 → 0.038 rsETH = 0.038 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(38_000_000_000_000_000u128));

        // GHO: 100 GHO = 100 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // FRAX: 100 FRAX = 100 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // EURS: ~$1.07 → 100 EURS = 100 * 10^2 (2 decimals)
        min_profit_by_token.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));

        // DAI: 100 DAI = 100 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));

        // rETH: ~$2644 → 0.038 rETH = 0.038 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(38_000_000_000_000_000u128));      

        // AAVE: ~$246 → 1 AAVE = 1 * 10^18
        min_profit_by_token.insert(Address::from_slice(&hex::decode("ba5ddd1f9d7f570dc94a51479a000e3bce967196").unwrap()), U256::from(1_000_000_000_000_000_000u128));

        min_profit_by_token
    };
}



/// Вычисляет следующую цену sqrt после добавления входящего объема токенов в пул Uniswap V3
///
/// # Аргументы
///
/// * `sqrt_price_x96` - Текущая цена в формате sqrt price Q64.96
/// * `liquidity` - Текущая ликвидность в пуле
/// * `amount_in` - Объем входящих токенов
/// * `zero_for_one` - Направление свопа (true для token0 -> token1, false для token1 -> token0)
///
/// # Возвращаемое значение
///
/// Возвращает новую цену sqrt в формате Q64.96 после выполнения свопа
///
/// # Детали реализации
///
/// Для zero_for_one (token0 -> token1):
/// 1. Вычисляет числитель как liquidity << 96
/// 2. Находит произведение входящего объема и текущей цены
/// 3. Вычисляет знаменатель как сумму числителя и произведения
/// 4. Итоговая цена = (числитель * текущая цена) / знаменатель
///
/// Для !zero_for_one (token1 -> token0):
/// 1. Вычисляет произведение входящего объема и 2^96
/// 2. Итоговая цена = текущая цена + (произведение / ликвидность)
pub fn get_next_sqrt_price_from_input(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_in: U256,
    zero_for_one: bool,
) -> U256 {
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление следующей sqrt цены. zero_for_one: {}, sqrt_price_x96: {}, liquidity: {}, amount_in: {}", zero_for_one, sqrt_price_x96, liquidity, amount_in);
    if zero_for_one {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Направление свопа token0 -> token1");
        let numerator = liquidity << 96;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Числитель: {}", numerator);
        let product = amount_in.checked_mul(sqrt_price_x96).unwrap_or(U256::MAX);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Произведение: {}", product);
        let denominator = numerator.checked_add(product).unwrap_or(U256::MAX);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Знаменатель: {}", denominator);
        let result = numerator.checked_mul(sqrt_price_x96).unwrap_or(U256::MAX)
            .checked_div(denominator).unwrap_or(U256::zero());
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Результат для zero_for_one: {}", result);
        result
    } else {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Направление свопа token1 -> token0");
        let product = amount_in.checked_mul(U256::from(1u128 << 96)).unwrap_or(U256::MAX);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Произведение: {}", product);
        let result = sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap_or(U256::zero()))
            .unwrap_or(U256::MAX);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Результат для !zero_for_one: {}", result);
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
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начало шага свопа. sqrt_price_x96: {}, target_sqrt_price_x96: {}, liquidity: {}, amount_remaining: {}, fee_pips: {}, zero_for_one: {}", 
        sqrt_price_x96, target_sqrt_price_x96, liquidity, amount_remaining, fee_pips, zero_for_one);

    if liquidity.is_zero() {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Ликвидность равна нулю, возвращаем нулевые значения");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    if zero_for_one && target_sqrt_price_x96 > sqrt_price_x96 {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Некорректное направление цены для zero_for_one, возвращаем нулевые значения");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }
    if !zero_for_one && target_sqrt_price_x96 < sqrt_price_x96 {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Некорректное направление цены для !zero_for_one, возвращаем нулевые значения");
        return Ok((sqrt_price_x96, U256::zero(), U256::zero(), U256::zero()));
    }

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление комиссии за своп");
    let fee_denom = U256::from(1_000_000u32);
    let fee_pips_u256 = U256::from(fee_pips);
    let fee_amount = amount_remaining
        .checked_mul(fee_pips_u256)
        .ok_or("Переполнение при умножении комиссии")?
        .checked_div(fee_denom)
        .ok_or("Переполнение при делении комиссии")?;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Сумма комиссии: {}", fee_amount);

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычитание комиссии из оставшейся суммы");
    let amount_remaining_less_fee = amount_remaining
        .checked_sub(fee_amount)
        .ok_or("Вычитание комиссии привело к отрицательному значению")?;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Оставшаяся сумма без комиссии: {}", amount_remaining_less_fee);

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление следующей sqrt цены");
    let next_sqrt_price_x96 = if zero_for_one {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление для zero_for_one");
        let numerator = liquidity << 96;
        let product = amount_remaining_less_fee
            .checked_mul(sqrt_price_x96)
            .ok_or("Переполнение при умножении")?;
        let denominator = numerator
            .checked_add(product)
            .ok_or("Переполнение при сложении")?;
        let result = numerator
            .checked_mul(sqrt_price_x96)
            .ok_or("Переполнение при умножении")?
            .checked_div(denominator)
            .ok_or("Переполнение при делении")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Следующая sqrt цена: {}", result);
        result
    } else {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление для !zero_for_one");
        let product = amount_remaining_less_fee
            .checked_mul(U256::from(1u128 << 96))
            .ok_or("Переполнение при умножении")?;
        let result = sqrt_price_x96
            .checked_add(product.checked_div(liquidity).ok_or("Переполнение при делении")?)
            .ok_or("Переполнение при сложении")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Следующая sqrt цена: {}", result);
        result
    };

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Проверка достижения целевой цены");
    let reached_target = if zero_for_one {
        next_sqrt_price_x96 <= target_sqrt_price_x96
    } else {
        next_sqrt_price_x96 >= target_sqrt_price_x96
    };
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Целевая цена достигнута: {}", reached_target);

    let (used_sqrt_price, delta) = if reached_target {
        (target_sqrt_price_x96, sqrt_price_x96.abs_diff(target_sqrt_price_x96))
    } else {
        (next_sqrt_price_x96, sqrt_price_x96.abs_diff(next_sqrt_price_x96))
    };
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Использованная sqrt цена: {}, Дельта: {}", used_sqrt_price, delta);

    let two_pow_96 = U256::from(1u128 << 96);
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] two_pow_96: {}", two_pow_96);

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление входных и выходных сумм");
    let (amount_in, amount_out) = if zero_for_one {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление для zero_for_one");
        let amount_in = (liquidity * delta + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or("Переполнение при делении")?;
        let amount_out = (liquidity * delta)
            .checked_div(two_pow_96)
            .ok_or("Переполнение при делении на 2^96")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Входная сумма: {}, Выходная сумма: {}", amount_in, amount_out);
        (amount_in, amount_out)
    } else {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление для !zero_for_one");
        let amount_in = (liquidity * delta * two_pow_96 + (used_sqrt_price - U256::one()))
            .checked_div(used_sqrt_price)
            .ok_or("Переполнение при делении")?;
        let amount_out = (liquidity * delta)
            .checked_div(used_sqrt_price)
            .ok_or("Переполнение при делении")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Входная сумма: {}, Выходная сумма: {}", amount_in, amount_out);
        (amount_in, amount_out)
    };

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Шаг свопа успешно завершен");
    Ok((next_sqrt_price_x96, amount_in, amount_out, fee_amount))
}



/// Симулирует своп токенов в пуле Uniswap, перебирая все тики
///
/// # Аргументы
/// * `pool` - Пул Uniswap, в котором выполняется своп
/// * `amount_in` - Входная сумма токенов для свопа
/// * `zero_for_one` - Направление свопа (true для token0 -> token1, false для token1 -> token0)
///
/// # Возвращаемое значение
/// * `Ok((U256, U256))` - Кортеж, содержащий:
///   - Выходную сумму токенов после свопа
///   - Конечную sqrt цену пула (sqrt_price_x96)
/// * `Err(String)` - Ошибка при выполнении свопа
///
/// # Алгоритм работы
/// 1. Инициализирует начальные параметры пула (sqrt цена, ликвидность)
/// 2. Создает итератор по тикам в зависимости от направления свопа
/// 3. Для каждого тика:
///    - Вычисляет целевую sqrt цену для текущего тика
///    - Проверяет достижение целевой цены
///    - Выполняет свопы до достижения целевой цены или исчерпания входной суммы
///    - Обновляет ликвидность пула на основе net_liquidity тика
///    - Обновляет параметры свопа (sqrt цена, выходная сумма, оставшаяся входная сумма)
/// 4. Обрабатывает оставшуюся входную сумму, если она есть
/// 5. Возвращает финальную выходную сумму и sqrt цену
///
/// # Особенности
/// - Учитывает комиссию пула (fee_pips)
/// - Поддерживает оба направления свопа
/// - Симулирует изменение ликвидности на каждом тике
/// - Предотвращает арифметические переполнения
pub fn simulate_swap_tick_by_tick(
    pool: &UniswapPool,
    amount_in: U256,
    zero_for_one: bool,
) -> Result<(U256, U256), String> {
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начало симуляции свопа по тикам для пула: {:?}", pool.uniswap_pool_address);
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Входная сумма: {}, zero_for_one: {}", amount_in, zero_for_one);

    let mut sqrt_price_x96 = pool.uniswap_sqrt_price;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начальная sqrt_price_x96: {}", sqrt_price_x96);

    let mut liquidity = pool.uniswap_liquidity;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начальная ликвидность: {}", liquidity);

    let mut amount_out = U256::zero();
    let fee_pips = pool.uniswap_fee_tier;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Комиссия пула: {}", fee_pips);
    let mut remaining_amount_in = amount_in;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начальная оставшаяся входная сумма: {}", remaining_amount_in);

    let start_tick = pool.uniswap_tick_current;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG ] Начальный тик пула: {}", start_tick);
    let tick_iter: Box<dyn Iterator<Item = (&i32, &(i128, U256))>> = if zero_for_one {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG ] Итерация тиков в обратном порядке (zero_for_one)");
        Box::new(pool.tick_map.range(..=start_tick).rev())
    } else {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG ] Итерация тиков в нормальном порядке (!zero_for_one)");
        Box::new(pool.tick_map.range(start_tick..))
    };

    for (next_tick_idx, (net_liquidity, _)) in tick_iter {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обработка тика: {}, net_liquidity: {}", next_tick_idx, net_liquidity);

    let target_sqrt_price = tick_to_sqrt_price(*next_tick_idx)?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Целевая sqrt цена: {}", target_sqrt_price);

        let reached_target = if zero_for_one {
            sqrt_price_x96 > target_sqrt_price
        } else {
            sqrt_price_x96 < target_sqrt_price
        };
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Целевая цена достигнута: {}", reached_target);

        if !reached_target {
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Целевая цена не достигнута, прерываем цикл");
            break;
        }

        while remaining_amount_in > U256::zero() && reached_target {
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Оставшаяся входная сумма: {}, вычисление шага свопа", remaining_amount_in);
            let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
                compute_swap_step(
                    sqrt_price_x96,
                    target_sqrt_price,
                    liquidity,
                    remaining_amount_in,
                    fee_pips,
                    zero_for_one,
                ).map_err(|e| format!("Ошибка вычисления шага свопа: {}", e))?;
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Результат шага свопа - next_sqrt_price_x96: {}, used_amount_in: {}, produced_amount_out: {}, fee_amount: {}", 
                next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount);

            if zero_for_one {
                if *net_liquidity >= 0 {
                    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Уменьшение ликвидности на: {}", *net_liquidity);
                    liquidity = liquidity
                        .checked_sub(
                            U256::try_from(*net_liquidity)
                                .map_err(|_| "Ошибка преобразования i128 в U256")?,
                        )
                        .ok_or("Переполнение ликвидности")?;
                } else {
                    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Увеличение ликвидности на: {}", net_liquidity.abs());
                    liquidity = liquidity
                        .checked_add(
                            U256::try_from(net_liquidity.abs())
                                .map_err(|_| "Ошибка преобразования i128 в U256")?,
                        )
                        .ok_or("Переполнение ликвидности")?;
                }
            } else {
                if *net_liquidity >= 0 {
                    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Увеличение ликвидности на: {}", *net_liquidity);
                    liquidity = liquidity
                        .checked_add(
                            U256::try_from(*net_liquidity)
                                .map_err(|_| "Ошибка преобразования i128 в U256")?,
                        )
                        .ok_or("Переполнение ликвидности")?;
                } else {
                    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Уменьшение ликвидности на: {}", net_liquidity.abs());
                    liquidity = liquidity
                        .checked_sub(
                            U256::try_from(net_liquidity.abs())
                                .map_err(|_| "Ошибка преобразования i128 в U256")?,
                        )
                        .ok_or("Переполнение ликвидности")?;
                }
            }
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обновленная ликвидность: {}", liquidity);

            sqrt_price_x96 = next_sqrt_price_x96;
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обновленная sqrt_price_x96: {}", sqrt_price_x96);

            amount_out = amount_out
                .checked_add(produced_amount_out)
                .ok_or("Переполнение выходной суммы")?;
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обновленная выходная сумма: {}", amount_out);

            let total_used = used_amount_in
                .checked_add(fee_amount)
                .ok_or("Арифметическое переполнение")?;
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Общая использованная сумма: {}", total_used);

            remaining_amount_in = remaining_amount_in
                .checked_sub(total_used)
                .ok_or("Недостаточная входная сумма")?;
            info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обновленная оставшаяся входная сумма: {}", remaining_amount_in);

            if sqrt_price_x96 == target_sqrt_price {
                info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Целевая sqrt цена достигнута, прерываем внутренний цикл");
                break;
            }
        }
    }

    if remaining_amount_in > U256::zero() {
        info!("[SIMULATE_SWAP_TICK_BY_TICK] Обработка остатка: {}", remaining_amount_in);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обработка остатка: {}", remaining_amount_in);
        let target_price = if zero_for_one {
            U256::one()
        } else {
            U256::MAX
        };
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная целевая цена: {}", target_price);
        let (next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount) =
            compute_swap_step(
                sqrt_price_x96,
                target_price,
                liquidity,
                remaining_amount_in,
                fee_pips,
                zero_for_one,
            ).map_err(|e| format!("Ошибка вычисления шага свопа: {}", e))?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальный шаг свопа - next_sqrt_price_x96: {}, used_amount_in: {}, produced_amount_out: {}, fee_amount: {}", 
            next_sqrt_price_x96, used_amount_in, produced_amount_out, fee_amount);

        sqrt_price_x96 = next_sqrt_price_x96;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная sqrt_price_x96: {}", sqrt_price_x96);

        amount_out = amount_out
            .checked_add(produced_amount_out)
            .ok_or("Переполнение выходной суммы")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная выходная сумма: {}", amount_out);

        let total_used = used_amount_in
            .checked_add(fee_amount)
            .ok_or("Арифметическое переполнение")?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная общая использованная сумма: {}", total_used);

        remaining_amount_in = remaining_amount_in
            .checked_sub(total_used)
            .ok_or("Недостаточная входная сумма")?;
        info!("[SIMULATE_SWAP_TICK_BY_TICK] Остаток после обработки: {}", remaining_amount_in);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная оставшаяся входная сумма: {}", remaining_amount_in);
    }

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Симуляция свопа завершена. amount_out: {}, sqrt_price_x96: {}", amount_out, sqrt_price_x96);
    Ok((amount_out, sqrt_price_x96))
}

/// Симулирует своп токенов по заданному пути пулов Uniswap
///
/// # Аргументы
/// * `pool_path` - Массив пулов Uniswap, через которые будет выполняться своп
/// * `start_amount` - Начальная сумма токенов для свопа
/// * `start_token` - Адрес начального токена
/// * `aave_liquidity` - Информация о ликвидности токенов в протоколе Aave
///
/// # Возвращаемое значение
/// * `Ok(Some((U256, Vec<U256>)))` - Успешный результат свопа, содержащий:
///   - Финальную сумму токенов после всех свопов
///   - Вектор промежуточных результатов для каждого свопа
/// * `Ok(None)` - Своп невозможен или неприбылен
/// * `Err(String)` - Ошибка при выполнении свопа
///
/// # Алгоритм работы
/// 1. Проверяет доступную ликвидность начального токена в Aave
/// 2. Корректирует начальную сумму с учетом доступной ликвидности
/// 3. Проверяет минимальный порог прибыльности для начального токена
/// 4. Последовательно симулирует свопы через каждый пул в пути
/// 5. Проверяет, что конечный токен совпадает с начальным
/// 6. Вычисляет комиссию Aave (0.09%)
/// 7. Сравнивает финальную сумму с порогом прибыльности
/// 8. Возвращает результат, если своп прибылен
pub fn simulate_path_swap(
    pool_path: &[UniswapPool],
    start_amount: U256,
    start_token: Address,
    aave_liquidity: &AaveTokenLiquidity,
) -> Result<Option<(U256, Vec<U256>)>, String> {
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начало симуляции свопа по пути. start_amount: {}, start_token: {:?}", start_amount, start_token);
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Длина пути пулов: {}", pool_path.len());

    let available_liquidity = aave_liquidity
        .token_info
        .get(&start_token)
        .map(|(_, liquidity)| *liquidity)
        .unwrap_or(U256::zero());
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Доступная ликвидность для токена {:?}: {}", start_token, available_liquidity);

    if available_liquidity.is_zero() {
        info!("[{}] Нет ликвидности для токена {:?}", "ARB SKIP".black().on_red(), start_token);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Нет ликвидности, возвращаем None");
        return Ok(None);
    }

    let max_start_amount = available_liquidity;
    let start_amount = start_amount.min(max_start_amount);
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Скорректированная начальная сумма: {}", start_amount);

    if start_amount.is_zero() {
        info!("[{}] Нулевая сумма для токена {:?}", "ARB SKIP".black().on_red(), start_token);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Нулевая начальная сумма, возвращаем None");
        return Ok(None);
    }

    let min_profit_threshold = match MIN_PROFIT_THRESHOLD_BY_TOKEN.get(&start_token) {
        Some(&threshold) => {
            info!(
                "[ UNISWAP_ARB_SCANNER_DEBUG ] Минимальный порог прибыли: {} {:?}", 
                threshold, start_token
            );
            threshold
        }
        None => {
            info!(
                "[{}] Токен {:?} не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", 
                "ARB SKIP".black().on_red(), start_token
            );
            return Ok(None);
        }
    };

    if pool_path.is_empty() {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Пустой путь пулов, возвращаем ошибку");
        return Err("Путь пуст".to_string());
    }

    let mut current_amount = start_amount;
    let mut current_token = start_token;
    let mut intermediate_outputs = Vec::with_capacity(pool_path.len());
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Начальная текущая сумма: {}, текущий токен: {:?}", current_amount, current_token);

    for (i, pool) in pool_path.iter().enumerate() {
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обработка пула {}: {:?}", i, pool.uniswap_pool_address);
        info!("[ UNISWAP_ARB_SCANNER_DEBUG ] Текущий тик пула: {}", pool.uniswap_tick_current);
        let zero_for_one = current_token == pool.uniswap_token_a;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] zero_for_one: {}", zero_for_one);
        let (amount_out, next_sqrt_price) = simulate_swap_tick_by_tick(pool, current_amount, zero_for_one)?;
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Результат свопа - amount_out: {}, next_sqrt_price: {}", amount_out, next_sqrt_price);
        intermediate_outputs.push(amount_out);
        current_amount = amount_out;
        current_token = if zero_for_one {
            pool.uniswap_token_b
        } else {
            pool.uniswap_token_a
        };
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Обновленная текущая сумма: {}, текущий токен: {:?}", current_amount, current_token);
    }

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальный токен: {:?}", current_token);
    if current_token != start_token {
        info!(
            "[{}] Некорректный путь (длина: {}): конечный токен {:?} != стартовый {:?}", 
            "ARB SKIP".black().on_red(), pool_path.len(), current_token, start_token
        );
        info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Некорректный путь, конечный токен не совпадает с начальным");
        return Ok(None);
    }

    let final_amount_out = current_amount;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Финальная выходная сумма: {}", final_amount_out);

    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Вычисление комиссии Aave");
    let aave_fee = start_amount * U256::from(9) / U256::from(10000); // 0.09%
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Комиссия Aave: {}", aave_fee);

    let total_threshold = min_profit_threshold
        .checked_add(aave_fee)
        .ok_or("Арифметическое переполнение при вычислении комиссии Aave")?;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Общий порог: {}", total_threshold);

    let profit_threshold = start_amount.checked_add(total_threshold).ok_or("Арифметическое переполнение")?;
    info!("[ UNISWAP_ARB_SCANNER_DEBUG  ] Порог прибыли: {}", profit_threshold);

    if final_amount_out > profit_threshold {
        let profit = final_amount_out.checked_sub(start_amount).ok_or("Арифметическое переполнение")?;
        info!(
            "[{}] Прибыль: {} {:?} для пути: {}", 
            "ARB SUCCESS".red(),
            profit,
            start_token,
            format!("{:?}", pool_path.iter().map(|p| p.uniswap_pool_address).collect::<Vec<_>>()).red()
        );
        info!("[ UNISWAP_ARB_SCANNER_DEBUG ] Прибыльный своп, прибыль: {} {:?}", profit, start_token);
        Ok(Some((final_amount_out, intermediate_outputs)))
    } else {
        info!(
            "[ UNISWAP_ARB_SCANNER_DEBUG ] Своп не прибыльный, final_amount_out: {} <= порог: {} {:?}", 
            final_amount_out, profit_threshold, start_token
        );
        Ok(None)
    }
}





