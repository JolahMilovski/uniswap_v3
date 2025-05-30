use crate::{path_builder::ArbitragePath, uniswap_graph::UniversalGraph};
use ethers::types::{Address, U256, U512};
use log::{info, warn};
use serde::Serialize;

/// Результат базовой арбитражной симуляции
#[derive(Debug, Clone, Serialize)]
pub struct BasicArbResult {
    pub input_token: Address,      // Адрес токена, с которого начинается арбитраж
    pub input_amount: U256,        // Сколько этого токена мы подаём на вход
    pub output_token: Address,     // Какой токен получаем в конце пути
    pub output_amount: U256,       // Сколько токенов мы получили на выходе
    pub profit: U256,              // Прибыль (output - input)
    pub path: ArbitragePath,       // Маршрут, по которому прошёл арбитраж
    pub is_profitable: bool,       // Был ли путь прибыльным (true/false)
    pub gas_estimate: U256,        // Оценка затрат газа
}

/// Результат свопа в пуле
#[derive(Debug, Clone, Serialize)]
pub struct SwapResult {
    pub amount_out: U256,
    pub new_sqrt_price: U256,
    pub liquidity: U256,
}

pub struct SwapStep {
    pub sqrt_price_next: U256,
    pub amount_in: U256,
    pub amount_out: U256,
    pub fee_amount: U256,
}


/// Константы для точности расчетов
const Q96: u128 = 2u128.pow(96);
const MIN_LIQUIDITY: u128 = 10_000; // Минимальная ликвидность для учета пула

/// Симулирует обычный путь без вмешательства - просто своп по всем пулам
/// 
/// # Аргументы
/// * `graph` - Граф пулов Uniswap
/// * `path` - Арбитражный путь для симуляции
/// * `input_token` - Токен входа
/// * `input_amount` - Количество входного токена
/// * `min_profit` - Минимальная прибыль для учета арбитража
/// * `gas_estimate` - Оценка затрат газа на транзакцию
/// 
/// # Возвращает
/// `BasicArbResult` с результатами симуляции
pub fn simulate_basic_arbitrage(
    graph: &UniversalGraph,
    path: &ArbitragePath,
    input_token: Address,
    input_amount: U256,
    min_profit: U256,
    gas_estimate: U256,
) -> BasicArbResult {
    // Проверка на минимальную сумму входа
    if input_amount < U256::from(1_000) { // Минимум 1000 единиц токена
        return BasicArbResult {
            input_token,
            input_amount,
            output_token: input_token,
            output_amount: U256::zero(),
            profit: U256::zero(),
            path: path.clone(),
            is_profitable: false,
            gas_estimate,
        };
    }

    let mut amount_in = input_amount;
    let mut token_in = input_token;

    for pool_addr in &path.pools {
        if let Some(pool) = graph.edges.get(pool_addr) {
            // Проверка ликвидности пула
            if pool.uniswap_liquidity < U512::from(MIN_LIQUIDITY) {
                warn!("Пул {} имеет недостаточную ликвидность", pool_addr);
                return BasicArbResult {
                    input_token,
                    input_amount,
                    output_token: token_in,
                    output_amount: U256::zero(),
                    profit: U256::zero(),
                    path: path.clone(),
                    is_profitable: false,
                    gas_estimate,
                };
            }

            let (token0, token1) = (pool.uniswap_token_a, pool.uniswap_token_b);
            let zero_for_one = token_in == token0;

            let result = match simulate_swap_tick_by_tick(
                amount_in,
                zero_for_one,
                U256::from(pool.uniswap_sqrt_price),
                U256::from(pool.uniswap_liquidity),
                pool.uniswap_fee_tier,
                &pool.tick_map,
            ) {
                Some(res) => res,
                None => {
                    warn!("Ошибка симуляции свопа в пуле {}", pool_addr);
                    return BasicArbResult {
                        input_token,
                        input_amount,
                        output_token: token_in,
                        output_amount: U256::zero(),
                        profit: U256::zero(),
                        path: path.clone(),
                        is_profitable: false,
                        gas_estimate,
                    };
                }
            };

            amount_in = result.amount_out;
            token_in = if zero_for_one { token1 } else { token0 };
        } else {
            // Пул отсутствует - обрываем симуляцию
            warn!("Пул {} не найден в графе", pool_addr);
            return BasicArbResult {
                input_token,
                input_amount,
                output_token: token_in,
                output_amount: U256::zero(),
                profit: U256::zero(),
                path: path.clone(),
                is_profitable: false,
                gas_estimate,
            };
        }
    }

    let output_token = token_in;
    let profit = amount_in.saturating_sub(input_amount);//
    let is_profitable = profit >= min_profit;

    BasicArbResult {
        input_token,
        input_amount,
        output_token,
        output_amount: amount_in,
        profit,
        path: path.clone(),
        is_profitable,
        gas_estimate,
    }
}

/// Сканирует все возможные арбитражные пути
/// 
/// # Аргументы
/// * `graph` - Граф пулов Uniswap
/// * `min_profit` - Минимальная прибыль для учета арбитража
/// * `default_input_amount` - Стандартная сумма для тестирования путей
/// * `gas_price` - Текущая цена газа для оценки затрат
pub fn scan_all_paths(
    graph: &UniversalGraph,
    min_profit: U256,
    default_input_amount: U256,
    gas_price: U256,
) -> Vec<BasicArbResult> {
    let paths = get_all_paths();
    let mut profitable_paths = Vec::new();

    for path in paths {
        if path.tokens.len() < 2 {
            continue;
        }

        let input_token = path.tokens[0];
        
        // Оценка газа: примерно 150k газа на пул + 200k базовых
        let gas_estimate = U256::from(200_000 + 150_000 * path.pools.len()) * gas_price;
        
        let result = simulate_basic_arbitrage(
            graph,
            &path,
            input_token,
            default_input_amount,
            min_profit,
            gas_estimate,
        );

        if result.is_profitable {
            info!(
                "🟢 Арбитраж найден: {} → {} | Профит: {} (Газ: {})",
                result.input_token, result.output_token, result.profit, result.gas_estimate
            );
            profitable_paths.push(result);
        }
    }

    profitable_paths
}

/// Получает все возможные пути из модуля path_builder
fn get_all_paths() -> Vec<ArbitragePath> {
    Vec::new()
}

/// Безопасно преобразует U512 в U256, обрезая до младших 256 бит (без потери значимых данных для sqrt_price_x96)
fn u512_to_u256(value: U512) -> U256 {
    let mut buf = [0u8; 64]; // U512 = 64 байта
    value.to_little_endian(&mut buf);
    U256::from_little_endian(&buf[..32]) // младшие 32 байта
}

/// Вычисляет следующий sqrt_price после входного токена (amount_in)
/// Формула: 
/// - zeroForOne: nextSqrtP = (L * sqrtP) / (L + amountIn * sqrtP / Q96)
/// - oneForZero: nextSqrtP = (L * sqrtP + amountIn * Q96) / L
pub fn get_next_sqrt_price_from_input(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_in: U256,
    zero_for_one: bool,
) -> Option<U256> {
    if amount_in.is_zero() || liquidity.is_zero() {
        return Some(sqrt_price_x96);
    }

    if zero_for_one {
        let numerator = U512::from(liquidity) * U512::from(sqrt_price_x96);
        let product = U512::from(amount_in) * U512::from(sqrt_price_x96);
        let denominator = U512::from(liquidity)
            .checked_add(product.checked_div(U512::from(Q96))?)?;

        Some(u512_to_u256(numerator.checked_div(denominator)?))
    } else {
        let product = U512::from(amount_in) * U512::from(Q96);
        let numerator = U512::from(liquidity) * U512::from(sqrt_price_x96) + product;
        let denominator = U512::from(liquidity);

        Some(u512_to_u256(numerator.checked_div(denominator)?))
    }
}

/// Вычисляет следующий sqrt_price после получения `amount_out`
/// Формула:
/// - zeroForOne: nextSqrtP = (L * sqrtP) / (L - amountOut * sqrtP / Q96)
/// - oneForZero: nextSqrtP = (L * sqrtP - amountOut * Q96) / L
pub fn get_next_sqrt_price_from_output(
    sqrt_price_x96: U256,
    liquidity: U256,
    amount_out: U256,
    zero_for_one: bool,
) -> Option<U256> {
    if amount_out.is_zero() || liquidity.is_zero() {
        return Some(sqrt_price_x96);
    }

    if zero_for_one {
        let numerator = U512::from(liquidity) * U512::from(sqrt_price_x96);
        let product = U512::from(amount_out) * U512::from(sqrt_price_x96);
        let denominator = U512::from(liquidity)
            .checked_sub(product.checked_div(U512::from(Q96))?)?;

        Some(u512_to_u256(numerator.checked_div(denominator)?))
    } else {
        let product = U512::from(amount_out) * U512::from(Q96);
        let numerator = U512::from(liquidity)
            .checked_mul(U512::from(sqrt_price_x96))?
            .checked_sub(product)?;
        let denominator = U512::from(liquidity);

        Some(u512_to_u256(numerator.checked_div(denominator)?))
    }
}


/// Точный расчет amount_out по формуле из Uniswap V3
fn calculate_amount_out(
    liquidity: U256,
    sqrt_price_start: U256,
    sqrt_price_end: U256,
    zero_for_one: bool,
) -> Option<U256> {
    if sqrt_price_start == sqrt_price_end {
        return Some(U256::zero());
    }

    if zero_for_one {
        // token0 -> token1: amountOut = liquidity * (sqrtPriceStart - sqrtPriceEnd) / (sqrtPriceStart * sqrtPriceEnd)
        let numerator = liquidity.checked_mul(
            sqrt_price_start.checked_sub(sqrt_price_end)?
        )?;
        
        let denominator = sqrt_price_start.checked_mul(sqrt_price_end)?;
        numerator.checked_div(denominator)
    } else {
        // token1 -> token0: amountOut = liquidity * (sqrtPriceStart - sqrtPriceEnd)
        liquidity.checked_mul(
            sqrt_price_end.checked_sub(sqrt_price_start)?
        )
    }
}

/// Рассчитывает потенциальную прибыль с учетом цены газа
pub fn calculate_net_profit(gross_profit: U256, gas_cost: U256) -> U256 {
    gross_profit.saturating_sub(gas_cost)
}

/// Фильтрует пути по минимальной прибыльности
pub fn filter_profitable_paths(
    paths: Vec<BasicArbResult>,
    min_net_profit: U256,
) -> Vec<BasicArbResult> {
    paths
        .into_iter()
        .filter(|res| {
            let net_profit = calculate_net_profit(res.profit, res.gas_estimate);
            net_profit >= min_net_profit
        })
        .collect()
}



/// Точная реализация свопа тик за тиком (Uniswap V3)
pub fn simulate_swap_tick_by_tick(
    mut amount_in: U256,
    zero_for_one: bool,
    mut sqrt_price_x96: U256,
    mut liquidity: U256,
    fee: u32,
    current_tick: i32,
    tick_spacing: i32,
    tick_map: &DashMap<i32, (i128, U512)>,
) -> Option<SwapResult> {
    let mut tick = current_tick;
    let mut total_amount_out = U256::zero();

    // Комиссия
    let fee_denominator = U256::from(1_000_000u64);
    let fee_numerator = U256::from(1_000_000u64 - fee);

    // Упорядоченная карта тиков
    let tick_entries: BTreeMap<i32, (i128, U512)> = tick_map.iter().map(|r| (*r.key(), r.value().clone())).collect();

    // Поиск следующего активного тика
    let mut next_active_tick = |tick: i32| -> Option<i32> {
        if zero_for_one {
            tick_entries.keys().rev().find(|&&t| t < tick).copied()
        } else {
            tick_entries.keys().find(|&&t| t > tick).copied()
        }
    };

    while amount_in > U256::zero() {
        let next_tick = match next_active_tick(tick) {
            Some(t) => t,
            None => break,
        };
        let target_sqrt_price = tick_entries.get(&next_tick)?.1;

        // Учитываем комиссию
        let amount_in_less_fee = amount_in.checked_mul(fee_numerator)?.checked_div(fee_denominator)?;

        // Рассчитываем delta sqrt_price
        let amount_in_shifted = amount_in_less_fee.checked_mul(U256::from(1u128 << 96))?;
        let delta = U512::from(amount_in_shifted).checked_div(U512::from(liquidity))?.as_u128();

        let next_price = if zero_for_one {
            if sqrt_price_x96.as_u128() <= delta {
                return None;
            }
            sqrt_price_x96 - U256::from(delta)
        } else {
            sqrt_price_x96 + U256::from(delta)
        };

        // Проверка достигли ли целевой цены
        let reached_tick = if zero_for_one {
            next_price <= target_sqrt_price
        } else {
            next_price >= target_sqrt_price
        };

        let end_price = if reached_tick {
            target_sqrt_price
        } else {
            next_price
        };

        // Вычисление amount_out
        let amount_out = calculate_amount_out(liquidity, sqrt_price_x96, end_price, zero_for_one)?;
        total_amount_out = total_amount_out.checked_add(amount_out)?;

        sqrt_price_x96 = end_price;

        if reached_tick {
            let liquidity_net = tick_entries.get(&next_tick)?.0;
            if liquidity_net < 0 {
                liquidity = liquidity.checked_sub(U256::from((-liquidity_net) as u128))?;
            } else {
                liquidity = liquidity.checked_add(U256::from(liquidity_net as u128))?;
            }
            tick = next_tick;
        } else {
            break;
        }

        // Своп полностью обработан
        amount_in = U256::zero();
    }

    Some(SwapResult {
        amount_out: total_amount_out,
        new_sqrt_price: sqrt_price_x96,
        liquidity,
    })
}

pub fn compute_swap_step(
    sqrt_price_x96: U256,
    sqrt_price_target_x96: U256,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: u32,
    zero_for_one: bool,
) -> Option<SwapStep> {
    let fee_denominator = U256::from(1_000_000u64);
    let fee_numerator = fee_denominator - U256::from(fee_pips);

    let max_amount_in = if zero_for_one {
        // amountIn = (L * (sqrtP - target)) / (sqrtP * target) * Q96
        let numerator = U512::from(liquidity)
            .checked_mul(U512::from(sqrt_price_x96 - sqrt_price_target_x96))?
            .checked_mul(U512::from(Q96))?;
        let denominator = U512::from(sqrt_price_x96)
            .checked_mul(U512::from(sqrt_price_target_x96))?;
        u512_to_u256(numerator.checked_div(denominator)?)
    } else {
        // amountIn = (L * (target - sqrtP)) / Q96
        let numerator = U512::from(liquidity)
            .checked_mul(U512::from(sqrt_price_target_x96 - sqrt_price_x96))?;
        u512_to_u256(numerator.checked_div(U512::from(Q96))?)
    };

    // Добавим комиссию
    let amount_in_with_fee = amount_remaining
        .checked_mul(fee_numerator)?
        .checked_div(fee_denominator)?;

    let (sqrt_price_next, amount_in, amount_out) = if amount_in_with_fee >= max_amount_in {
        // можно дойти до target sqrt_price
        let sqrt_price_next = sqrt_price_target_x96;
        let amount_in = max_amount_in;

        let amount_out = if zero_for_one {
            // Δy = L * (sqrtP_next - sqrtP)
            let delta = U512::from(liquidity)
                .checked_mul(U512::from(sqrt_price_x96 - sqrt_price_next))?;
            u512_to_u256(delta.checked_div(U512::from(Q96))?)
        } else {
            // Δx = L * (sqrtP_next - sqrtP) / (sqrtP_next * sqrtP) * Q96
            let numerator = U512::from(liquidity)
                .checked_mul(U512::from(sqrt_price_next - sqrt_price_x96))?
                .checked_mul(U512::from(Q96))?;
            let denominator = U512::from(sqrt_price_next)
                .checked_mul(U512::from(sqrt_price_x96))?;
            u512_to_u256(numerator.checked_div(denominator)?)
        };

        (sqrt_price_next, amount_in, amount_out)
    } else {
        // не дойдём до target sqrt_price
        let sqrt_price_next = if zero_for_one {
            get_next_sqrt_price_from_input(
                sqrt_price_x96,
                liquidity,
                amount_in_with_fee,
                true,
            )?
        } else {
            get_next_sqrt_price_from_input(
                sqrt_price_x96,
                liquidity,
                amount_in_with_fee,
                false,
            )?
        };

        let amount_in = amount_in_with_fee;
        let amount_out = if zero_for_one {
            // Δy = L * (sqrtP_start - sqrtP_next) / Q96
            let delta = U512::from(liquidity)
                .checked_mul(U512::from(sqrt_price_x96 - sqrt_price_next))?;
            u512_to_u256(delta.checked_div(U512::from(Q96))?)
        } else {
            // Δx = L * (sqrtP_next - sqrtP_start) / (sqrtP_next * sqrtP_start) * Q96
            let numerator = U512::from(liquidity)
                .checked_mul(U512::from(sqrt_price_next - sqrt_price_x96))?
                .checked_mul(U512::from(Q96))?;
            let denominator = U512::from(sqrt_price_next)
                .checked_mul(U512::from(sqrt_price_x96))?;
            u512_to_u256(numerator.checked_div(denominator)?)
        };

        (sqrt_price_next, amount_in, amount_out)
    };

    let fee_amount = amount_in
        .checked_mul(U256::from(fee_pips))?
        .checked_div(fee_denominator)?;

    Some(SwapStep {
        sqrt_price_next,
        amount_in,
        amount_out,
        fee_amount,
    })
}

