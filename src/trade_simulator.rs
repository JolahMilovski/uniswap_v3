use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    path_builder::{ArbitragePath, PathBuilder},
    uniswap_events::PoolEventInfo,
    uniswap_graph::{UniswapPool, UniversalGraph},
    uniswap_v3::tick_to_sqrt_price,
};

use arc_swap::ArcSwap;
use ethers::types::{Address, U256};
use im::OrdMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{debug, error, info, warn};

// Константы для математики Uniswap V3
const MIN_TICK: i32 = -887272; // Минимальный тик
const MAX_TICK: i32 = 887272; // Максимальный тик

/// Симулятор торгов для поиска арбитражных возможностей
/// Основные возможности:
/// - Обработка событий пулов Uniswap в реальном времени
/// - Поиск прибыльных арбитражных путей
/// - Оптимизация размера займа для максимизации прибыли
/// - Проверка ликвидности на всех этапах арбитража
/// - Асинхронная обработка множественных путей

#[derive(Clone)]
pub struct TradeSimulator {
    route_builder: Arc<PathBuilder>,
    aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
    graph: Arc<ArcSwap<UniversalGraph>>,
}

impl TradeSimulator {


    /// Создает новый экземпляр симулятора торгов
    pub fn new(
        route_builder: Arc<PathBuilder>,
        aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<ArcSwap<UniversalGraph>>,
    ) -> Self {
        info!(
            "[⚡TRADE_SIMULATOR_NEW 📈 ] Инициализация симулятора: {} путей загружено",
            route_builder.paths.len()
        );
        TradeSimulator {
            route_builder,
            aave_liquidity_rx,
            graph,
        }
    }


    /// Запускает основной цикл симулятора для обработки событий пулов
    pub async fn run(&mut self, mut simulator_rx: MpscReceiver<PoolEventInfo>) {

        info!("[⚡  TRADE_SIMULATOR  📈 ] Запуск симулятора ");

        while let Some(event) = simulator_rx.recv().await {
            
            info!(
                "[⚡  TRADE_SIMULATOR  📈 event: {}] Получено событие пула: адрес={:?}, тик={}",
                event.event_id, event.address, event.current_tick
            );

            if let Err(e) = self.process_trade_event(event).await {
                error!(
                    "[⚡  TRADE_SIMULATOR  📈 event: N/A] Ошибка обработки события: {:?}",
                    e
                );
            }
        }
        info!("[⚡  TRADE_SIMULATOR  📈 ] Симулятор завершил работу");
    }



/// Находит следующий инициализированный тик в направлении свапа
fn find_next_initialized_tick(
    tick_current: i32,
    zero_for_one: bool,
    tick_map: &OrdMap<i32, (i128, U256)>,
    event_id: &str,
    path_index: &str,
) -> i32 {
    debug!(
        "[⚡ TRADE_SIMULATOR_find_next_initialized_tick 📈 event: {} path: {}] Поиск следующего тика: текущий тик={}, направление={}",
        event_id, path_index, tick_current, if zero_for_one { "token0 → token1" } else { "token1 → token0" }
    );

    let next_tick = if zero_for_one {
        tick_map
            .range(..tick_current)
            .next_back()
            .map(|(&tick, _)| tick)
            .unwrap_or(MIN_TICK)
    } else {
        tick_map
            .range((tick_current + 1)..)
            .next()
            .map(|(&tick, _)| tick)
            .unwrap_or(MAX_TICK)
    };

    debug!(
        "[⚡ TRADE_SIMULATOR_find_next_initialized_tick 📈 event: {} path: {}] Найден следующий тик: {} (граничный: {})",
        event_id, path_index, next_tick, if zero_for_one { MIN_TICK } else { MAX_TICK }
    );
    next_tick
}

/// Вычисляет один шаг свапа до следующего тика
fn compute_swap_step(
    sqrt_price_current: u128,
    sqrt_price_target: u128,
    liquidity: u128,
    amount_remaining: u128,
    fee_pips: u32,
    zero_for_one: bool,
    event_id: &str,
    path_index: &str,
) -> (u128, u128, u128) {
    debug!(
        "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Вычисление шага свапа: текущая цена={}, целевая цена={}, ликвидность={}, остаток суммы={}, комиссия={} pips",
        event_id, path_index, sqrt_price_current, sqrt_price_target, liquidity, amount_remaining, fee_pips
    );

    if liquidity == 0 || amount_remaining == 0 {
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Шаг свапа прерван: ликвидность={} или остаток суммы={}",
            event_id, path_index, liquidity, amount_remaining
        );
        return (0, 0, sqrt_price_current);
    }

    let q96 = 1u128 << 96;
    let exact_in = true;

    let max_amount_in = if zero_for_one {
        if sqrt_price_target < sqrt_price_current {
            let delta = sqrt_price_current - sqrt_price_target;
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Вычисление max_amount_in (token0 → token1): delta={}",
                event_id, path_index, delta
            );
            let denominator = sqrt_price_current
                .checked_mul(sqrt_price_target)
                .and_then(|x| x.checked_div(q96))
                .unwrap_or(1);
            let numerator = liquidity.checked_mul(delta).unwrap_or(0);
            let result = numerator.checked_div(denominator).unwrap_or(0);
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] max_amount_in: числитель={}, знаменатель={}, результат={}",
                event_id, path_index, numerator, denominator, result
            );
            result
        } else {
            0
        }
    } else {
        if sqrt_price_target > sqrt_price_current {
            let delta = sqrt_price_target - sqrt_price_current;
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Вычисление max_amount_in (token1 → token0): delta={}",
                event_id, path_index, delta
            );
            let result = liquidity
                .checked_mul(delta)
                .and_then(|x| x.checked_div(q96))
                .unwrap_or(0);
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] max_amount_in: результат={}",
                event_id, path_index, result
            );
            result
        } else {
            0
        }
    };

    let amount_in = if exact_in {
        amount_remaining.min(max_amount_in)
    } else {
        max_amount_in
    };
    let fee = (amount_in * fee_pips as u128) / 1_000_000;
    let amount_in_after_fee = amount_in.saturating_sub(fee);
    debug!(
        "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Комиссия: вход={}, комиссия={}, вход после комиссии={}",
        event_id, path_index, amount_in, fee, amount_in_after_fee
    );

    let sqrt_price_new = if amount_in_after_fee >= max_amount_in {
        debug!(
            "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Полный переход к целевой цене={}",
            event_id, path_index, sqrt_price_target
        );
        sqrt_price_target
    } else {
        debug!(
            "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Частичный переход, вычисление промежуточной цены",
            event_id, path_index
        );
        if zero_for_one {
            let numerator = amount_in_after_fee
                .checked_mul(sqrt_price_current)
                .and_then(|x| x.checked_mul(sqrt_price_target))
                .and_then(|x| x.checked_div(q96))
                .unwrap_or(0);
            let denominator = liquidity
                .checked_add(
                    amount_in_after_fee
                        .checked_mul(sqrt_price_target)
                        .and_then(|x| x.checked_div(q96))
                        .unwrap_or(0),
                )
                .unwrap_or(1);
            let delta = numerator.checked_div(denominator).unwrap_or(0);
            let result = sqrt_price_current.saturating_sub(delta);
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Новая цена (token0 → token1): delta={}, результат={}",
                event_id, path_index, delta, result
            );
            result
        } else {
            let delta = amount_in_after_fee
                .checked_mul(q96)
                .and_then(|x| x.checked_div(liquidity))
                .unwrap_or(0);
            let result = sqrt_price_current.saturating_add(delta);
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Новая цена (token1 → token0): delta={}, результат={}",
                event_id, path_index, delta, result
            );
            result
        }
    };

    let amount_out = if zero_for_one {
        let price_change = sqrt_price_current.saturating_sub(sqrt_price_new);
        let result = price_change
            .checked_mul(liquidity)
            .and_then(|x| x.checked_div(sqrt_price_new.max(1)))
            .unwrap_or(0);
        debug!(
            "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Выход (token0 → token1): изменение цены={}, результат={}",
            event_id, path_index, price_change, result
        );
        result
    } else {
        if sqrt_price_new > sqrt_price_current {
            let delta_inv = (sqrt_price_new - sqrt_price_current)
                .checked_mul(q96)
                .and_then(|x| {
                    x.checked_div(
                        sqrt_price_current
                            .checked_mul(sqrt_price_new)
                            .and_then(|x| x.checked_div(q96))
                            .unwrap_or(1),
                    )
                })
                .unwrap_or(1);
            let result = liquidity.checked_div(delta_inv.max(1)).unwrap_or(0);
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Выход (token1 → token0): delta_inv={}, результат={}",
                event_id, path_index, delta_inv, result
            );
            result
        } else {
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Выход (token1 → token0): цена не изменилась",
                event_id, path_index
            );
            0
        }
    };

    debug!(
        "[⚡ TRADE_SIMULATOR_compute_swap_step 📈 event: {} path: {}] Шаг свапа завершен: вход={}, выход={}, новая цена={}",
        event_id, path_index, amount_in, amount_out, sqrt_price_new
    );

    (amount_in, amount_out, sqrt_price_new)
}

/// Обновляет активную ликвидность при пересечении тика
fn update_liquidity_on_tick_cross(
    liquidity_current: u128,
    tick: i32,
    tick_map: &OrdMap<i32, (i128, U256)>,
    zero_for_one: bool,
    event_id: &str,
    path_index: &str,
) -> u128 {
    debug!(
        "[⚡ TRADE_SIMULATOR_update_liquidity_on_tick_cross 📈 event: {} path: {}] Обновление ликвидности: тик={}, текущая ликвидность={}",
        event_id, path_index, tick, liquidity_current
    );

    if let Some(&(liquidity_delta, amount)) = tick_map.get(&tick) {
        debug!(
            "[⚡ TRADE_SIMULATOR_update_liquidity_on_tick_cross 📈 event: {} path: {}] Тик {}: изменение ликвидности={}, сумма={}",
            event_id, path_index, tick, liquidity_delta, amount
        );

        let new_liquidity = if zero_for_one {
            let result = liquidity_current.saturating_sub(liquidity_delta.abs() as u128);
            debug!(
                "[⚡ TRADE_SIMULATOR_update_liquidity_on_tick_cross 📈 event: {} path: {}] Движение влево: вычитаем {}, новая ликвидность={}",
                event_id, path_index, liquidity_delta.abs(), result
            );
            result
        } else {
            let result = liquidity_current + (liquidity_delta.abs() as u128);
            debug!(
                "[⚡ TRADE_SIMULATOR_update_liquidity_on_tick_cross 📈 event: {} path: {}] Движение вправо: добавляем {}, новая ликвидность={}",
                event_id, path_index, liquidity_delta.abs(), result
            );
            result
        };
        new_liquidity
    } else {
        warn!(
            "[⚡ TRADE_SIMULATOR_update_liquidity_on_tick_cross 📈 event: {} path: {}] Тик {} не найден, ликвидность не изменена",
            event_id, path_index, tick
        );
        liquidity_current
    }
}

/// Точная симуляция свапа Uniswap V3 с использованием тиковой математики
async fn compute_amount_out(
    &self,
    sqrt_price_x96: U256,
    liquidity: u128,
    tick_map: &OrdMap<i32, (i128, U256)>,
    current_tick: i32,
    fee_tier: u32,
    amount_in: U256,
    zero_for_one: bool,
    event_id: &str,
    path_index: &str,
) -> Option<U256> {
    info!(
        "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Симуляция свапа: вход={}, ликвидность={}, тик={}, комиссия={} pips, направление={}",
        event_id, path_index, amount_in, liquidity, current_tick, fee_tier, if zero_for_one { "token0 → token1" } else { "token1 → token0" }
    );

    if liquidity == 0 || amount_in.is_zero() {
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Симуляция прервана: ликвидность={} или вход={}",
            event_id, path_index, liquidity, amount_in
        );
        return None;
    }

    let mut sqrt_price_current = sqrt_price_x96.as_u128();
    let mut liquidity_active = liquidity;
    let mut amount_remaining = amount_in.as_u128();
    let mut amount_out_total = 0u128;
    let mut tick_current = current_tick;
    let mut steps_count = 0;

    while amount_remaining > 0 && liquidity_active > 0 {
        steps_count += 1;
        debug!(
            "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Шаг симуляции #{}, остаток={}, ликвидность={}, тик={}, цена={}",
            event_id, path_index, steps_count, amount_remaining, liquidity_active, tick_current, sqrt_price_current
        );

        let tick_next = Self::find_next_initialized_tick(tick_current, zero_for_one, tick_map, event_id, path_index);
        if zero_for_one && tick_next <= MIN_TICK {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Достигнут минимальный тик={}",
                event_id, path_index, MIN_TICK
            );
            break;
        }
        if !zero_for_one && tick_next >= MAX_TICK {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Достигнут максимальный тик={}",
                event_id, path_index, MAX_TICK
            );
            break;
        }

        let sqrt_price_next = match tick_to_sqrt_price(tick_next) {
            Ok(price) => price.as_u128(),
            Err(e) => {
                warn!(
                    "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Ошибка вычисления цены для тика {}: {}",
                    event_id, path_index, tick_next, e
                );
                break;
            }
        };

        let (amount_in_step, amount_out_step, sqrt_price_new) = Self::compute_swap_step(
            sqrt_price_current,
            sqrt_price_next,
            liquidity_active,
            amount_remaining,
            fee_tier,
            zero_for_one,
            event_id,
            path_index,
        );

        if amount_in_step == 0 {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Шаг свапа не выполнен: вход=0",
                event_id, path_index
            );
            break;
        }

        amount_remaining = amount_remaining.saturating_sub(amount_in_step);
        amount_out_total += amount_out_step;
        sqrt_price_current = sqrt_price_new;

        let crossed_tick = if zero_for_one {
            sqrt_price_new <= sqrt_price_next
        } else {
            sqrt_price_new >= sqrt_price_next
        };

        if crossed_tick {
            liquidity_active = Self::update_liquidity_on_tick_cross(
                liquidity_active,
                tick_next,
                tick_map,
                zero_for_one,
                event_id,
                path_index,
            );
            tick_current = tick_next;
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Тик {} пересечен, новая ликвидность={}",
                event_id, path_index, tick_next, liquidity_active
            );

            if liquidity_active == 0 {
                warn!(
                    "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Ликвидность исчерпана",
                    event_id, path_index
                );
                break;
            }
        }

        if amount_in_step == 0 && amount_out_step == 0 {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Застой в симуляции: вход=0, выход=0",
                event_id, path_index
            );
            break;
        }
    }

    if amount_out_total == 0 {
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Симуляция завершилась с нулевым выходом",
            event_id, path_index
        );
        return None;
    }

    info!(
        "[⚡ TRADE_SIMULATOR_compute_amount_out 📈 event: {} path: {}] Симуляция завершена: вход={}, выход={}, остаток={}, шагов={}",
        event_id, path_index, amount_in.as_u128(), amount_out_total, amount_remaining, steps_count
    );
    Some(U256::from(amount_out_total))
}

/// Вычисляет потенциальную прибыль от арбитража по заданному пути
async fn compute_arbitrage_profit(
    &self,
    path: &ArbitragePath,
    borrow_amount: U256,
    event_id: &str,
    path_index: &str,
) -> Option<U256> {
    debug!(
        "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Расчет прибыли арбитража: сумма займа={}, путь={:?}",
        event_id, path_index, borrow_amount, path.tokens
    );

    let mut current_amount = borrow_amount;
    let graph = self.graph.load();
    for (i, pool_address) in path.pools.iter().enumerate() {
        let token_in = path.tokens[i];
        let token_out = path.tokens[i + 1];
        if let Some(pool) = graph.edges.get(pool_address) {
            let zero_for_one = *pool.uniswap_token_a == token_in;
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Свап в пуле {:?}: токен входа={:?}, токен выхода={:?}, направление={}",
                event_id, path_index, pool_address, token_in, token_out, if zero_for_one { "token0 → token1" } else { "token1 → token0" }
            );

            let amount_out = self
                .compute_amount_out(
                    pool.uniswap_sqrt_price,
                    pool.uniswap_liquidity.as_u128(),
                    &pool.tick_map,
                    pool.uniswap_tick_current,
                    pool.uniswap_fee_tier,
                    current_amount,
                    zero_for_one,
                    event_id,
                    path_index,
                )
                .await
                .unwrap_or(U256::zero());

            if amount_out.is_zero() {
                warn!(
                    "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Нулевой выход для пула {:?}",
                    event_id, path_index, pool_address
                );
                return None;
            }
            debug!(
                "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Свап завершен: вход={}, выход={}",
                event_id, path_index, current_amount, amount_out
            );
            current_amount = amount_out;
        } else {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Пул {:?} не найден",
                event_id, path_index, pool_address
            );
            return None;
        }
    }

    let aave_fee = borrow_amount * U256::from(9) / U256::from(10000);
    if current_amount > borrow_amount + aave_fee {
        let profit = current_amount - borrow_amount - aave_fee;
        info!(
            "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Прибыль арбитража: {}, займ={}, комиссия Aave={}",
            event_id, path_index, profit, borrow_amount, aave_fee
        );
        Some(profit)
    } else {
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_arbitrage_profit 📈 event: {} path: {}] Арбитраж убыточен: выход={}, требуется={}",
            event_id, path_index, current_amount, borrow_amount + aave_fee
        );
        None
    }
}

/// Оптимизирует сумму займа для максимизации прибыли арбитража
/// Возвращает кортеж (оптимальная сумма займа, максимальная прибыль)
async fn optimize_borrow_amount(
    &self,
    path: &ArbitragePath,
    max_borrow: U256,
    event_id: &str,
    path_index: &str,
) -> Option<(U256, U256)> {
    info!(
        "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Оптимизация суммы займа: максимум={}, пулов={}",
        event_id, path_index, max_borrow, path.pools.len()
    );

    let coarse_step = max_borrow / U256::from(10);
    let mut optimal_amount = U256::zero();
    let mut max_profit = U256::zero();

    debug!(
        "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Фаза 1: Грубый поиск с шагом {}",
        event_id, path_index, coarse_step
    );

    for i in 1..=10 {
        let amount = coarse_step * U256::from(i);
        debug!(
            "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Проверка суммы {} (итерация {})",
            event_id, path_index, amount, i
        );

        if let Some(profit) = self.compute_arbitrage_profit(path, amount, event_id, path_index).await {
            debug!(
                "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Прибыль для суммы {}: {}",
                event_id, path_index, amount, profit
            );
            if profit > max_profit {
                max_profit = profit;
                optimal_amount = amount;
                debug!(
                    "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Новый максимум: сумма={}, прибыль={}",
                    event_id, path_index, amount, profit
                );
            }
        } else {
            debug!(
                "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Арбитраж убыточен для суммы {}",
                event_id, path_index, amount
            );
        }
    }

    if max_profit.is_zero() {
        warn!(
            "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Не найдено прибыльных вариантов в грубом поиске",
            event_id, path_index
        );
        return None;
    }

    info!(
        "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Грубый поиск завершен: оптимальная сумма={}, прибыль={}",
        event_id, path_index, optimal_amount, max_profit
    );

    let fine_step = coarse_step / U256::from(10);
    let search_start = optimal_amount.saturating_sub(coarse_step);
    let search_end = optimal_amount.saturating_add(coarse_step);

    debug!(
        "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Фаза 2: Точный поиск с шагом {}, диапазон {}-{}",
        event_id, path_index, fine_step, search_start, search_end
    );

    let mut current_amount = search_start;
    let mut iteration = 0;
    while current_amount <= search_end {
        debug!(
            "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Проверка суммы {} (итерация {})",
            event_id, path_index, current_amount, iteration
        );

        if let Some(profit) = self.compute_arbitrage_profit(path, current_amount, event_id, path_index).await {
            debug!(
                "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Прибыль для суммы {}: {}",
                event_id, path_index, current_amount, profit
            );
            if profit > max_profit {
                max_profit = profit;
                optimal_amount = current_amount;
                debug!(
                    "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Новый максимум: сумма={}, прибыль={}",
                    event_id, path_index, current_amount, profit
                );
            }
        }
        current_amount = current_amount.saturating_add(fine_step);
        iteration += 1;
    }

    if max_profit.is_zero() {
        warn!(
            "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Не найдено прибыльных вариантов в точном поиске",
            event_id, path_index
        );
        return None;
    }

    info!(
        "[⚡ TRADE_SIMULATOR_optimize_borrow_amount 📈 event: {} path: {}] Оптимизация завершена: оптимальная сумма={}, прибыль={}, итераций={}",
        event_id, path_index, optimal_amount, max_profit, iteration
    );
    Some((optimal_amount, max_profit))
}

/// Вычисляет оптимальную сумму займа из Aave для данного арбитражного пути
/// Возвращает кортеж (сумма займа, прибыль арбитража)
async fn compute_aave_borrow_amount(
    &self,
    path: &ArbitragePath,
    event_id: &str,
    path_index: &str,
) -> Option<(U256, U256)> {
    let base_token = path.tokens.first().copied().unwrap_or_default();
    debug!(
        "[⚡ TRADE_SIMULATOR_compute_aave_borrow_amount 📈 event: {} path: {}] Проверка займа Aave для токена {:?}: путь={:?}",
        event_id, path_index, base_token, path.tokens
    );

    let aave_liquidity = self.aave_liquidity_rx.borrow().clone();

    if let Some((_, virtual_balance)) = aave_liquidity.token_info.get(&base_token) {
        let max_borrow = *virtual_balance;
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_aave_borrow_amount 📈 event: {} path: {}] Ликвидность Aave: токен={:?}, максимальный заем={}",
            event_id, path_index, base_token, max_borrow
        );

        let optimal_result = self
            .optimize_borrow_amount(path, max_borrow, event_id, path_index)
            .await;

        if let Some((optimal_amount, profit)) = optimal_result {
            info!(
                "[⚡ TRADE_SIMULATOR_compute_aave_borrow_amount 📈 event: {} path: {}] Оптимальная сумма займа для токена {:?}: {} (максимум={}), прибыль={}",
                event_id, path_index, base_token, optimal_amount, max_borrow, profit
            );
            Some((optimal_amount, profit))
        } else {
            warn!(
                "[⚡ TRADE_SIMULATOR_compute_aave_borrow_amount 📈 event: {} path: {}] Не найдено прибыльных вариантов для токена {:?}",
                event_id, path_index, base_token
            );
            None
        }
    } else {
        warn!(
            "[⚡ TRADE_SIMULATOR_compute_aave_borrow_amount 📈 event: {} path: {}] Токен {:?} недоступен в Aave",
            event_id, path_index, base_token
        );
        None
    }
}

/// Обрабатывает торговое событие от пула Uniswap и ищет арбитражные возможности
pub async fn process_trade_event(&self, event: PoolEventInfo) -> Result<(), String> {
    let event_id = event.event_id.to_string();
    let pool_address = event.address;
    debug!(
        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Обработка события пула: адрес={:?}, текущий тик={}",
        event_id, pool_address, event.current_tick
    );

    let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
    if aave_liquidity.token_info.is_empty() {
        warn!(
            "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Нет ликвидности в Aave, обработка прервана",
            event_id
        );
        return Ok(());
    }

    debug!(
        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Ликвидность Aave доступна: {} токенов",
        event_id, aave_liquidity.token_info.len()
    );

    let graph = self.graph.load();
    let mut handles = Vec::new();

    let route_indices = self
        .route_builder
        .pool_to_paths
        .get(&pool_address)
        .map(|entry| entry.value().clone())
        .unwrap_or_default();

    info!(
        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Найдено {} путей для пула {:?}",
        event_id, route_indices.len(), pool_address
    );

    for route_index in route_indices {
        let path_index = format!("{}-{}", event_id, route_index);
        if let Some(route) = self.route_builder.paths.get(route_index) {
            debug!(
                "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Проверка пути: токены={:?}, пулы={:?}",
                event_id, path_index, route.tokens, route.pools
            );

            let route_pools: Vec<UniswapPool> = route
                .pools
                .iter()
                .filter_map(|addr| graph.edges.get(addr).map(|pool| pool.clone()))
                .collect();

            if route_pools.is_empty() {
                warn!(
                    "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Путь пуст, пропуск",
                    event_id, path_index
                );
                continue;
            }

            let simulator = self.clone();
            let route = route.clone();
            let aave_liquidity = aave_liquidity.clone();
            let event_id_clone = event_id.clone();
            let path_index_clone = path_index.clone();

            let handle = tokio::spawn(async move {
                let base_token = route.tokens.first().copied().unwrap_or_default();
                debug!(
                    "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Проверка базового токена {:?}",
                    event_id_clone, path_index_clone, base_token
                );

                if !aave_liquidity.token_info.contains_key(&base_token) {
                    warn!(
                        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Базовый токен {:?} недоступен в Aave",
                        event_id_clone, path_index_clone, base_token
                    );
                    return;
                }

                let aave_borrow_result = simulator
                    .compute_aave_borrow_amount(&route, &event_id_clone, &path_index_clone)
                    .await;
                if aave_borrow_result.is_none() {
                    warn!(
                        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Недостаточно ликвидности Aave для токена {:?}",
                        event_id_clone, path_index_clone, route.tokens.first().unwrap_or(&Address::zero())
                    );
                    return;
                }
                let (aave_borrow_amount, profit) = aave_borrow_result.unwrap();
                info!(
                    "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Сумма займа Aave: {}, прибыль: {}",
                    event_id_clone, path_index_clone, aave_borrow_amount, profit
                );

                let can_borrow_all = true;
                /*
                for (i, token) in route
                    .tokens
                    .iter()
                    .skip(1)
                    .take(route.tokens.len() - 2)
                    .enumerate()
                {
                    let pool_address = route.pools[i];
                    debug!(
                        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Проверка ликвидности токена {:?} в пуле {:?}",
                        event_id_clone, path_index_clone, token, pool_address
                    );
                    if !simulator
                        .compute_uniswap_borrow_amount(
                            *token,
                            pool_address,
                            aave_borrow_amount,
                            &event_id_clone,
                        )
                        .await
                    {
                        can_borrow_all = false;
                        debug!(
                            "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Недостаточно ликвидности для токена {:?} в пуле {:?}",
                            event_id_clone, path_index_clone, token, pool_address
                        );
                        break;
                    }
                }
                */

                if can_borrow_all && !profit.is_zero() {
                    info!(
                        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Прибыльный путь: токены={:?}, пулы={:?}, сумма займа={}, прибыль={}",
                        event_id_clone, path_index_clone, route.tokens, route.pools, aave_borrow_amount, profit
                    );
                } else {
                    debug!(
                        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Путь убыточен или ликвидность недостаточна",
                        event_id_clone, path_index_clone
                    );
                }
            });
            handles.push(handle);
        } else {
            warn!(
                "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {} path: {}] Путь не найден",
                event_id, path_index
            );
        }
    }

    info!(
        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Запущено {} потоков симуляций",
        event_id, handles.len()
    );

    for handle in handles {
        if let Err(e) = handle.await {
            error!(
                "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Ошибка в асинхронной задаче: {:?}",
                event_id, e
            );
        }
    }
    info!(
        "[⚡ TRADE_SIMULATOR_process_trade_event 📈 event: {}] Обработка события завершена",
        event_id
    );
    Ok(())
}

    /// Проверяет возможность займа указанного количества токена из пула Uniswap
    async fn compute_uniswap_borrow_amount(
        &self,
        token: Address,
        pool_address: Address,
        required_amount: U256,
        event_id: &str,
    ) -> bool {
        debug!(
            "[⚡  TRADE_SIMULATOR   📈 event: {}] Проверка ликвидности Uniswap: токен={:?}, пул={:?}, требуется={}",
            event_id, token, pool_address, required_amount
        );
        if let Some(pool) = self.graph.load().edges.get(&pool_address) {
            if self.check_token_liquidity(&pool, token, required_amount, event_id) {
                debug!(
                    "[⚡  TRADE_SIMULATOR   📈 event: {}] Ликвидность достаточна для токена {:?}",
                    event_id, token
                );
                true
            } else {
                warn!(
                    "[⚡  TRADE_SIMULATOR   📈 event: {}] Недостаточно ликвидности для токена {:?}",
                    event_id, token
                );
                false
            }
        } else {
            warn!(
                "[⚡  TRADE_SIMULATOR   📈 event: {}] Пул {:?} не найден в графе",
                event_id, pool_address
            );
            false
        }
    }

/// Проверяет достаточность ликвидности токена в пуле
    fn check_token_liquidity(
        &self,
        pool: &UniswapPool,
        token: Address,
        required_amount: U256,
        event_id: &str,
    ) -> bool {
        let is_token0 = *pool.uniswap_token_a == token;
        let liquidity = if is_token0 {
            pool.liquidity_token0
        } else {
            pool.liquidity_token1
        };
        let sufficient = liquidity >= required_amount;
        debug!(
            "[⚡  TRADE_SIMULATOR   📈 event: {}] Проверка ликвидности токена {:?}: требуется={}, доступно={}, достаточно={}",
            event_id, token, required_amount, liquidity, sufficient
        );
        sufficient
    }

}



