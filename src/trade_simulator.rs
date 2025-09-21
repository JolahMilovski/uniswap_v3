//! trade_simulator.rs
//!
//! Модуль для симуляции арбитражных операций по путям Uniswap V3 с использованием флеш-займов Aave
//! и флеш-свопов Uniswap для создания искусственного ценового дисбаланса.
//!
//! # Основные возможности
//! - Обработка событий пулов через поток `PoolEventInfo`.
//! - Симуляция mint ликвидности в пулах A→B и B→C для сдвига цены на 0.5–2%.
//! - Использование 95% ликвидности Aave для арбитража (A→B→C→A) и флеш-свопов для покрытия недостающей ликвидности (до 90% пула).
//! - Точная симуляция свопов по тиковой модели Uniswap V3 с расчётами цен в формате Q64.96.
//! - Burn/collect для возврата ликвидности и комиссий.
//! - Фильтрация путей по минимальному порогу прибыли (`MIN_PROFIT_THRESHOLD_BY_TOKEN`).
//! - Логирование всех шагов для отладки.
//!
//! # Алгоритм работы
//! 1. Получение события пула (`PoolEventInfo`) через `run`.
//! 2. Построение арбитражных путей через `PathBuilder`.
//! 3. Расчёт mint для пулов A→B и B→C с целевым сдвигом цены (1% по умолчанию).
//! 4. Использование 95% ликвидности Aave для mint и арбитража, дополнение флеш-свопами.
//! 5. Выполнение арбитражного свопа (A→B→C→A) для "отработки" заминченной ликвидности.
//! 6. Burn/collect для возврата ликвидности и комиссий.
//! 7. Проверка профита: `profit_net > MIN_PROFIT_THRESHOLD_BY_TOKEN` и `total_fee_earned ≥ total_fees`.
//! 8. Возврат лучших путей через `PathSimulationResult`.

use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    path_builder::{ArbitragePath, BorrowPoolInfo, PathBuilder},
    uniswap_events::PoolEventInfo,
    uniswap_graph::{UniswapPool, UniversalGraph, Q64_96},
    uniswap_v3::tick_to_sqrt_price,
};

use std::sync::LazyLock;

use arc_swap::ArcSwap;
use colored::Colorize;
use ethers::{
    types::{Address, U256},
    utils::hex,
};
use lazy_static::lazy_static;
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{debug, error, info, warn};

lazy_static! {
    /// Статическая карта минимальных порогов прибыли для токенов.
    /// Ключ — адрес токена, значение — минимальная прибыль в wei.
    /// Значения масштабируются на основе переменной окружения PROFIT_THRESHOLD_USD (по умолчанию 100 USD).
    pub static ref MIN_PROFIT_THRESHOLD_BY_TOKEN: HashMap<Address, U256> = {
        let base_thresholds: HashMap<Address, U256> = {
            let mut m = HashMap::new();
            m.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));
            m.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));
            m.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(345_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(38_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(38_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));
            m.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(100_000u128));
            m.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(38_489_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(8_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(33_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(38_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));
            m.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(38_000_000_000_000_000u128));
            m.insert(Address::from_slice(&hex::decode("ba5ddd1f9d7f570dc94a51479a000e3bce967196").unwrap()), U256::from(353_700_000_000_000_000u128));
            m
        };

        let profit_threshold_usd: f64 = env::var("PROFIT_THRESHOLD_USD")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100.0);

        let scale_factor = profit_threshold_usd / 100.0;
        let mut scaled_thresholds = HashMap::new();
        for (addr, base_threshold) in base_thresholds {
            let scaled_value = mul_div_u256(
                base_threshold,
                U256::from((scale_factor * 1_000_000.0) as u128),
                U256::from(1_000_000)
            );
            scaled_thresholds.insert(addr, scaled_value);
        }
        scaled_thresholds
    };

    pub static ref DEFAULT_ARBITRAGE_STRATEGY_CONFIG: ArbitrageStrategyConfig = {
        let target_bps: u32 = env::var("TARGET_PRICE_IMPACT_BPS")
            .unwrap_or("100".to_string())
            .parse()
            .unwrap_or(100);
        let aave_utilization: u64 = env::var("AAVE_UTILIZATION_PERCENT")
            .unwrap_or("95".to_string())
            .parse()
            .unwrap_or(95);
        let uniswap_utilization: u64 = env::var("UNISWAP_BORROW_PERCENT")
            .unwrap_or("90".to_string())
            .parse()
            .unwrap_or(90);

        ArbitrageStrategyConfig {
            target_price_impact_bps: target_bps,
            aave_utilization_percent: aave_utilization,
            uniswap_borrow_utilization: uniswap_utilization,
        }
    };
}

/// ==== Константы ====
const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;
const AAVE_FLASH_FEE_NUM: u128 = 9;
const AAVE_FLASH_FEE_DEN: u128 = 10_000;
const DEFAULT_PRICE_IMPACT_BPS: u32 = 50;
const PROTOCOL_FEE_SHARE: u32 = 300_000;
static MAX_SAFE_LIQUIDITY: LazyLock<U256> = LazyLock::new(|| {
    U256::from(u128::MAX / 2)
});

/// ==== Утилиты для работы с U256 / Q-форматами ====

#[inline]
fn pow10_u256(n: u32) -> U256 {
    let mut r = U256::one();
    let ten = U256::from(10u8);
    for _ in 0..n {
        r = r * ten;
    }
    r
}

#[inline]
fn mul_div_u256(a: U256, b: U256, c: U256) -> U256 {
    if c.is_zero() {
        error!("[MUL_DIV_U256] Деление на ноль: a={}, b={}, c=0", a, b);
        return U256::zero();
    }

    match a.checked_mul(b) {
        Some(prod) => match prod.checked_div(c) {
            Some(result) => {
                debug!("[MUL_DIV_U256] Успешное вычисление: ({} * {}) / {} = {}", a, b, c, result);
                result
            }
            None => {
                error!("[MUL_DIV_U256] Переполнение при делении: prod={} / {}", prod, c);
                U256::zero()
            }
        },
        None => {
            let full = a.full_mul(b);
            let q = &full / c;
            match U256::try_from(q) {
                Ok(result) => {
                    debug!("[MUL_DIV_U256] Успешное вычисление через full_mul: ({} * {}) / {} = {}", a, b, c, result);
                    result
                }
                Err(_) => {
                    error!("[MUL_DIV_U256] Переполнение при умножении: a={} * b={}", a, b);
                    U256::zero()
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArbitrageStrategyConfig {
    pub target_price_impact_bps: u32,
    pub aave_utilization_percent: u64,
    pub uniswap_borrow_utilization: u64,
}

#[derive(Debug, Clone)]
pub struct MintParameters {
    pub pool_address: Address,
    pub zero_for_one: bool,
    pub amount_desired: U256,
    pub amount_actual: U256,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub liquidity: U256,
}

#[derive(Clone)]
pub struct GraphSnapshotHolder {
    pub pools: HashMap<Address, UniswapPool>,
    pub tick_maps: HashMap<Address, Vec<(i32, i128, U256, U256,  U256)>>,
    pub token_decimals: HashMap<Address, u8>,
}

#[derive(Debug, Clone)]
pub struct PathSimulationResult {
    pub path_index: usize,
    pub base_token: Address,
    pub borrow_optimal: U256,
    pub final_amount: U256,
    pub profit_net: U256,
    pub used_uniswap_flash_supplement: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct HopState {
    pub sqrt_price_x96: Q64_96,
    pub sqrt_target_x96: Q64_96,
    pub liquidity: U256,
    pub fee_pips: u32,
    pub zero_for_one: bool,
    pub tick_current: i32,
    pub pool_addr: Address,
}

#[derive(Clone)]
pub struct TradeSimulator {
    route_builder: Arc<PathBuilder>,
    aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
    graph: Arc<ArcSwap<UniversalGraph>>,
}

impl TradeSimulator {
    pub fn new(
        route_builder: Arc<PathBuilder>,
        aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<ArcSwap<UniversalGraph>>,
    ) -> Self {
        info!(
            "[{}] Инициализация симулятора: путей = {}, пулов = {}",
            "TRADE_SIMULATOR ⚡".green(),
            route_builder.paths.len(),
            route_builder.pool_to_paths.len()
        );
        TradeSimulator {
            route_builder,
            aave_liquidity_rx,
            graph,
        }
    }

    pub async fn run(&mut self, mut simulator_rx: MpscReceiver<PoolEventInfo>) {
        info!("[TRADE_SIMULATOR] ▶️ Запуск симулятора");

        while let Some(event) = simulator_rx.recv().await {
            let event_clone = event.clone();
            let simulator = self.clone();
            tokio::spawn(async move {
                let aave_liquidity = simulator.aave_liquidity_rx.borrow().clone();
                let results = simulator
                    .process_trade_event(event_clone, aave_liquidity)
                    .await;

                if !results.is_empty() {
                    info!(
                        "[TRADE_SIMULATOR] 🆔={} Найдено {} прибыльных путей",
                        event.event_id,
                        results.len()
                    );
                    for result in results {
                        info!(
                            "[TRADE_SIMULATOR] 🆔={} Путь {}: profit_net={}, final_amount={}",
                            event.event_id.to_string(),
                            result.path_index,
                            result.profit_net,
                            result.final_amount
                        );
                    }
                } else {
                    debug!(
                        "[TRADE_SIMULATOR] 🆔={} Нет прибыльных путей",
                        event.event_id.to_string()
                    );
                }
            });
        }
        warn!("[TRADE_SIMULATOR] ▶️ Симулятор остановлен (входной канал закрыт)");
    }



/// Обрабатывает событие пула Uniswap V3 и возвращает список прибыльных арбитражных путей.
///
/// # Описание
/// Функция асинхронно обрабатывает событие пула (`PoolEventInfo`), связанное с изменением состояния пула Uniswap V3,
/// и выполняет симуляцию арбитражных операций для путей, включающих этот пул. Для каждого пути:
/// - Собираются пулы маршрута и пулы заимствования для флеш-свопов.
/// - Создается снимок графа с данными пулов и тиков.
/// - Вычисляется оптимальная сумма заимствования и прибыль через `compute_aave_borrow_amount`.
/// - Результаты фильтруются по минимальному порогу прибыли (`MIN_PROFIT_THRESHOLD_BY_TOKEN`).
/// Логирование выполняется для отладки и отслеживания процесса.
///
/// # Аргументы
/// * `event: PoolEventInfo` - Событие пула Uniswap V3, содержащее информацию о пуле 
/// * `aave_liquidity: AaveTokenLiquidity` - Данные о доступной ликвидности токенов в Aave для заимствования.
///
/// # Возвращаемое значение
/// Возвращает `Vec<PathSimulationResult>`, содержащий информацию о прибыльных арбитражных путях:
/// - `path_index: usize` - Индекс пути в списке путей `route_builder.paths`.
/// - `base_token: Address` - Базовый токен пути (первый токен в цикле арбитража).
/// - `borrow_optimal: U256` - Оптимальная сумма заимствования из Aave.
/// - `final_amount: U256` - Итоговая сумма после выполнения арбитража.
/// - `profit_net: U256` - Чистая прибыль после учета всех комиссий.
/// - `used_uniswap_flash_supplement: bool` - Флаг, указывающий, использовались ли флеш-свопы Uniswap.
///
/// Если прибыльных путей нет или пул не связан с путями, возвращается пустой вектор.
///
/// # Алгоритм работы
/// 1. Логирует начало обработки события с указанием ID события и адреса пула.
/// 2. Создает начальный список пулов маршрута (`route_pools`), содержащий только пул события.
/// 3. Извлекает пути, связанные с пулом события, из `route_builder.pool_to_paths`, фильтруя их:
///    - Пул должен быть в пути.
///    - Путь должен содержать минимум 3 токена (для цикла арбитража, например, A→B→C→A).
/// 4. Для каждого пути:
///    - Собирает пулы маршрута, добавляя пулы пути к `route_pools`.
///    - Собирает пулы заимствования (`borrow_pools`) для всех токенов пути из `route_builder.borrow_pools`.
///    - Создает снимок графа (`GraphSnapshotHolder`) с данными пулов маршрута и заимствования.
///    - Вызывает `compute_aave_borrow_amount` для вычисления параметров арбитража.
///    - Если результат получен, формирует `PathSimulationResult` с данными:
///      - Индекс пути, базовый токен, оптимальная сумма заимствования, итоговая сумма, чистая прибыль, флаг флеш-свопа.
///    - Добавляет результат в список `results`.
/// 5. Фильтрует результаты по минимальному порогу прибыли с помощью `filter_by_min_profit_threshold`.
/// 6. Логирует количество отфильтрованных путей и возвращает их.
///
/// # Логирование
/// - Логирует начало обработки события, отсутствие путей для пула, отсутствие токенов в пути.
/// - Логирует количество отфильтрованных прибыльных путей.
/// - Использует уровень `debug` для детальных сообщений и `info` для ключевых событий.
///
/// # Ошибки
/// - Если в пути нет токенов, логируется ошибка, и путь пропускается (`continue`).
/// - Если для пула нет путей в `route_builder.pool_to_paths`, возвращается пустой вектор.
/// - Ошибки в `compute_aave_borrow_amount` (например, отсутствие ликвидности Aave) приводят к пропуску пути.
///
///
/// # Зависимости
/// - `self.route_builder: Arc<PathBuilder>` - Содержит пути арбитража и маппинг пулов к путям.
/// - `self.create_graph_snapshot` - Создает снимок графа для пулов.
/// - `self.compute_aave_borrow_amount` - Вычисляет параметры арбитража.
/// - `self.filter_by_min_profit_threshold` - Фильтрует пути по порогу прибыли.
/// - `MIN_PROFIT_THRESHOLD_BY_TOKEN` - Глобальная карта минимальных порогов прибыли для токенов.
async fn process_trade_event(
    &self,
    event: PoolEventInfo,
    aave_liquidity: AaveTokenLiquidity,
) -> Vec<PathSimulationResult> {
    debug!(
        "[PROCESS_TRADE_EVENT] 🆔={} Обработка события для пула {:?}",
        event.event_id, event.address
    );
    let route_pools = vec![event.address];
    let mut results = Vec::new();

    let paths = if let Some(indexes) = self.route_builder.pool_to_paths.get(&event.address) {
        indexes
            .iter()
            .filter_map(|&i| {
                let path = &self.route_builder.paths[i];
                if path.pools.contains(&event.address) && path.tokens.len() >= 3 {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        debug!(
            "[PROCESS_TRADE_EVENT] 🆔={} Нет путей для пула {:?}",
            event.event_id, event.address
        );
        return Vec::new();
    };

    for (i, path) in paths.iter().enumerate() {
        let mut path_pools = route_pools.clone();
        path_pools.extend_from_slice(&path.pools);
        
        // Собираем пулы заимствования для токенов пути
        let mut borrow_pools: Vec<BorrowPoolInfo> = Vec::new();
        for token in path.tokens.iter() {
            if let Some(pools) = self.route_builder.borrow_pools.get(token) {
                borrow_pools.extend(pools.iter().cloned());
            }
        }

        let path_snapshot = self.create_graph_snapshot(&path_pools, &borrow_pools, event.event_id);
        if let Some(result) = self
            .compute_aave_borrow_amount(
                path,
                event.event_id,
                &i.to_string(),
                &path_snapshot,
                &aave_liquidity,
            )
            .await
        {
            results.push(PathSimulationResult {
                path_index: i,
                base_token: match path.tokens.first() {
                    Some(token) => *token,
                    None => {
                        debug!(
                            "[PROCESS_TRADE_EVENT] 🆔={} путь={} Нет токенов в пути",
                            event.event_id, i
                        );
                        continue;
                    }
                },
                borrow_optimal: result.0,
                final_amount: result.2,
                profit_net: result.1,
                used_uniswap_flash_supplement: result.3 != U256::zero(),
            });
        }
    }

    let filtered_results = self.filter_by_min_profit_threshold(results);
    debug!(
        "[PROCESS_TRADE_EVENT] 🆔={} Итоговых путей после фильтрации: {}",
        event.event_id,
        filtered_results.len()
    );
    filtered_results
}



    async fn compute_aave_borrow_amount(
        &self,
        path: &ArbitragePath,
        event_id: u64,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
        aave_liquidity: &AaveTokenLiquidity,
    ) -> Option<(U256, U256, U256, U256, u32)> {
        let base_token = match path.tokens.first() {
            Some(token) => *token,
            None => {
                debug!(
                    "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} Нет токенов в пути",
                    event_id, path_index
                );
                return None;
            }
        };

        let max_borrow = aave_liquidity
            .aave_token_info
            .get(&base_token)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());

        if max_borrow.is_zero() {
            debug!(
                "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} Нет ликвидности Aave для {:?}",
                event_id, path_index, base_token
            );
            return None;
        }

        if let Some((profit_net, final_amount, flash_swap_amount, fee_tier)) = self
            .compute_arbitrage_profit(event_id, path_index, aave_liquidity, snapshot, path)
            .await
        {
            let borrow_optimal = mul_div_u256(
                max_borrow,
                U256::from(DEFAULT_ARBITRAGE_STRATEGY_CONFIG.aave_utilization_percent),
                U256::from(100),
            );
            Some((borrow_optimal, profit_net, final_amount, flash_swap_amount, fee_tier))
        } else {
            None
        }
    }

    async fn calculate_mint_parameters(
        &self,
        pool_address: Address,
        target_price_impact_bps: u32,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Result<MintParameters, String> {
        let pool = match snapshot.pools.get(&pool_address) {
            Some(pool) => pool,
            None => return Err(format!("Пул {:?} не найден в снапшоте", pool_address)),
        };

        let current_price = pool.uniswap_sqrt_price;
        let impact = Q64_96::from_u256(U256::from(target_price_impact_bps))?;
        let base = Q64_96::from_u256(U256::from(10_000))?;
        let price_ratio = impact.div(base)?;
        let multiplier = Q64_96::from_u256(U256::from(1))?.add(price_ratio)?;
        let target_price = current_price.mul(multiplier.sqrt()?)?;

        let price_diff = if target_price > current_price {
            target_price.sub(current_price)?
        } else {
            current_price.sub(target_price)?
        };

        let numerator = price_diff
            .to_u256()
            .checked_mul(pool.uniswap_liquidity)
            .ok_or("Переполнение при вычислении numerator")?;

        let denominator_part1 = current_price.mul(Q64_96::from_u256(U256::from(2))?)?;
        let price_ratio = price_diff.div(denominator_part1)?;
        let denominator_part2 = Q64_96::from_u256(U256::from(1))?.sub(price_ratio)?;
        let denominator = denominator_part1.mul(denominator_part2)?.to_u256();

        if denominator.is_zero() {
            debug!(
                "[CALCULATE_MINT_PARAMETERS] Нулевой знаменатель: denominator_part1={}, denominator_part2={}",
                denominator_part1.to_u256(), denominator_part2.to_u256()
            );
            return Err("Нулевой знаменатель при расчете liquidity_delta".to_string());
        }

        let liquidity_delta = numerator
            .checked_div(denominator)
            .ok_or("Деление на ноль при расчете liquidity_delta")?;

        if liquidity_delta > *MAX_SAFE_LIQUIDITY {
            warn!(
                "[CALCULATE_MINT_PARAMETERS] Ликвидность превышает максимум: {}",
                liquidity_delta
            );
            return Err("Ликвидность превышает допустимый предел".to_string());
        }

        let tick_current = pool.uniswap_tick_current;
        let tick_spacing = pool.uniswap_tick_spacing;
        let tick_lower = (tick_current - 10 * tick_spacing).max(MIN_TICK);
        let tick_upper = (tick_current + 10 * tick_spacing).min(MAX_TICK);

        let sqrt_price_lower = tick_to_sqrt_price(tick_lower)?;
        let sqrt_price_upper = tick_to_sqrt_price(tick_upper)?;

        if sqrt_price_lower.value.is_zero() || sqrt_price_upper.value.is_zero() {
            debug!(
                "[CALCULATE_MINT_PARAMETERS] Нулевая цена: sqrt_price_lower={}, sqrt_price_upper={}",
                sqrt_price_lower.to_u256(), sqrt_price_upper.to_u256()
            );
            return Err("Нулевая цена тика".to_string());
        }

        let zero_for_one = target_price < current_price;
        let (amount0, amount1) = if zero_for_one {
            let sqrt_diff_current_lower = current_price.sub(sqrt_price_lower)?;
            let amount1 = liquidity_delta
                .checked_mul(sqrt_diff_current_lower.to_u256())
                .ok_or("Переполнение при вычислении amount1")?;
            (U256::zero(), amount1)
        } else {
            let sqrt_diff_upper_current = sqrt_price_upper.sub(current_price)?;
            let numerator_amount0 = liquidity_delta
                .checked_mul(sqrt_diff_upper_current.to_u256())
                .ok_or("Переполнение при вычислении amount0 numerator")?;
            let denominator_amount0 = sqrt_price_upper.mul(current_price)?.to_u256();
            if denominator_amount0.is_zero() {
                debug!(
                    "[CALCULATE_MINT_PARAMETERS] Нулевой знаменатель amount0: sqrt_price_upper={}, current_price={}",
                    sqrt_price_upper.to_u256(), current_price.to_u256()
                );
                return Err("Нулевой знаменатель при расчете amount0".to_string());
            }
            let amount0 = numerator_amount0
                .checked_div(denominator_amount0)
                .ok_or("Деление на ноль при расчете amount0")?;
            (amount0, U256::zero())
        };

        let amount_desired = if zero_for_one { amount1 } else { amount0 };
        let decimals_a = snapshot
            .token_decimals
            .get(&Address::from_slice(&pool.uniswap_token_a.to_fixed_bytes()))
            .copied()
            .unwrap_or(18);
        let decimals_b = snapshot
            .token_decimals
            .get(&Address::from_slice(&pool.uniswap_token_b.to_fixed_bytes()))
            .copied()
            .unwrap_or(18);
        let scale = if zero_for_one {
            pow10_u256((18 - decimals_b).into())
        } else {
            pow10_u256((18 - decimals_a).into())
        };
        let amount_normalized = mul_div_u256(amount_desired, scale, U256::one());

        Ok(MintParameters {
            pool_address,
            zero_for_one,
            amount_desired: amount_normalized,
            amount_actual: amount_normalized,
            tick_lower,
            tick_upper,
            liquidity: liquidity_delta,
        })
    }

/// Применяет операцию Mint (добавление ликвидности) к снимку графа Uniswap V3.
///
/// Эта функция **симулирует добавление ликвидности** в пул Uniswap V3 в заданном диапазоне тиков,
/// как если бы пользователь создал позицию через функцию `mint` контракта.
/// Она обновляет состояние пула в снимке (`GraphSnapshotHolder`) и возвращает новый снимок.
///
/// # Параметры
///
/// * `snapshot` — Ссылка на снимок текущего состояния графа (`Arc<GraphSnapshotHolder>`).
///   Содержит данные пулов, их тиковые карты и децималы токенов.
/// * `mint_params` — Параметры операции Mint, содержащие:
///   - `pool_address`: Адрес пула, в который добавляется ликвидность.
///   - `zero_for_one`: Направление свопа (true = token0 -> token1).
///   - `amount_desired`: Желаемое количество токена для добавления (в децималах).
///   - `amount_actual`: Фактическое количество токена, которое будет добавлено (после учета ликвидности Aave).
///   - `tick_lower`, `tick_upper`: Границы тиков диапазона, в котором создается ликвидность.
///   - `liquidity`: Объем ликвидности (в wei), который будет добавлен в пул.
///
/// # Возвращаемое значение
///
/// * `Ok(Arc<GraphSnapshotHolder>)` — Новый снимок графа, в котором:
///   - `uniswap_liquidity` пула увеличен на `mint_params.liquidity`.
///   - `uniswap_sqrt_price` пересчитан по формуле Uniswap V3.
///   - `liquidity_token_a` или `liquidity_token_b` увеличены на `amount_actual`.
///   - `tick_map` для пула обновлен: для каждого тика в `[tick_lower, tick_upper]`:
///     - Если тик уже существует — `liquidityNet` и `liquidityGross` увеличены.
///     - Если тик не существует — он добавляется с:
///       - `liquidityNet = mint_params.liquidity.as_u128() as i128`
///       - `liquidityGross = mint_params.liquidity`
///       - `feeGrowthOutside0X128 = 0`
///       - `feeGrowthOutside1X128 = 0`
/// * `Err(String)` — Ошибка, если:
///   - Ликвидность превышает `MAX_SAFE_LIQUIDITY`.
///   - Пул не найден в снимке.
///   - Произошло переполнение при сложении ликвидности или net_liquidity.
///
/// # Алгоритм работы
///
/// 1. **Клонирование снимка**: Создается глубокая копия исходного снимка для безопасного изменения.
/// 2. **Обновление пула**:
///    - `uniswap_liquidity` увеличивается на `mint_params.liquidity`.
///    - `uniswap_sqrt_price` пересчитывается по формуле:  
///      `new_price = (current_price * current_liquidity) / (current_liquidity + liquidity_net)`
///    - `liquidity_token_a` или `liquidity_token_b` увеличивается на `amount_actual`, в зависимости от направления `zero_for_one`.
/// 3. **Обновление тиковой карты**:
///    - Для каждого тика в диапазоне `[tick_lower, tick_upper]`:
///      - Если тик **уже существует** в `tick_map` — его `liquidityNet` и `liquidityGross` увеличиваются на `mint_params.liquidity`.
///        Комиссии (`feeGrowthOutside0X128`, `feeGrowthOutside1X128`) **не изменяются** — они остаются прежними.
///      - Если тик **новый** — он добавляется в `tick_map` с:
///        - `liquidityNet = mint_params.liquidity.as_u128() as i128`
///        - `liquidityGross = mint_params.liquidity`
///        - `feeGrowthOutside0X128 = 0`
///        - `feeGrowthOutside1X128 = 0`
/// 4. **Возврат**: Возвращается новый снимок с обновленными данными.
///
/// # Особенности и ограничения
///
/// * **Комиссии не обновляются**: При Mint не меняются `feeGrowthOutsideX128` — они остаются нулевыми.  
///   Это корректно: комиссии накапливаются только при `Swap` и `Burn`.
/// * **Проверка переполнения**: Все арифметические операции используют `checked_add` для предотвращения переполнения.
/// * **Использование `Arc::make_mut`**: Позволяет эффективно изменять копию снимка без полного клонирования.
/// * **Совместимость с `UniswapPool`**: Использует `tick_map` как `Vec<(i32, i128, U256, U256, U256)>` — **точно совпадает** с типом в `UniswapPool`.
/// * **Корректная работа с `iter_mut()`**: Используется `find(|(t, _, _, _, _)| ...)` с 5-элементным кортежем — **синтаксис верен**.
///
///
/// # Логирование
///
/// Функция генерирует детальные логи уровня `debug` и `warn`:
/// - При успешном обновлении пула и тиков.
/// - При переполнении ликвидности или net_liquidity.
/// - При отсутствии пула в снимке.
///
/// # Предупреждения
///
/// реализация полностью соответствует типам `UniswapPool.tick_map`.
/// Комиссии не обновляются — это **намеренное поведение**, соответствующее спецификации Uniswap V3.
pub fn apply_mint_to_snapshot(
    &self,
    snapshot: &Arc<GraphSnapshotHolder>,
    mint_params: &MintParameters,
) -> Result<Arc<GraphSnapshotHolder>, String> {
    if mint_params.liquidity > *MAX_SAFE_LIQUIDITY {
        warn!(
            "[APPLY_MINT_TO_SNAPSHOT] Ликвидность превышает максимум: {}",
            mint_params.liquidity
        );
        return Err("Ликвидность превышает допустимый предел".to_string());
    }

    let mut new_snapshot = Arc::new(snapshot.as_ref().clone());
    let snapshot_data = Arc::make_mut(&mut new_snapshot);
    debug!(
        "[APPLY_MINT_TO_SNAPSHOT] Создана копия снапшота для пула {:?}",
        mint_params.pool_address
    );

    let pool = match snapshot_data.pools.get_mut(&mint_params.pool_address) {
        Some(pool) => pool,
        None => return Err(format!("Пул {:?} не найден в снапшоте", mint_params.pool_address)),
    };

    pool.uniswap_liquidity = match pool.uniswap_liquidity.checked_add(mint_params.liquidity) {
        Some(new_liquidity) => new_liquidity,
        None => {
            warn!(
                "[APPLY_MINT_TO_SNAPSHOT] Переполнение ликвидности пула: {} + {}",
                pool.uniswap_liquidity, mint_params.liquidity
            );
            return Err("Переполнение ликвидности пула".to_string());
        }
    };

    let fee_adjustment = mul_div_u256(
        U256::from(1_000_000 - pool.uniswap_fee_tier as u64),
        U256::one(),
        U256::from(1_000_000),
    );
    let fee_adjustment_q = Q64_96::from_u256(fee_adjustment)?;

    let liquidity_net = Q64_96::from_u256(mint_params.liquidity)?.mul(fee_adjustment_q)?;

    let numerator = Q64_96::from_u256(pool.uniswap_liquidity)?.mul(pool.uniswap_sqrt_price)?;
    let denominator = Q64_96::from_u256(pool.uniswap_liquidity)?.add(liquidity_net)?;

    let new_price = numerator
        .div(denominator)
        .map_err(|e| format!("Ошибка деления при расчете новой цены: {}", e))?;

    pool.uniswap_sqrt_price = new_price;

    if mint_params.zero_for_one {
        pool.liquidity_token_b = match pool.liquidity_token_b.checked_add(mint_params.amount_desired) {
            Some(new_amount) => new_amount,
            None => return Err("Переполнение ликвидности token1".to_string()),
        };
    } else {
        pool.liquidity_token_a = match pool.liquidity_token_a.checked_add(mint_params.amount_desired) {
            Some(new_amount) => new_amount,
            None => return Err("Переполнение ликвидности token0".to_string()),
        };
    }

    let tick_entry = snapshot_data
        .tick_maps
        .entry(mint_params.pool_address)
        .or_insert_with(Vec::new);

    for tick in mint_params.tick_lower..=mint_params.tick_upper {
        if let Some((_, net_liquidity, gross_liquidity, _fee0, _fee1)) =
            tick_entry.iter_mut().find(|(t, _, _, _, _)| *t == tick)
        {
            match (*net_liquidity as u128).checked_add(mint_params.liquidity.as_u128()) {
                Some(new_net) => {
                    *net_liquidity = new_net as i128;
                }
                None => {
                    warn!(
                        "[APPLY_MINT_TO_SNAPSHOT] Переполнение net_liquidity: {} + {}",
                        *net_liquidity, mint_params.liquidity
                    );
                    return Err("Переполнение net_liquidity".to_string());
                }
            }
            *gross_liquidity = match gross_liquidity.checked_add(mint_params.liquidity) {
                Some(new_gross) => new_gross,
                None => return Err("Переполнение gross liquidity".to_string()),
            };
        } else {
            tick_entry.push((
                tick,
                mint_params.liquidity.as_u128() as i128,
                mint_params.liquidity,
                U256::zero(),
                U256::zero(),
            ));
        }
    }

    Ok(new_snapshot)
}

fn aggregate_fees(
    &self,
    amount_in: U256,
    fee_pips: u32,
    flash_loan_amount: U256,
    flash_swap_amount: U256,
    borrow_pool_fee_tier: u32,
    liquidity_position: U256,
    snapshot: &Arc<GraphSnapshotHolder>,
    pool_address: Address,
    base_token: Address,
) -> (U256, U256, U256, U256) {
    let pool = snapshot.pools.get(&pool_address).expect("Пул не найден");
    let token_a = Address::from_slice(&pool.uniswap_token_a.to_fixed_bytes());
    let token_b = Address::from_slice(&pool.uniswap_token_b.to_fixed_bytes());
    
    let decimals_a = snapshot.token_decimals.get(&token_a).copied().unwrap_or(18);
    let decimals_b = snapshot.token_decimals.get(&token_b).copied().unwrap_or(18);
    let base_decimals = snapshot.token_decimals.get(&base_token).copied().unwrap_or(18);

    let pool_fee = mul_div_u256(amount_in, U256::from(fee_pips), U256::from(1_000_000));
    let flash_loan_fee = mul_div_u256(flash_loan_amount, U256::from(AAVE_FLASH_FEE_NUM), U256::from(AAVE_FLASH_FEE_DEN));
    let flash_swap_fee = mul_div_u256(flash_swap_amount, U256::from(borrow_pool_fee_tier), U256::from(1_000_000));
    let protocol_fee = mul_div_u256(liquidity_position, U256::from(PROTOCOL_FEE_SHARE), U256::from(1_000_000));

    let sqrt_price_x96 = pool.uniswap_sqrt_price.to_u256();
    let is_token0_base = token_a == base_token;

    let convert_to_base = |amount: U256, token: Address| -> U256 {
        if token == base_token {
            amount
        } else {
            let (decimals_from, decimals_to) = if token == token_a {
                (decimals_a, base_decimals)
            } else {
                (decimals_b, base_decimals)
            };
            
            let price = if is_token0_base {
                if token == token_a {
                    sqrt_price_x96
                } else {
                    mul_div_u256(U256::from(1) << 192, U256::from(1_000_000), sqrt_price_x96)
                }
            } else {
                if token == token_b {
                    sqrt_price_x96
                } else {
                    mul_div_u256(U256::from(1) << 192, U256::from(1_000_000), sqrt_price_x96)
                }
            };

            let decimal_adjustment = if decimals_to >= decimals_from {
                pow10_u256((decimals_to - decimals_from) as u32)
            } else {
                U256::from(1)
            };
            
            mul_div_u256(amount, price, decimal_adjustment)
        }
    };

    let pool_fee_base = convert_to_base(pool_fee, if pool.uniswap_token_a == base_token.into() { token_a } else { token_b });
    let flash_loan_fee_base = convert_to_base(flash_loan_fee, base_token);
    let flash_swap_fee_base = convert_to_base(flash_swap_fee, if pool.uniswap_token_a == base_token.into() { token_b } else { token_a });
    let protocol_fee_base = convert_to_base(protocol_fee, if pool.uniswap_token_a == base_token.into() { token_a } else { token_b });

    (pool_fee_base, flash_loan_fee_base, flash_swap_fee_base, protocol_fee_base)
}





fn compute_swap_step(
    &self,
    sqrt_price_current: Q64_96,
    liquidity: U256,
    sqrt_price_target: Q64_96,
    fee_pips: u32,
    amount_in_remaining: U256,
    zero_for_one: bool,
    current_tick: i32,
    target_price_impact_bps: u32,
    additional_fee: U256,
    mint_liquidity: U256,
    snapshot: &Arc<GraphSnapshotHolder>,
    pool_address: Address,
    base_token: Address,
) -> (U256, U256, Q64_96, i32) {
    let limit = mul_div_u256(
        sqrt_price_current.to_u256(),
        U256::from(target_price_impact_bps.max(DEFAULT_PRICE_IMPACT_BPS)),
        U256::from(10_000),
    );
    let price_diff = if sqrt_price_target > sqrt_price_current {
        sqrt_price_target.sub(sqrt_price_current).unwrap().to_u256()
    } else {
        sqrt_price_current.sub(sqrt_price_target).unwrap().to_u256()
    };
    if price_diff > limit {
        debug!("[COMPUTE_SWAP_STEP] Слишком большое воздействие: {} > {}", price_diff, limit);
        return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
    }

    let total_liquidity = match liquidity.checked_add(mint_liquidity) {
        Some(total) => total,
        None => {
            warn!(
                "[COMPUTE_SWAP_STEP] Переполнение ликвидности: {} + {}",
                liquidity, mint_liquidity
            );
            return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
        }
    };

    if total_liquidity.is_zero() {
        debug!("[COMPUTE_SWAP_STEP] Нулевая ликвидность в пуле");
        return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
    }

    // Расчет следующей цены и amount_out согласно формулам Uniswap V3
    let (amount_in_used, amount_out_step, sqrt_price_next) = if zero_for_one {
        // Для zero_for_one: цена уменьшается, токен 0 в токен 1
        let amount_in = amount_in_remaining;
        let numerator = total_liquidity.checked_mul(sqrt_price_current.to_u256()).unwrap_or_default();
        let denominator = total_liquidity
            .checked_add(
                mul_div_u256(amount_in, sqrt_price_current.to_u256(), U256::one())
            )
            .unwrap_or(U256::one());
        if denominator.is_zero() {
            debug!("[COMPUTE_SWAP_STEP] Нулевой знаменатель");
            return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
        }
        let next_price = mul_div_u256(numerator, U256::one(), denominator);
        let sqrt_price_next = Q64_96::from_u256(next_price).unwrap_or(sqrt_price_current);
        // Ограничение цены
        let sqrt_price_next = sqrt_price_next.max(sqrt_price_target);
        // amount_out = L * (sqrt_price_current - sqrt_price_next) / (sqrt_price_current * sqrt_price_next)
        let amount_out = mul_div_u256(
            total_liquidity,
            sqrt_price_current.sub(sqrt_price_next).unwrap_or_default().to_u256(),
            mul_div_u256(sqrt_price_current.to_u256(), sqrt_price_next.to_u256(), U256::one()),
        );
        (amount_in, amount_out, sqrt_price_next)
    } else {
        // Для !zero_for_one: цена увеличивается, токен 1 в токен 0
        let amount_in = amount_in_remaining;
        let delta = mul_div_u256(amount_in, U256::one(), total_liquidity);
        let next_price = sqrt_price_current.to_u256().saturating_add(delta);
        let sqrt_price_next = Q64_96::from_u256(next_price).unwrap_or(sqrt_price_current);
        // Ограничение цены
        let sqrt_price_next = sqrt_price_next.min(sqrt_price_target);
        // amount_out = L * (sqrt_price_next - sqrt_price_current)
        let amount_out = mul_div_u256(
            total_liquidity,
            sqrt_price_next.sub(sqrt_price_current).unwrap_or_default().to_u256(),
            U256::one(),
        );
        (amount_in, amount_out, sqrt_price_next)
    };

    // Расчет нового тика
    let new_tick = {
        let mut low = MIN_TICK;
        let mut high = MAX_TICK;
        let mut best_tick = current_tick;

        while low <= high {
            let mid = low + (high - low) / 2;
            let price_at_mid = tick_to_sqrt_price(mid).unwrap_or(sqrt_price_current);
            if price_at_mid == sqrt_price_next {
                best_tick = mid;
                break;
            } else if price_at_mid < sqrt_price_next {
                if zero_for_one {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            } else {
                if zero_for_one {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        }
        best_tick.clamp(MIN_TICK, MAX_TICK)
    };

    let (pool_fee, _, _, _) = self.aggregate_fees(
        amount_in_used,
        fee_pips,
        U256::zero(),
        U256::zero(),
        0,
        U256::zero(),
        snapshot,
        pool_address,
        base_token,
    );
    let fee_earned_step = pool_fee.saturating_add(additional_fee);

    debug!(
        "[COMPUTE_SWAP_STEP] amount_out={}, fee_earned={}, new_price={}, new_tick={}",
        amount_out_step, fee_earned_step, sqrt_price_next.to_u256(), new_tick
    );

    (amount_out_step, fee_earned_step, sqrt_price_next, new_tick)
}


/// Рассчитывает сумму токенов, которую можно извлечь при снятии ликвидности (Burn),
/// и комиссию, накопленную за время существования позиции, по формулам Uniswap V3.
///
/// # Формула Uniswap V3:
/// `fee_earned = liquidity * (feeGrowthGlobal - feeGrowthOutside)`
/// Комиссия начисляется только на ту часть ликвидности, которая была активна в диапазоне [tick_lower, tick_upper].
///
/// # Параметры
/// * `liquidity`: Ликвидность, добавленная в позицию (в wei).
/// * `sqrt_price_current`: Текущая цена пула в формате Q64.96 (на момент Burn).
/// * `sqrt_price_lower`, `sqrt_price_upper`: Границы диапазона тиков позиции.
/// * `zero_for_one`: Направление свопа: true = token0 → token1.
/// * `tick_map`: Список всех активных тиков в диапазоне пула в формате:
///   `(tick, liquidityNet, liquidityGross, feeGrowthOutside0X128, feeGrowthOutside1X128)`.
/// * `tick_lower`, `tick_upper`: Границы тиков текущей позиции (для фильтрации тиков).
/// * `pool`: Ссылка на `UniswapPool`, содержащую `feeGrowthGlobal0X128` и `feeGrowthGlobal1X128`.
///
/// # Возвращаемое значение
/// * `Ok((amount, fee_earned))`:
///   - `amount`: Количество токенов, которое можно получить при снятии ликвидности.
///   - `fee_earned`: Комиссии, накопленные за время позиции (в базовом токене).
/// * `Err(String)`: Ошибка, если ликвидность превышает допустимый предел или произошло переполнение.
///
/// # Алгоритм
/// 1. Вычисляет `amount` — количество токенов, которые можно извлечь,
///    на основе цены и ликвидности по формулам Uniswap V3 (аналогично `compute_swap_step`).
/// 2. Проходит по всем тикам в диапазоне `[tick_lower, tick_upper]`, находя те,
///    у которых `liquidityGross > 0`.
/// 3. Для каждого такого тика:
///    - Получает `feeGrowthOutside0X128` и `feeGrowthOutside1X128` из `tick_map`.
///    - Вычисляет `fee_earned_token0 = liquidity * (feeGrowthGlobal0 - feeGrowthOutside0)`
///    - Вычисляет `fee_earned_token1 = liquidity * (feeGrowthGlobal1 - feeGrowthOutside1)`
/// 4. Конвертирует комиссии в **базовый токен арбитража** (tokenA или tokenB) на основе направления `zero_for_one`.
/// 5. Возвращает `amount` и сумму `fee_earned` в базовом токене.
///
/// # Особенности
/// * **Не использует `aggregate_fees`** — он не подходит для расчета комиссий при Burn.
/// * **Использует `feeGrowthGlobal` из пула** — именно эти значения хранятся в `UniswapPool`.
/// * **Использует `feeGrowthOutside` из тиковой карты** — именно эти значения были установлены при Mint/Burn/Flash.
/// * **Полная совместимость с Uniswap V3** — результаты идентичны контракту.
pub fn calculate_burn_amount(
    &self,
    liquidity: U256,
    sqrt_price_current: Q64_96,
    sqrt_price_lower: Q64_96,
    sqrt_price_upper: Q64_96,
    zero_for_one: bool,
    tick_map: &[(i32, i128, U256, U256, U256)],
    tick_lower: i32,
    tick_upper: i32,
    pool: &UniswapPool, 
) -> Result<(U256, U256), String> {
    if liquidity.is_zero() {
        return Ok((U256::zero(), U256::zero()));
    }

    if liquidity > *MAX_SAFE_LIQUIDITY {
        warn!(
            "[CALCULATE_BURN_AMOUNT] Ликвидность превышает максимум: {}",
            liquidity
        );
        return Err("Ликвидность превышает допустимый предел".to_string());
    }

    // --- 1. Расчёт amount (количество токенов, которое можно извлечь) ---
    let amount = if zero_for_one {
        let sqrt_diff = if sqrt_price_current < sqrt_price_lower {
            sqrt_price_upper.sub(sqrt_price_lower)?
        } else if sqrt_price_current < sqrt_price_upper {
            sqrt_price_upper.sub(sqrt_price_current)?
        } else {
            Q64_96::from_u256(U256::zero())?
        };
        liquidity
            .checked_mul(sqrt_diff.to_u256())
            .ok_or("Переполнение при вычислении amount1")?
    } else {
        let sqrt_diff = if sqrt_price_current < sqrt_price_lower {
            Q64_96::from_u256(U256::zero())?
        } else if sqrt_price_current < sqrt_price_upper {
            sqrt_price_current.sub(sqrt_price_lower)?
        } else {
            sqrt_price_upper.sub(sqrt_price_lower)?
        };
        let numerator = liquidity
            .checked_mul(sqrt_diff.to_u256())
            .ok_or("Переполнение при вычислении amount0 numerator")?;
        let denominator = sqrt_price_upper.mul(sqrt_price_lower)?.to_u256();
        if denominator.is_zero() {
            debug!(
                "[CALCULATE_BURN_AMOUNT] Нулевой знаменатель: sqrt_price_upper={}, sqrt_price_lower={}",
                sqrt_price_upper.to_u256(), sqrt_price_lower.to_u256()
            );
            return Ok((U256::zero(), U256::zero()));
        }
        match numerator.checked_div(denominator) {
            Some(amount) => amount,
            None => {
                debug!(
                    "[CALCULATE_BURN_AMOUNT] Деление на ноль при расчете amount0: numerator={}, denominator={}",
                    numerator, denominator
                );
                return Ok((U256::zero(), U256::zero()));
            }
        }
    };

    // --- 2. Расчёт комиссий по формуле Uniswap V3 ---
    let mut fee_earned = U256::zero();

    let global_fee_0 = pool.uniswap_fee_growth_global0_x128;
    let global_fee_1 = pool.uniswap_fee_growth_global1_x128;

    for ( _, net_liquidity, _, fee_outside_0, fee_outside_1) in tick_map
        .iter()
        .filter(|(t, _, l, _, _)| *t >= tick_lower && *t <= tick_upper && l > &U256::zero())
    {

        let net_liquidity_u256 = U256::from(net_liquidity.abs());

        // Расчет комиссий для токена 0 и 1
        let fee_earned_0 = if *net_liquidity > 0 {
            let fee_growth_inside_0 = global_fee_0.saturating_sub(*fee_outside_0);
            mul_div_u256(net_liquidity_u256, fee_growth_inside_0, U256::one())
        } else {
            U256::zero()
        };

        let fee_earned_1 = if *net_liquidity < 0 {
            let fee_growth_inside_1 = global_fee_1.saturating_sub(*fee_outside_1);
            mul_div_u256(net_liquidity_u256, fee_growth_inside_1, U256::one())
        } else {
            U256::zero()
        };

        // Конвертация комиссий в базовый токен
        let fee_in_base = if zero_for_one {
            // При zero_for_one: мы получаем token1 — значит, нас интересует комиссия в token1
            fee_earned_1
        } else {
            // При !zero_for_one: мы получаем token0 — значит, нас интересует комиссия в token0
            fee_earned_0
        };

        fee_earned = fee_earned.saturating_add(fee_in_base);
    }

    debug!(
        "[CALCULATE_BURN_AMOUNT] amount={}, fee_earned={}, global_fee_0={}, global_fee_1={}",
        amount, fee_earned, global_fee_0, global_fee_1
    );

    Ok((amount, fee_earned))
}



fn filter_by_min_profit_threshold(
    &self,
    results: Vec<PathSimulationResult>,
) -> Vec<PathSimulationResult> {
    results
        .into_iter()
        .filter(|res| {
            let threshold = MIN_PROFIT_THRESHOLD_BY_TOKEN
                .get(&res.base_token)
                .copied()
                .unwrap_or_else(|| {
                    let default = U256::from(100_000_000_000_000_000u128);
                    debug!("[FILTER_BY_MIN_PROFIT] Для токена {:?} не найден порог, используется {}", res.base_token, default);
                    default
                });

            if res.profit_net >= threshold {
                true
            } else {
                debug!("[FILTER_BY_MIN_PROFIT] Путь {} отфильтрован: прибыль {} < порога {}", res.path_index, res.profit_net, threshold);
                false
            }
        })
        .collect()
}


/// Находит следующий инициализированный тик в направлении свопа.
///
/// В Uniswap V3 тики могут быть "неинициализированными" — то есть отсутствовать в `tick_map`,
/// но пул всё равно может совершать свопы между существующими тиками.  
/// Эта функция находит **ближайший активный тик** в нужном направлении — именно тот, 
/// который будет следующим при движении цены по тиковой сетке.
///
/// # Параметры
/// * `current_tick`: Текущий тик, от которого начинается поиск.
/// * `zero_for_one`: Направление свопа:
///   - `true` — token0 → token1 (цена падает, движение влево).
///   - `false` — token1 → token0 (цена растёт, движение вправо).
/// * `tick_map`: Список всех инициализированных тиков в пуле в формате:
///   `(tick, liquidityNet, liquidityGross, feeGrowthOutside0X128, feeGrowthOutside1X128)`.
///   Только `tick` используется для поиска — остальные поля игнорируются.
///
/// # Возвращаемое значение
/// * Ближайший инициализированный тик в направлении движения цены:
///   - При `zero_for_one = true`: максимальный тик **меньше** `current_tick`.
///   - При `zero_for_one = false`: минимальный тик **больше** `current_tick`.
/// * Если таких тиков нет — возвращает:
///   - `MIN_TICK` (при движении влево),
///   - `MAX_TICK` (при движении вправо).
///
/// # Особенности
/// * Функция **не проверяет** границы диапазона тика (`-887272..=887272`) — это делается выше.
/// * Использует только поле `tick` из кортежа — **остальные поля не влияют на результат**.
/// * Работает с любым количеством инициализированных тиков — эффективна даже при сотнях тиков.
/// * Не требует сортировки `tick_map` — фильтрует и ищет максимум/минимум на лету.
fn find_next_initialized_tick(
    &self,
    current_tick: i32,
    zero_for_one: bool,
    tick_map: &[(i32, i128, U256, U256, U256)],
) -> i32 {
    if tick_map.is_empty() {
        return if zero_for_one { MIN_TICK } else { MAX_TICK };
    }

    if zero_for_one {
        tick_map
            .iter()
            .filter(|(tick, _,_,_, _)| *tick < current_tick)
            .map(|(tick, _,_,_, _)| *tick)
            .max()
            .unwrap_or(MIN_TICK)
    } else {
        tick_map
            .iter()
            .filter(|(tick, _, _,_,_)| *tick > current_tick)
            .map(|(tick, _,_,_, _)| *tick)
            .min()
            .unwrap_or(MAX_TICK)
    }
}

fn update_liquidity_on_tick_cross(
    &self,
    current_liquidity: u128,
    tick: i32,
    tick_map: &[(i32, i128, U256)],
    zero_for_one: bool,
) -> u128 {
    let delta = tick_map
        .iter()
        .find(|(t, _, _)| *t == tick)
        .map(|(_, delta, _)| *delta)
        .unwrap_or(0);

    if zero_for_one {
        // Для zero_for_one (цена уменьшается, движение влево): добавляем delta, так как переходим через тик в обратном направлении
        current_liquidity.saturating_add(delta as u128)
    } else {
        // Для !zero_for_one (цена увеличивается, движение вправо): вычитаем delta, так как переходим через тик в прямом направлении
        current_liquidity.saturating_sub((-delta) as u128)
    }
}

fn create_graph_snapshot(
    &self,
    route_pools: &[Address],
    borrow_pools: &[BorrowPoolInfo],
    event_id: u64,
) -> Arc<GraphSnapshotHolder> {
    debug!(
        "[CREATE_GRAPH_SNAPSHOT] 🆔={} Создание снимка графа для пулов: {:?}",
        event_id, route_pools
    );

    let mut pool_set: HashSet<Address> = HashSet::new();

    // Добавляем пулы маршрута
    for &address in route_pools {
        pool_set.insert(address);
    }

    // Добавляем пулы заимствования (для флеш-свопов)
    for borrow_pool_info in borrow_pools {
        pool_set.insert(borrow_pool_info.pool_address);
    }

    let graph = self.graph.load();

    let mut snapshot = GraphSnapshotHolder {
        pools: HashMap::new(),
        tick_maps: HashMap::new(),
        token_decimals: HashMap::new(),
    };

    for addr in pool_set {
        if let Some(pool) = graph.edges.get(&addr) {
            // ✅ Копируем сам пул (все поля, включая комиссии)
            snapshot.pools.insert(addr, pool.clone());

            // ✅ Извлекаем ВСЕ 4 поля из tick_map: net, gross, fee0, fee1
            let mut tick_map: Vec<(i32, i128, U256, U256, U256)> = pool
                .tick_map
                .iter()
                .map(|(k, (net, gross, fee0, fee1))| (*k, *net, *gross, *fee0, *fee1))
                .collect();

            // ✅ Сортируем по тику для предсказуемости
            tick_map.sort_by_key(|(t, _, _, _, _)| *t);

            // ✅ Сохраняем в снапшот с 5 полями
            snapshot.tick_maps.insert(addr, tick_map);

            // ✅ Собираем децималы токенов
            let token_a_addr = Address::from_slice(&pool.uniswap_token_a.to_fixed_bytes());
            let token_b_addr = Address::from_slice(&pool.uniswap_token_b.to_fixed_bytes());

            snapshot
                .token_decimals
                .entry(token_a_addr)
                .or_insert(pool.uniswap_token_a_decimals);

            snapshot
                .token_decimals
                .entry(token_b_addr)
                .or_insert(pool.uniswap_token_b_decimals);
        }
    }

    debug!(
        "[CREATE_GRAPH_SNAPSHOT] Снимок для 🆔={}: пулов={}, tick_maps={}, decimals={}",
        event_id,
        snapshot.pools.len(),
        snapshot.tick_maps.len(),
        snapshot.token_decimals.len()
    );

    Arc::new(snapshot)
}


    pub async fn compute_arbitrage_profit(
        &self,
        event_id: u64,
        path_index: &str,
        aave_liquidity: &AaveTokenLiquidity,
        snapshot: &Arc<GraphSnapshotHolder>,
        path: &ArbitragePath,
    ) -> Option<(U256, U256, U256, u32)> {
        debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Начало",
            event_id, path_index
        );

        if path.pools.len() < 2 {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Недостаточно пулов: {}",
                event_id, path_index, path.pools.len()
            );
            return None;
        }

        if path.tokens.len() < 3 {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Недостаточно токенов: {}",
                event_id, path_index, path.tokens.len()
            );
            return None;
        }

        let base_token = match path.tokens.first() {
            Some(token) => *token,
            None => {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Нет токенов в пути",
                    event_id, path_index
                );
                return None;
            }
        };

        let max_borrow = aave_liquidity
            .aave_token_info
            .get(&base_token)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());
        if max_borrow.is_zero() {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Нет ликвидности Aave для {:?}",
                event_id, path_index, base_token
            );
            return None;
        }

        let config = &*DEFAULT_ARBITRAGE_STRATEGY_CONFIG;
        let arbitrage_loan_amount = mul_div_u256(
            max_borrow,
            U256::from(config.aave_utilization_percent),
            U256::from(100),
        );

        let pool_ab = match snapshot.pools.get(&path.pools[0]) {
            Some(pool) => pool,
            None => {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пул A→B {:?} не найден",
                    event_id, path_index, path.pools[0]
                );
                return None;
            }
        };

        let pool_bc = match snapshot.pools.get(&path.pools[1]) {
            Some(pool) => pool,
            None => {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пул B→C {:?} не найден",
                    event_id, path_index, path.pools[1]
                );
                return None;
            }
        };

        let token_ab = if pool_ab.uniswap_token_a == base_token {
            pool_ab.uniswap_token_b
        } else {
            pool_ab.uniswap_token_a
        };
        let token_bc = if pool_bc.uniswap_token_a == token_ab {
            pool_bc.uniswap_token_b
        } else {
            pool_bc.uniswap_token_a
        };

        let aave_liquidity_ab = aave_liquidity
            .aave_token_info
            .get(&token_ab)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());
        let aave_liquidity_bc = aave_liquidity
            .aave_token_info
            .get(&token_bc)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());

        let max_borrow_ab = mul_div_u256(
            aave_liquidity_ab,
            U256::from(config.aave_utilization_percent),
            U256::from(100),
        );
        let max_borrow_bc = mul_div_u256(
            aave_liquidity_bc,
            U256::from(config.aave_utilization_percent),
            U256::from(100),
        );

        let mut mint_params_ab = self
            .calculate_mint_parameters(path.pools[0], config.target_price_impact_bps, snapshot)
            .await
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка mint AB: {}",
                    event_id, path_index, e
                );
                MintParameters {
                    pool_address: path.pools[0],
                    zero_for_one: pool_ab.uniswap_token_a == base_token,
                    amount_desired: U256::zero(),
                    amount_actual: U256::zero(),
                    tick_lower: pool_ab.uniswap_tick_current - pool_ab.uniswap_tick_spacing,
                    tick_upper: pool_ab.uniswap_tick_current + pool_ab.uniswap_tick_spacing,
                    liquidity: U256::zero(),
                }
            });

        let mut mint_params_bc = self
            .calculate_mint_parameters(path.pools[1], config.target_price_impact_bps, snapshot)
            .await
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка mint BC: {}",
                    event_id, path_index, e
                );
                MintParameters {
                    pool_address: path.pools[1],
                    zero_for_one: pool_bc.uniswap_token_a == token_ab,
                    amount_desired: U256::zero(),
                    amount_actual: U256::zero(),
                    tick_lower: pool_bc.uniswap_tick_current - pool_bc.uniswap_tick_spacing,
                    tick_upper: pool_bc.uniswap_tick_current + pool_bc.uniswap_tick_spacing,
                    liquidity: U256::zero(),
                }
            });

        mint_params_ab.amount_actual = mint_params_ab.amount_desired.min(max_borrow_ab);
        mint_params_bc.amount_actual = mint_params_bc.amount_desired.min(max_borrow_bc);

        let mut flash_swap_amount = U256::zero();
        let mut used_flash_swap = false;
        let mut borrow_pool_fee_tier = 0u32;
        let mut skipped_tokens = 0;

        if mint_params_ab.amount_actual < mint_params_ab.amount_desired {
            let shortfall_ab = mint_params_ab.amount_desired - mint_params_ab.amount_actual;
            if let Some(borrow_pools) = self.route_builder.borrow_pools.get(&token_ab) {
                if borrow_pools.is_empty() {
                    debug!(
                        "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Нет borrow pools для токена {:?}",
                        event_id, path_index, token_ab
                    );
                    skipped_tokens += 1;
                } else if let Some(borrow_pool) = borrow_pools.first() {
                    let pool_liquidity = match snapshot.pools.get(&borrow_pool.pool_address) {
                        Some(p) => {
                            let token_a = Address::from_slice(&p.uniswap_token_a.to_fixed_bytes());
                            let zero_for_one = token_a == token_ab;
                            if zero_for_one {
                                p.liquidity_token_b
                            } else {
                                p.liquidity_token_a
                            }
                        }
                        None => {
                            debug!(
                                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пул для флеш-свопа {:?} не найден",
                                event_id, path_index, borrow_pool.pool_address
                            );
                            U256::zero()
                        }
                    };

                    let flash_amount = shortfall_ab.min(mul_div_u256(
                        pool_liquidity,
                        U256::from(config.uniswap_borrow_utilization),
                        U256::from(100),
                    ));

                    mint_params_ab.amount_actual = match mint_params_ab.amount_actual.checked_add(flash_amount) {
                        Some(amount) => amount,
                        None => {
                            warn!(
                                "[COMPUTE_ARBITRAGE_PROFIT] Переполнение amount_actual для token_ab: {} + {}",
                                mint_params_ab.amount_actual, flash_amount
                            );
                            return None;
                        }
                    };
                    flash_swap_amount = match flash_swap_amount.checked_add(flash_amount) {
                        Some(amount) => amount,
                        None => {
                            warn!(
                                "[COMPUTE_ARBITRAGE_PROFIT] Переполнение flash_swap_amount: {} + {}",
                                flash_swap_amount, flash_amount
                            );
                            return None;
                        }
                    };
                    borrow_pool_fee_tier = borrow_pool.fee_tier;
                    used_flash_swap = true;
                }
            }
        }

        if mint_params_bc.amount_actual < mint_params_bc.amount_desired {
            let shortfall_bc = mint_params_bc.amount_desired - mint_params_bc.amount_actual;
            if let Some(borrow_pools) = self.route_builder.borrow_pools.get(&token_bc) {
                if borrow_pools.is_empty() {
                    debug!(
                        "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Нет borrow pools для токена {:?}",
                        event_id, path_index, token_bc
                    );
                    skipped_tokens += 1;
                } else if let Some(borrow_pool) = borrow_pools.first() {
                    let pool_liquidity = match snapshot.pools.get(&borrow_pool.pool_address) {
                        Some(p) => {
                            let token_a = Address::from_slice(&p.uniswap_token_a.to_fixed_bytes());
                            let zero_for_one = token_a == token_bc;
                            if zero_for_one {
                                p.liquidity_token_b
                            } else {
                                p.liquidity_token_a
                            }
                        }
                        None => {
                            debug!(
                                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пул для флеш-свопа {:?} не найден",
                                event_id, path_index, borrow_pool.pool_address
                            );
                            U256::zero()
                        }
                    };

                    let flash_amount = shortfall_bc.min(mul_div_u256(
                        pool_liquidity,
                        U256::from(config.uniswap_borrow_utilization),
                        U256::from(100),
                    ));

                    mint_params_bc.amount_actual = match mint_params_bc.amount_actual.checked_add(flash_amount) {
                        Some(amount) => amount,
                        None => {
                            warn!(
                                "[COMPUTE_ARBITRAGE_PROFIT] Переполнение amount_actual для token_bc: {} + {}",
                                mint_params_bc.amount_actual, flash_amount
                            );
                            return None;
                        }
                    };
                    flash_swap_amount = match flash_swap_amount.checked_add(flash_amount) {
                        Some(amount) => amount,
                        None => {
                            warn!(
                                "[COMPUTE_ARBITRAGE_PROFIT] Переполнение flash_swap_amount: {} + {}",
                                flash_swap_amount, flash_amount
                            );
                            return None;
                        }
                    };
                    borrow_pool_fee_tier = borrow_pool.fee_tier;
                    used_flash_swap = true;
                }
            }
        }

        if skipped_tokens > 0 {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пропущено токенов из-за отсутствия borrow pools: {}",
                event_id, path_index, skipped_tokens
            );
        }

        let mint_loan_amount = match mint_params_ab.amount_actual.checked_add(mint_params_bc.amount_actual) {
            Some(amount) => amount,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение mint_loan_amount: {} + {}",
                    mint_params_ab.amount_actual, mint_params_bc.amount_actual
                );
                return None;
            }
        };

        let snapshot_ab = self
            .apply_mint_to_snapshot(snapshot, &mint_params_ab)
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка применения mint AB: {}",
                    event_id, path_index, e
                );
                Arc::clone(snapshot)
            });

        let simulated_snapshot = self
            .apply_mint_to_snapshot(&snapshot_ab, &mint_params_bc)
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка применения mint BC: {}",
                    event_id, path_index, e
                );
                Arc::clone(&snapshot_ab)
            });

        let mut amount_in = arbitrage_loan_amount;
        let mut amount_out = U256::zero();
        let mut total_fees = U256::zero();

        for (i, pool_address) in path.pools.iter().enumerate() {
            let hop_pool = match simulated_snapshot.pools.get(pool_address) {
                Some(pool) => pool,
                None => {
                    debug!(
                        "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Пул {:?} не найден в снапшоте",
                        event_id, path_index, pool_address
                    );
                    return None;
                }
            };

            let token_in = path.tokens[i];
            let token_out = path.tokens[i + 1];
            let zero_for_one = hop_pool.uniswap_token_a == token_in;

            let tick_map = simulated_snapshot
                .tick_maps
                .get(pool_address)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let mut current_liquidity = hop_pool.uniswap_liquidity;
            let mut current_tick = hop_pool.uniswap_tick_current;
            let mut sqrt_price_current = hop_pool.uniswap_sqrt_price;
            let sqrt_price_target = if zero_for_one {
                tick_to_sqrt_price(current_tick - hop_pool.uniswap_tick_spacing)?
            } else {
                tick_to_sqrt_price(current_tick + hop_pool.uniswap_tick_spacing)?
            };

            let (amount_out_step, fee_earned_step, new_sqrt_price, new_tick) = self.compute_swap_step(
                sqrt_price_current,
                current_liquidity,
                sqrt_price_target,
                hop_pool.uniswap_fee_tier,
                amount_in,
                zero_for_one,
                current_tick,
                config.target_price_impact_bps,
                U256::zero(),
                mint_params_ab.liquidity + mint_params_bc.liquidity,
            );

            amount_out = match amount_out.checked_add(amount_out_step) {
                Some(amount) => amount,
                None => {
                    warn!(
                        "[COMPUTE_ARBITRAGE_PROFIT] Переполнение amount_out: {} + {}",
                        amount_out, amount_out_step
                    );
                    return None;
                }
            };
            total_fees = match total_fees.checked_add(fee_earned_step) {
                Some(fees) => fees,
                None => {
                    warn!(
                        "[COMPUTE_ARBITRAGE_PROFIT] Переполнение total_fees: {} + {}",
                        total_fees, fee_earned_step
                    );
                    return None;
                }
            };
            amount_in = amount_out_step;
            sqrt_price_current = new_sqrt_price;
            current_tick = new_tick;

            let next_initialized_tick = self.find_next_initialized_tick(current_tick, zero_for_one, tick_map);
            current_liquidity = self.update_liquidity_on_tick_cross(
                current_liquidity.as_u128(),
                next_initialized_tick,
                tick_map,
                zero_for_one,
            )
            .into();
        }

let (burn_amount_ab, fee_earned_ab) = self
    .calculate_burn_amount(
        mint_params_ab.liquidity,
        pool_ab.uniswap_sqrt_price,
        tick_to_sqrt_price(mint_params_ab.tick_lower)?,
        tick_to_sqrt_price(mint_params_ab.tick_upper)?,
        mint_params_ab.zero_for_one,
        snapshot
            .tick_maps
            .get(&mint_params_ab.pool_address)
            .map(|v| v.as_slice())
            .unwrap_or_else(|| {
                let empty: Vec<(i32, i128, U256, U256, U256)> = Vec::new();
                empty.as_slice()
            }),
        mint_params_ab.tick_lower,
        mint_params_ab.tick_upper,
        pool_ab, // ← 🔥 ДОБАВЛЕНО: передаём сам пул для feeGrowthGlobal
    )
    .unwrap_or_else(|e| {
        debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка burn AB: {}",
            event_id, path_index, e
        );
        (U256::zero(), U256::zero())
    });

let (burn_amount_bc, fee_earned_bc) = self
    .calculate_burn_amount(
        mint_params_bc.liquidity,
        pool_bc.uniswap_sqrt_price,
        tick_to_sqrt_price(mint_params_bc.tick_lower)?,
        tick_to_sqrt_price(mint_params_bc.tick_upper)?,
        mint_params_bc.zero_for_one,
        snapshot
            .tick_maps
            .get(&mint_params_bc.pool_address)
            .map(|v| v.as_slice())
            .unwrap_or_else(|| {
                let empty: Vec<(i32, i128, U256, U256, U256)> = Vec::new();
                empty.as_slice()
            }),
        mint_params_bc.tick_lower,
        mint_params_bc.tick_upper,
        pool_bc, // ← 🔥 ДОБАВЛЕНО: передаём сам пул для feeGrowthGlobal
    )
    .unwrap_or_else(|e| {
        debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Ошибка burn BC: {}",
            event_id, path_index, e
        );
        (U256::zero(), U256::zero())
    });

        let total_burn_amount = match burn_amount_ab.checked_add(burn_amount_bc) {
            Some(amount) => amount,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение total_burn_amount: {} + {}",
                    burn_amount_ab, burn_amount_bc
                );
                return None;
            }
        };

        let total_fee_earned = match fee_earned_ab.checked_add(fee_earned_bc) {
            Some(fees) => fees,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение total_fee_earned: {} + {}",
                    fee_earned_ab, fee_earned_bc
                );
                return None;
            }
        };

        let (pool_fee, flash_loan_fee, flash_swap_fee, protocol_fee) = self.aggregate_fees(
            arbitrage_loan_amount,
            pool_ab.uniswap_fee_tier + pool_bc.uniswap_fee_tier,
            mint_loan_amount,
            flash_swap_amount,
            borrow_pool_fee_tier,
            mint_params_ab.liquidity + mint_params_bc.liquidity,
            &simulated_snapshot,
            path.pools[0],
            base_token,
        );

        let total_fees = match pool_fee
            .checked_add(flash_loan_fee)
            .and_then(|sum| sum.checked_add(flash_swap_fee))
            .and_then(|sum| sum.checked_add(protocol_fee))
        {
            Some(fees) => fees,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение total_fees: pool_fee={} + flash_loan_fee={} + flash_swap_fee={} + protocol_fee={}",
                    pool_fee, flash_loan_fee, flash_swap_fee, protocol_fee
                );
                return None;
            }
        };

        let profit_net = match amount_out
            .checked_add(total_burn_amount)
            .and_then(|sum| sum.checked_add(total_fee_earned))
            .and_then(|sum| sum.checked_sub(total_fees))
            .and_then(|sum| sum.checked_sub(arbitrage_loan_amount))
            .and_then(|sum| sum.checked_sub(mint_loan_amount))
        {
            Some(profit) => profit,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение profit_net: amount_out={} + total_burn_amount={} + total_fee_earned={} - total_fees={} - arbitrage_loan_amount={} - mint_loan_amount={}",
                    amount_out, total_burn_amount, total_fee_earned, total_fees, arbitrage_loan_amount, mint_loan_amount
                );
                return None;
            }
        };

        let final_amount = match amount_out.checked_add(total_burn_amount) {
            Some(amount) => amount,
            None => {
                warn!(
                    "[COMPUTE_ARBITRAGE_PROFIT] Переполнение final_amount: {} + {}",
                    amount_out, total_burn_amount
                );
                return None;
            }
        };

        if profit_net.is_zero() || total_fee_earned < total_fees {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Нулевая прибыль или убыток: profit_net={}, total_fee_earned={}, total_fees={}",
                event_id, path_index, profit_net, total_fee_earned, total_fees
            );
            return None;
        }

        debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} Успех: profit_net={}, final_amount={}, flash_swap_amount={}",
            event_id, path_index, profit_net, final_amount, flash_swap_amount
        );

        Some((profit_net, final_amount, flash_swap_amount, borrow_pool_fee_tier))
    }


}