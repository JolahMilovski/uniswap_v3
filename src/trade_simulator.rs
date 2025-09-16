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
        // Базовые значения, соответствующие 100 USD
        let base_thresholds: HashMap<Address, U256> = {
            let mut m = HashMap::new();
            // Mai Stablecoin (MAI)
            m.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            // USD Coin (USDC)
            m.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));
            // Bridged USDC (USDC.e)
            m.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));
            // Arbitrum (ARB)
            m.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(345_000_000_000_000_000_000u128));
            // Renzo Restaked ETH (ezETH)
            m.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(38_000_000_000_000_000u128));
            // Wrapped Ether (WETH)
            m.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(38_000_000_000_000_000u128));
            // USD₮0 (USD₮0)
            m.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));
            // Wrapped BTC (WBTC)
            m.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(100_000u128));
            // LUSD Stablecoin (LUSD)
            m.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            // Wrapped eETH (weETH)
            m.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(38_489_000_000_000_000u128));
            // ChainLink Token (LINK)
            m.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(8_000_000_000_000_000_000u128));
            // Wrapped (wstETH)
            m.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(33_000_000_000_000_000u128));
            // KelpDao Restaked ETH (rsETH)
            m.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(38_000_000_000_000_000u128));
            // GHO Token (GHO)
            m.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            // Frax (FRAX)
            m.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            // STASIS EURS Token (EURS)
            m.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));
            // Dai Stablecoin (DAI)
            m.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            // Rocket Pool ETH (rETH)
            m.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(38_000_000_000_000_000u128));
            // Aave Token (AAVE)
            m.insert(Address::from_slice(&hex::decode("ba5ddd1f9d7f570dc94a51479a000e3bce967196").unwrap()), U256::from(353_700_000_000_000_000u128));
            m
        };

        // Загружаем значение PROFIT_THRESHOLD_USD из переменной окружения (по умолчанию 100)
        let profit_threshold_usd: f64 = env::var("PROFIT_THRESHOLD_USD")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100.0);

        // Масштабируем пороги относительно 100 USD
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
            .unwrap_or("100".to_string()) // 1% по умолчанию
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
/// Максимальное допустимое ценовое воздействие по умолчанию (50 базисных пунктов = 0.5%)
const PROTOCOL_FEE_SHARE: u32 = 300_000; // 30% комиссии протокола

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

/// Безопасное mulDiv для U256: (a * b) / c.
#[inline]
fn mul_div_u256(a: U256, b: U256, c: U256) -> U256 {
    if c.is_zero() {
        error!("[  MUL_DIV_U256] Деление на ноль: a={}, b={}, c=0", a, b);
        return U256::zero();
    }

    match a.checked_mul(b) {
        Some(prod) => match prod.checked_div(c) {
            Some(result) => {
                debug!(
                    "[  MUL_DIV_U256] Успешное вычисление: ({} * {}) / {} = {}",
                    a, b, c, result
                );
                result
            }
            None => {
                error!(
                    "[  MUL_DIV_U256] Переполнение при делении: prod={} / {}",
                    prod, c
                );
                U256::zero()
            }
        },
        None => {
            let full = a.full_mul(b);
            let q = &full / c;
            match U256::try_from(q) {
                Ok(result) => {
                    debug!(
                        "[  MUL_DIV_U256] Успешное вычисление через full_mul: ({} * {}) / {} = {}",
                        a, b, c, result
                    );
                    result
                }
                Err(_) => {
                    error!(
                        "[  MUL_DIV_U256] Переполнение при умножении: a={} * b={}",
                        a, b
                    );
                    U256::zero()
                }
            }
        }
    }
}

// Структура конфигурации стратегии арбитража
#[derive(Debug, Clone)]
pub struct ArbitrageStrategyConfig {
    pub target_price_impact_bps: u32,
    pub aave_utilization_percent: u64,
    pub uniswap_borrow_utilization: u64,
}

// Структура для параметров mint
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

/// ==== Snapshot графа ====
#[derive(Clone)]
pub struct GraphSnapshotHolder {
    pub pools: HashMap<Address, UniswapPool>,
    pub tick_maps: HashMap<Address, Vec<(i32, i128, U256)>>,
    pub token_decimals: HashMap<Address, u8>,
}

/// ==== Результат симуляции одного пути ====
#[derive(Debug, Clone)]
pub struct PathSimulationResult {
    pub path_index: usize,
    pub base_token: Address,
    pub borrow_optimal: U256,
    pub final_amount: U256,
    pub profit_net: U256,
    pub used_uniswap_flash_supplement: bool,
}

/// Структура для хранения состояния хопа
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

/// ==== Торговый симулятор ====
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

    /// Запускает симулятор, обрабатывая входящие события пулов
    /// # Аргументы
    /// * `simulator_rx` - Канал для получения событий пулов
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

    /// Рассчитывает все комиссии для свопа, флеш-займа, флеш-свопа и протокола.
    ///
    /// # Аргументы
    /// * `amount_in` - Входная сумма для свопа.
    /// * `fee_pips` - Комиссия пула в пунктах (fee_tier).
    /// * `flash_loan_amount` - Сумма флеш-займа Aave.
    /// * `flash_swap_amount` - Сумма флеш-свопа Uniswap.
    /// * `borrow_pool_fee_tier` - Комиссия пула для флеш-свопа.
    /// * `liquidity_position` - Ликвидность позиции для расчёта комиссии протокола.
    ///
    /// # Возвращает
    /// Кортеж `(pool_fee, flash_loan_fee, flash_swap_fee, protocol_fee)` с комиссиями в `U256`.
    fn aggregate_fees(
        &self,
        amount_in: U256,
        fee_pips: u32,
        flash_loan_amount: U256,
        flash_swap_amount: U256,
        borrow_pool_fee_tier: u32,
        liquidity_position: U256,
    ) -> (U256, U256, U256, U256) {
        let pool_fee = mul_div_u256(amount_in, U256::from(fee_pips), U256::from(1_000_000));
        let flash_loan_fee = mul_div_u256(
            flash_loan_amount,
            U256::from(AAVE_FLASH_FEE_NUM),
            U256::from(AAVE_FLASH_FEE_DEN),
        );
        let flash_swap_fee = mul_div_u256(
            flash_swap_amount,
            U256::from(borrow_pool_fee_tier),
            U256::from(1_000_000),
        );
        let protocol_fee = mul_div_u256(
            liquidity_position,
            U256::from(PROTOCOL_FEE_SHARE),
            U256::from(1_000_000),
        );
        (pool_fee, flash_loan_fee, flash_swap_fee, protocol_fee)
    }

    /// Обрабатывает событие пула Uniswap V3, выполняя симуляцию арбитража по всем связанным путям.
    ///
    /// Эта функция вызывается для каждого события пула, полученного через канал `simulator_rx`.
    /// Она извлекает пути, связанные с пулом события, создаёт снапшот графа для каждого пути,
    /// выполняет расчёт арбитража с использованием Aave и флеш-свопов, а затем фильтрует
    /// результаты по минимальному порогу прибыли.
    ///
    /// # Аргументы
    /// * `event` - Событие пула (`PoolEventInfo`), содержащее информацию о пуле и событии.
    /// * `aave_liquidity` - Текущая ликвидность Aave для токенов, используемых в арбитраже.
    ///
    /// # Возвращает
    /// Вектор `PathSimulationResult`, содержащий результаты симуляции для прибыльных путей
    /// после фильтрации по минимальному порогу прибыли.
    ///
    /// # Алгоритм
    /// 1. Логирование начала обработки события с указанием ID события и адреса пула.
    /// 2. Получение всех путей, связанных с пулом события, из `route_builder`.
    /// 3. Для каждого пути:
    ///    - Создание снапшота графа, включающего пул события и пулы пути.
    ///    - Вызов `compute_aave_borrow_amount` для расчёта арбитражной прибыли.
    ///    - Сохранение результатов в `PathSimulationResult`, если прибыль найдена.
    /// 4. Фильтрация результатов по минимальному порогу прибыли.
    /// 5. Логирование итогового количества прибыльных путей.
    /// 6. Возврат отфильтрованных результатов.
    ///
    /// # Замечания
    /// - Если для пула события нет путей, возвращается пустой вектор.
    /// - Снапшот создаётся индивидуально для каждого пути, чтобы включить все необходимые пулы.
    /// - Поле `used_uniswap_flash_supplement` в `PathSimulationResult` определяется как
    ///   `flash_swap_amount != U256::zero()`, чтобы преобразовать `U256` в `bool`.
    async fn process_trade_event(
        &self,
        event: PoolEventInfo,
        aave_liquidity: AaveTokenLiquidity,
    ) -> Vec<PathSimulationResult> {
        // Логируем начало обработки события с указанием идентификатора события и адреса пула
        debug!(
            "[PROCESS_TRADE_EVENT] 🆔={} Обработка события для пула {:?}",
            event.event_id, event.address
        );

        // Формируем начальный список пулов, включающий только пул события
        let route_pools = vec![event.address];
        // Инициализируем вектор для хранения результатов симуляции
        let mut results = Vec::new();

        // Извлекаем пути, связанные с пулом события, из route_builder
        let paths = if let Some(indexes) = self.route_builder.pool_to_paths.get(&event.address) {
            indexes
                .iter()
                .map(|&i| self.route_builder.paths[i].clone())
                .collect::<Vec<_>>()
        } else {
            // Если путей нет, логируем это и возвращаем пустой результат
            debug!(
                "[PROCESS_TRADE_EVENT] 🆔={} Нет путей для пула {:?}",
                event.event_id, event.address
            );
            return Vec::new();
        };

        // Обрабатываем каждый путь
        for (i, path) in paths.iter().enumerate() {
            // Формируем список пулов для снапшота, добавляя пулы текущего пути к route_pools
            let mut path_pools = route_pools.clone();
            path_pools.extend_from_slice(&path.pools);

            // Создаём снапшот графа, включающий пул события и пулы пути
            let path_snapshot = self.create_graph_snapshot(&path_pools, &[], event.event_id);

            // Выполняем расчёт арбитража для текущего пути
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
                // Преобразуем результат в PathSimulationResult
                results.push(PathSimulationResult {
                    path_index: i,
                    base_token: path.tokens.first().copied().unwrap_or_default(),
                    borrow_optimal: result.0, // Оптимальная сумма займа
                    final_amount: result.1,   // Итоговая сумма после арбитража
                    profit_net: result.0,     // Чистая прибыль
                    // Преобразуем flash_swap_amount (U256) в bool: true, если использовался флеш-своп
                    used_uniswap_flash_supplement: result.2 != U256::zero(),
                });
            }
        }

        // Фильтруем результаты по минимальному порогу прибыли
        let filtered_results = self.filter_by_min_profit_threshold(results);

        // Логируем количество отфильтрованных путей
        debug!(
            "[PROCESS_TRADE_EVENT] 🆔={} Итоговых путей после фильтрации: {}",
            event.event_id,
            filtered_results.len()
        );

        // Возвращаем отфильтрованные результаты
        filtered_results
    }

    //----

    /// Вычисляет шаг свопа с учётом созданной ликвидности в пуле.
    ///
    /// # Аргументы
    /// * `sqrt_price_current` - Текущая цена пула (Q64_96).
    /// * `liquidity` - Текущая ликвидность пула (U256).
    /// * `tick_map` - Карта тиков пула.
    /// * `tick_current` - Текущий тик пула.
    /// * `fee_pips` - Комиссия пула в пунктах.
    /// * `amount_in` - Входная сумма для свопа.
    /// * `zero_for_one` - Направление свопа (true для A→B, false для B→A).
    /// * `event_id` - Идентификатор события.
    /// * `path_index` - Индекс пути.
    /// * `pool_address` - Адрес пула.
    /// * `target_price_impact_bps` - Целевое ценовое воздействие в базисных пунктах.
    /// * `additional_fee` - Дополнительные комиссии (например, от флеш-займов).
    /// * `mint_liquidity` - Ликвидность, добавленная через mint.
    ///
    /// # Возвращает
    /// Кортеж `(amount_out, fee_earned, crossed_ticks)` с выходной суммой, комиссиями и списком пересечённых тиков.
    fn compute_amount_out(
        &self,
        sqrt_price_current: Q64_96,
        liquidity: U256,
        tick_map: &[(i32, i128, U256)],
        tick_current: i32,
        fee_pips: u32,
        amount_in: U256,
        zero_for_one: bool,
        event_id: u64,
        path_index: &str,
        pool_address: Address,
        target_price_impact_bps: u32,
        additional_fee: U256,
        mint_liquidity: U256,
    ) -> (U256, U256, Vec<i32>) {
        let mut current_sqrt_price = sqrt_price_current;
        let mut remaining_amount = amount_in;
        let mut total_fee_earned = U256::zero();
        let mut crossed_ticks = Vec::new();

        let fee_pips = fee_pips;
        let mut current_liquidity = liquidity;
        let mut current_tick = tick_current;

        debug!(
            "[COMPUTE_AMOUNT_OUT] 🆔={} путь={} Пул: {:?}",
            event_id, path_index, pool_address
        );

        while !remaining_amount.is_zero() {
            
            let next_tick = self.find_next_initialized_tick(current_tick, zero_for_one, tick_map);


            let sqrt_price_target = tick_to_sqrt_price(next_tick).unwrap_or(if zero_for_one {
                Q64_96::from_u256(U256::zero()).unwrap()
            } else {
                Q64_96::from_u256(U256::max_value()).unwrap()
            });

            let (amount_out_step, fee_earned_step, new_sqrt_price, new_tick) = self
                .compute_swap_step(
                    current_sqrt_price,
                    current_liquidity,
                    sqrt_price_target,
                    fee_pips,
                    remaining_amount,
                    zero_for_one,
                    current_tick,
                    target_price_impact_bps,
                    additional_fee,
                    mint_liquidity,
                );

            if amount_out_step.is_zero() {
                break;
            }

            total_fee_earned = total_fee_earned.saturating_add(fee_earned_step);
            remaining_amount = remaining_amount.saturating_sub(mul_div_u256(
                amount_out_step,
                U256::one(),
                U256::from(1_000_000 - fee_pips as u64),
            ));
            current_sqrt_price = new_sqrt_price;
            current_tick = new_tick;

            if zero_for_one && current_tick <= next_tick
                || !zero_for_one && current_tick >= next_tick
            {
                crossed_ticks.push(next_tick);
                current_liquidity = U256::from(self.update_liquidity_on_tick_cross(
                    current_liquidity.as_u128(),
                    next_tick,
                    tick_map,
                    zero_for_one,
                ));
            }

            debug!(
                "[COMPUTE_AMOUNT_OUT] 🆔={} путь={} Пул: {:?} Шаг свопа: fee_earned={}",
                event_id, path_index, pool_address, fee_earned_step
            );
        }

        let amount_out_total = amount_in.saturating_sub(remaining_amount);
        debug!("[COMPUTE_AMOUNT_OUT] 🆔={} путь={} Пул: {:?} Итог: amount_out={}, fee_earned={}, crossed_ticks={:?}", 
           event_id, path_index, pool_address, amount_out_total, total_fee_earned, crossed_ticks);

        (amount_out_total, total_fee_earned, crossed_ticks)
    }

    /// Вычисляет прибыль от арбитража с двумя mint, флеш-свопами, burn/collect.
    ///
    /// # Аргументы
    /// * `event_id` - Идентификатор события.
    /// * `path_index` - Индекс пути.
    /// * `aave_liquidity` - Ликвидность Aave.
    /// * `snapshot` - Снапшот графа.
    /// * `path` - Арбитражный путь.
    ///
    /// # Возвращает
    /// `Option<(profit_net, final_amount, used_flash_swap, reserved)>` или `None`.
    pub async fn compute_arbitrage_profit(
        &self,
        event_id: u64,
        path_index: &str,
        aave_liquidity: &AaveTokenLiquidity,
        snapshot: &Arc<GraphSnapshotHolder>,
        path: &ArbitragePath,
    ) -> Option<(U256, U256, bool, u32)> {
        debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} начало",
            event_id, path_index
        );

        let config = &*DEFAULT_ARBITRAGE_STRATEGY_CONFIG;
        let base_token = path.tokens.first().copied().unwrap_or_default();

        let max_borrow = aave_liquidity
            .aave_token_info
            .get(&base_token)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());
        if max_borrow.is_zero() {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} нет ликвидности Aave для {:?}",
                event_id, path_index, base_token
            );
            return None;
        }
        let arbitrage_loan_amount = mul_div_u256(
            max_borrow,
            U256::from(config.aave_utilization_percent),
            U256::from(100),
        );

        let pool_ab = snapshot
            .pools
            .get(&path.pools[0])
            .expect("Пул A→B не найден");
        let pool_bc = snapshot
            .pools
            .get(&path.pools[1])
            .expect("Пул B→C не найден");
        let token_ab = if pool_ab.uniswap_token_a == path.tokens[0].into() {
            pool_ab.uniswap_token_b.clone()
        } else {
            pool_ab.uniswap_token_a.clone()
        };
        let token_bc = if pool_bc.uniswap_token_a == path.tokens[1].into() {
            pool_bc.uniswap_token_b.clone()
        } else {
            pool_bc.uniswap_token_a.clone()
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

        let mint_params_ab = self
            .calculate_mint_parameters(path.pools[0], config.target_price_impact_bps, snapshot)
            .await
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} ошибка mint AB: {}",
                    event_id, path_index, e
                );
                MintParameters {
                    pool_address: path.pools[0],
                    zero_for_one: pool_ab.uniswap_token_a == path.tokens[0].into(),
                    amount_desired: U256::zero(),
                    amount_actual: U256::zero(),
                    tick_lower: pool_ab.uniswap_tick_current - pool_ab.uniswap_tick_spacing,
                    tick_upper: pool_ab.uniswap_tick_current + pool_ab.uniswap_tick_spacing,
                    liquidity: U256::zero(),
                }
            });

        let mint_params_bc = self
            .calculate_mint_parameters(path.pools[1], config.target_price_impact_bps, snapshot)
            .await
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} ошибка mint BC: {}",
                    event_id, path_index, e
                );
                MintParameters {
                    pool_address: path.pools[1],
                    zero_for_one: pool_bc.uniswap_token_a == path.tokens[1].into(),
                    amount_desired: U256::zero(),
                    amount_actual: U256::zero(),
                    tick_lower: pool_bc.uniswap_tick_current - pool_bc.uniswap_tick_spacing,
                    tick_upper: pool_bc.uniswap_tick_current + pool_bc.uniswap_tick_spacing,
                    liquidity: U256::zero(),
                }
            });

        let mut mint_params_ab = mint_params_ab;
        let mut mint_params_bc = mint_params_bc;
        mint_params_ab.amount_actual = mint_params_ab.amount_desired.min(max_borrow_ab);
        mint_params_bc.amount_actual = mint_params_bc.amount_desired.min(max_borrow_bc);

        let mut flash_swap_amount = U256::zero();
        let mut used_flash_swap = false;
        let mut borrow_pool_fee_tier = 0;

        if mint_params_ab.amount_actual < mint_params_ab.amount_desired {
            let shortfall_ab = mint_params_ab.amount_desired - mint_params_ab.amount_actual;
            if let Some(borrow_pools) = self.route_builder.borrow_pools.get(&token_ab) {
                if let Some(borrow_pool) = borrow_pools.first() {
                    let pool_liquidity = snapshot
                        .pools
                        .get(&borrow_pool.pool_address)
                        .map(|p| {
                            let token_a = Address::from_slice(&p.uniswap_token_a.to_fixed_bytes());
                            let zero_for_one = token_a == *token_ab;
                            if zero_for_one {
                                p.liquidity_token_b
                            } else {
                                p.liquidity_token_a
                            }
                        })
                        .unwrap_or(U256::zero());

                    let flash_amount = shortfall_ab.min(mul_div_u256(
                        pool_liquidity,
                        U256::from(config.uniswap_borrow_utilization),
                        U256::from(100),
                    ));

                    mint_params_ab.amount_actual += flash_amount;
                    flash_swap_amount += flash_amount;
                    borrow_pool_fee_tier = borrow_pool.fee_tier;
                    used_flash_swap = true;
                }
            }
        }

        if mint_params_bc.amount_actual < mint_params_bc.amount_desired {
            let shortfall_bc = mint_params_bc.amount_desired - mint_params_bc.amount_actual;
            if let Some(borrow_pools) = self.route_builder.borrow_pools.get(&token_bc) {
                if let Some(borrow_pool) = borrow_pools.first() {
                    let pool_liquidity = snapshot
                        .pools
                        .get(&borrow_pool.pool_address)
                        .map(|p| {
                            let token_a = Address::from_slice(&p.uniswap_token_a.to_fixed_bytes());
                            let zero_for_one = token_a == *token_bc;
                            if zero_for_one {
                                p.liquidity_token_b
                            } else {
                                p.liquidity_token_a
                            }
                        })
                        .unwrap_or(U256::zero());

                    let flash_amount = shortfall_bc.min(mul_div_u256(
                        pool_liquidity,
                        U256::from(config.uniswap_borrow_utilization),
                        U256::from(100),
                    ));

                    mint_params_bc.amount_actual += flash_amount;
                    flash_swap_amount += flash_amount;
                    borrow_pool_fee_tier = borrow_pool.fee_tier;
                    used_flash_swap = true;
                }
            }
        }

        let mint_loan_amount = mint_params_ab.amount_actual + mint_params_bc.amount_actual;

        let snapshot_ab = self
            .apply_mint_to_snapshot(snapshot, &mint_params_ab)
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} ошибка применения mint AB: {}",
                    event_id, path_index, e
                );
                Arc::clone(snapshot)
            });
        let simulated_snapshot = self
            .apply_mint_to_snapshot(&snapshot_ab, &mint_params_bc)
            .unwrap_or_else(|e| {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} ошибка применения mint BC: {}",
                    event_id, path_index, e
                );
                Arc::clone(&snapshot_ab)
            });

        let mut current_amount = arbitrage_loan_amount;
        let mut current_token = base_token;
        let mut total_fee_earned = U256::zero();

        for (i, pool_address) in path.pools.iter().enumerate() {
            let hop_pool = simulated_snapshot
                .pools
                .get(pool_address)
                .expect("Пул не найден");
            let zero_for_one = hop_pool.uniswap_token_a == current_token.into();

            let (amount_out, fee_earned, crossed_ticks) = self.compute_amount_out(
                hop_pool.uniswap_sqrt_price,
                hop_pool.uniswap_liquidity,
                &hop_pool
                    .tick_map
                    .iter()
                    .map(|(t, (d, l))| (*t, *d, *l))
                    .collect::<Vec<_>>(),
                hop_pool.uniswap_tick_current,
                hop_pool.uniswap_fee_tier,
                current_amount,
                zero_for_one,
                event_id,
                &format!("{}_hop{}", path_index, i),
                *pool_address,
                config.target_price_impact_bps,
                if i == 0 {
                    mint_loan_amount + arbitrage_loan_amount + flash_swap_amount
                } else {
                    U256::zero()
                },
                if i == 0 {
                    mint_params_ab.liquidity
                } else {
                    mint_params_bc.liquidity
                },
            );

            if !crossed_ticks.is_empty() {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} хоп={} пересечено тиков: {:?}",
                    event_id, path_index, i, crossed_ticks
                );
                let liquidity_delta: i128 = crossed_ticks.iter().fold(0, |acc, &tick| {
                    let delta = hop_pool
                        .tick_map
                        .iter()
                        .find(|(t, _)| **t == tick)
                        .map(|(_, (d, _))| *d)
                        .unwrap_or(0);
                    acc + delta
                });
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} хоп={} дельта ликвидности: {}",
                    event_id, path_index, i, liquidity_delta
                );
            }

            if amount_out.is_zero() {
                debug!(
                    "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} нулевой выход на хопе {}",
                    event_id, path_index, i
                );
                return None;
            }

            current_amount = amount_out;
            total_fee_earned = total_fee_earned.saturating_add(fee_earned);
            current_token = if zero_for_one {
                *hop_pool.uniswap_token_b
            } else {
                *hop_pool.uniswap_token_a
            };
        }

        if current_token != base_token {
            debug!(
                "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} конечный токен не совпадает с базовым",
                event_id, path_index
            );
            return None;
        }

        let mut returned_amount_ab = U256::zero();
        let mut returned_amount_bc = U256::zero();
        let mut fee_earned_ab = U256::zero();
        let mut fee_earned_bc = U256::zero();

        if mint_params_ab.liquidity > U256::zero() {
            let sqrt_price_current = simulated_snapshot
                .pools
                .get(&mint_params_ab.pool_address)
                .map(|p| p.uniswap_sqrt_price)
                .unwrap_or_default();
            let sqrt_price_lower = tick_to_sqrt_price(mint_params_ab.tick_lower).ok()?;
            let sqrt_price_upper = tick_to_sqrt_price(mint_params_ab.tick_upper).ok()?;
            let (amount, fee) = self
                .calculate_burn_amount(
                    mint_params_ab.liquidity,
                    sqrt_price_current,
                    sqrt_price_lower,
                    sqrt_price_upper,
                    mint_params_ab.zero_for_one,
                    &simulated_snapshot
                        .pools
                        .get(&mint_params_ab.pool_address)
                        .unwrap()
                        .tick_map
                        .iter()
                        .map(|(t, (d, l))| (*t, *d, *l))
                        .collect::<Vec<_>>(),
                    mint_params_ab.tick_lower,
                    mint_params_ab.tick_upper,
                )
                .ok()?;
            returned_amount_ab = amount;
            fee_earned_ab = fee;
        }

        if mint_params_bc.liquidity > U256::zero() {
            let sqrt_price_current = simulated_snapshot
                .pools
                .get(&mint_params_bc.pool_address)
                .map(|p| p.uniswap_sqrt_price)
                .unwrap_or_default();
            let sqrt_price_lower = tick_to_sqrt_price(mint_params_bc.tick_lower).ok()?;
            let sqrt_price_upper = tick_to_sqrt_price(mint_params_bc.tick_upper).ok()?;
            let (amount, fee) = self
                .calculate_burn_amount(
                    mint_params_bc.liquidity,
                    sqrt_price_current,
                    sqrt_price_lower,
                    sqrt_price_upper,
                    mint_params_bc.zero_for_one,
                    &simulated_snapshot
                        .pools
                        .get(&mint_params_bc.pool_address)
                        .unwrap()
                        .tick_map
                        .iter()
                        .map(|(t, (d, l))| (*t, *d, *l))
                        .collect::<Vec<_>>(),
                    mint_params_bc.tick_lower,
                    mint_params_bc.tick_upper,
                )
                .ok()?;
            returned_amount_bc = amount;
            fee_earned_bc = fee;
        }

        let total_borrow = mint_loan_amount + arbitrage_loan_amount + flash_swap_amount;
        let (pool_fee, flash_loan_fee, flash_swap_fee, protocol_fee) = self.aggregate_fees(
            current_amount,
            pool_ab.uniswap_fee_tier,
            mint_loan_amount + arbitrage_loan_amount,
            flash_swap_amount,
            borrow_pool_fee_tier,
            mint_params_ab.liquidity + mint_params_bc.liquidity,
        );
        let total_fees = pool_fee
            .saturating_add(flash_loan_fee)
            .saturating_add(flash_swap_fee)
            .saturating_add(protocol_fee);
        let total_fee_earned = total_fee_earned.saturating_add(pool_fee); // Добавляем pool_fee к общей сумме
        let total_returned = current_amount
            .saturating_add(returned_amount_ab)
            .saturating_add(returned_amount_bc)
            .saturating_add(total_fee_earned)
            .saturating_add(fee_earned_ab)
            .saturating_add(fee_earned_bc);
        let profit_net = total_returned
            .saturating_sub(total_borrow)
            .saturating_sub(total_fees);

        let threshold = MIN_PROFIT_THRESHOLD_BY_TOKEN
            .get(&base_token)
            .copied()
            .unwrap_or(U256::from(100_000_000_000_000_000u128));

        if profit_net < threshold || total_fee_earned < total_fees {
            debug!(
            "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} профит {} < порога {} или комиссии {} < {}",
            event_id, path_index, profit_net, threshold, total_fee_earned, total_fees
        );
            return None;
        }

        debug!(
        "[COMPUTE_ARBITRAGE_PROFIT] 🆔={} путь={} успех: profit={}, final_amount={}, fee_earned={}, pool_fee={}, returned_ab={}, returned_bc={}",
        event_id, path_index, profit_net, total_returned, total_fee_earned, pool_fee, returned_amount_ab, returned_amount_bc
    );

        Some((profit_net, total_returned, used_flash_swap, 0))
    }

    //---

    /// Вычисляет оптимальную сумму займа из Aave и прибыль от арбитража
    /// # Аргументы
    /// * `path` - Арбитражный путь
    /// * `event_id` - Идентификатор события
    /// * `path_index` - Индекс пути
    /// * `snapshot` - Снапшот графа пулов
    /// # Возвращает
    /// Кортеж (borrow_optimal, profit_after_fee, final_amount, used_flash_swap) или None
    async fn compute_aave_borrow_amount(
        &self,
        path: &ArbitragePath,
        event_id: u64,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
        aave_liquidity: &AaveTokenLiquidity,
    ) -> Option<(U256, U256, U256, bool)> {
        let base_token = path.tokens.first().copied().unwrap_or_default();
        debug!(
            "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} начало расчета, base_token={:?}",
            event_id, path_index, base_token
        );

        let aave_liquidity = aave_liquidity;

        let max_borrow = aave_liquidity
            .aave_token_info
            .get(&base_token)
            .map(|(_, virtual_balance)| *virtual_balance)
            .unwrap_or(U256::zero());

        if max_borrow.is_zero() {
            debug!(
                "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} нет ликвидности Aave для {:?}",
                event_id, path_index, base_token
            );
            return None;
        }

        if let Some((profit_net, final_amount, used_flash_swap, _)) = self
            .compute_arbitrage_profit(event_id, path_index, aave_liquidity, snapshot, path)
            .await
        {
            let borrow_optimal = mul_div_u256(
                max_borrow,
                U256::from(DEFAULT_ARBITRAGE_STRATEGY_CONFIG.aave_utilization_percent),
                U256::from(100),
            );
            let aave_fee = mul_div_u256(
                borrow_optimal,
                U256::from(AAVE_FLASH_FEE_NUM),
                U256::from(AAVE_FLASH_FEE_DEN),
            );
            let profit_after_fee = profit_net.saturating_sub(aave_fee);

            if profit_after_fee.is_zero() {
                debug!(
                    "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} прибыль после комиссии нулевая",
                    event_id, path_index
                );
                return None;
            }

            debug!("[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} успех: borrow_optimal={}, profit_net={}, final_amount={}", 
                   event_id, path_index, borrow_optimal, profit_after_fee, final_amount);

            Some((
                borrow_optimal,
                profit_after_fee,
                final_amount,
                used_flash_swap,
            ))
        } else {
            warn!(
                "[COMPUTE_AAVE_BORROW_AMOUNT] 🆔={} путь={} не найдено прибыльных вариантов",
                event_id, path_index
            );
            None
        }
    }

    /// Вычисляет параметры для mint, чтобы достичь целевого сдвига цены.
    ///
    /// # Аргументы
    /// * `pool_address` - Адрес пула Uniswap V3.
    /// * `target_price_impact_bps` - Целевой сдвиг цены в базисных пунктах (50–200).
    /// * `snapshot` - Снапшот графа пулов.
    ///
    /// # Возвращает
    /// `Result<MintParameters, String>` с параметрами mint или ошибкой.
    async fn calculate_mint_parameters(
        &self,
        pool_address: Address,
        target_price_impact_bps: u32,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Result<MintParameters, String> {
        let pool = snapshot
            .pools
            .get(&pool_address)
            .ok_or_else(|| format!("Пул {:?} не найден в снапшоте", pool_address))?;

        let current_price = pool.uniswap_sqrt_price;
        let fee_tier = pool.uniswap_fee_tier; // Используем pool_address для получения fee_tier
        debug!(
            "[CALCULATE_MINT_PARAMETERS] Пул {:?}, fee_tier={}",
            pool_address, fee_tier
        );

        // Целевая цена: sqrt(current_price * (1 + impact / 10_000))
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

        let liquidity_delta = numerator
            .checked_div(denominator)
            .ok_or("Деление на ноль при расчете liquidity_delta")?;

        let tick_current = pool.uniswap_tick_current;
        let tick_spacing = pool.uniswap_tick_spacing;
        let tick_lower = (tick_current - 10 * tick_spacing).max(MIN_TICK);
        let tick_upper = (tick_current + 10 * tick_spacing).min(MAX_TICK);

        let sqrt_price_lower = tick_to_sqrt_price(tick_lower)?;
        let sqrt_price_upper = tick_to_sqrt_price(tick_upper)?;

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
    /// Применяет mint к снапшоту, возвращая новый изменённый снапшот
    /// # Аргументы
    /// * `snapshot` - Исходный снапшот
    /// * `mint_params` - Параметры mint
    /// # Возвращает
    /// Новый снапшот или ошибку
    fn apply_mint_to_snapshot(
        &self,
        snapshot: &Arc<GraphSnapshotHolder>,
        mint_params: &MintParameters,
    ) -> Result<Arc<GraphSnapshotHolder>, String> {
        let mut new_snapshot = Arc::new(snapshot.as_ref().clone());

        let pool = Arc::get_mut(&mut new_snapshot)
            .unwrap()
            .pools
            .get_mut(&mint_params.pool_address)
            .ok_or_else(|| format!("Пул {:?} не найден в снапшоте", mint_params.pool_address))?;

        pool.uniswap_liquidity = pool
            .uniswap_liquidity
            .checked_add(mint_params.liquidity)
            .ok_or("Переполнение ликвидности пула")?;

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
            pool.liquidity_token_b = pool
                .liquidity_token_b
                .checked_add(mint_params.amount_desired)
                .ok_or("Переполнение ликвидности token1")?;
        } else {
            pool.liquidity_token_a = pool
                .liquidity_token_a
                .checked_add(mint_params.amount_desired)
                .ok_or("Переполнение ликвидности token0")?;
        }

        let tick_entry = Arc::get_mut(&mut new_snapshot)
            .unwrap()
            .tick_maps
            .entry(mint_params.pool_address)
            .or_insert_with(Vec::new);

        for tick in mint_params.tick_lower..=mint_params.tick_upper {
            if let Some((_, net_liquidity, gross_liquidity)) =
                tick_entry.iter_mut().find(|(t, _, _)| *t == tick)
            {
                *net_liquidity = (*net_liquidity as u128 + mint_params.liquidity.as_u128()) as i128;
                *gross_liquidity = gross_liquidity
                    .checked_add(mint_params.liquidity)
                    .ok_or("Переполнение gross liquidity")?;
            } else {
                tick_entry.push((
                    tick,
                    mint_params.liquidity.as_u128() as i128,
                    mint_params.liquidity,
                ));
            }
        }

        Ok(new_snapshot)
    }

    /// Рассчитывает сумму токенов и комиссий, возвращаемых при burn.
    ///
    /// # Аргументы
    /// * `liquidity` - Ликвидность позиции.
    /// * `sqrt_price_current` - Текущая цена пула (Q64_96).
    /// * `sqrt_price_lower` - Нижняя цена диапазона (Q64_96).
    /// * `sqrt_price_upper` - Верхняя цена диапазона (Q64_96).
    /// * `zero_for_one` - Направление позиции.
    /// * `tick_map` - Карта тиков пула.
    /// * `tick_lower`, `tick_upper` - Диапазон позиции.
    ///
    /// # Возвращает
    /// `Result<(amount, fee), String>` с возвращённой суммой и комиссиями или ошибкой.
    fn calculate_burn_amount(
        &self,
        liquidity: U256,
        sqrt_price_current: Q64_96,
        sqrt_price_lower: Q64_96,
        sqrt_price_upper: Q64_96,
        zero_for_one: bool,
        tick_map: &[(i32, i128, U256)],
        tick_lower: i32,
        tick_upper: i32,
    ) -> Result<(U256, U256), String> {
        if liquidity.is_zero() {
            return Ok((U256::zero(), U256::zero()));
        }

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
            // Проверка на нулевой знаменатель
            if denominator.is_zero() {
                debug!(
                "[CALCULATE_BURN_AMOUNT] Нулевой знаменатель: sqrt_price_upper={}, sqrt_price_lower={}",
                sqrt_price_upper.to_u256(), sqrt_price_lower.to_u256()
            );
                return Ok((U256::zero(), U256::zero()));
            }
            numerator
                .checked_div(denominator)
                .ok_or("Деление на ноль при расчете amount0")?
        };

        let mut fee_earned = U256::zero();
        let mut total_liquidity = U256::zero();
        let mut crossed_ticks = Vec::new();
        for (tick, _, tick_liquidity) in tick_map
            .iter()
            .filter(|(t, _, l)| *t >= tick_lower && *t <= tick_upper && l > &U256::zero())
        {
            debug!(
                "[CALCULATE_BURN_AMOUNT] Обрабатываем тик: {}, ликвидность тика: {}",
                tick, tick_liquidity
            );
            total_liquidity = total_liquidity.saturating_add(*tick_liquidity);
            crossed_ticks.push(*tick);
        }

        if !total_liquidity.is_zero() {
            let (pool_fee, _, _, protocol_fee) = self.aggregate_fees(
                amount,
                0, // fee_pips не используется для burn
                U256::zero(),
                U256::zero(),
                0,
                liquidity,
            );
            let fee_position = mul_div_u256(amount, U256::from(1_000_000), U256::from(1_000_000));
            let fee_net = mul_div_u256(fee_position, liquidity, total_liquidity.max(U256::one()));
            fee_earned = fee_net.saturating_add(protocol_fee);
            debug!(
                "[CALCULATE_BURN_AMOUNT] Комиссия пула: {}, протокол: {}, чистая: {}, итоговая: {}",
                pool_fee, protocol_fee, fee_net, fee_earned
            );
        }

        debug!(
            "[CALCULATE_BURN_AMOUNT] Пересечено тиков: {}, список: {:?}",
            crossed_ticks.len(),
            crossed_ticks
        );
        let expected_range = ((tick_upper - tick_lower) / 200) as usize;
        if crossed_ticks.len() < expected_range {
            warn!(
                "[CALCULATE_BURN_AMOUNT] Недостаточно пересечённых тиков: {} < {}",
                crossed_ticks.len(),
                expected_range
            );
        }

        Ok((amount, fee_earned))
    }

    /// Фильтрует результаты по минимальному порогу прибыли
    /// # Аргументы
    /// * `results` - Вектор результатов симуляции
    /// # Возвращает
    /// Отфильтрованный вектор результатов
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
                        debug!("[FILTER_BY_MIN_PROFIT] для токена {:?} не найден порог, используется {}", res.base_token, default);
                        default
                    });

                if res.profit_net >= threshold {
                    true
                } else {
                    debug!("[FILTER_BY_MIN_PROFIT] путь {} отфильтрован: прибыль {} < порога {}", res.path_index, res.profit_net, threshold);
                    false
                }
            })
            .collect()
    }

    /// Находит следующий инициализированный тик в карте тиков
    /// # Аргументы
    /// * `current_tick` - Текущий тик
    /// * `zero_for_one` - Направление свопа
    /// * `tick_map` - Карта тиков
    /// # Возвращает
    /// Следующий инициализированный тик
    fn find_next_initialized_tick(
        &self,
        current_tick: i32,
        zero_for_one: bool,
        tick_map: &[(i32, i128, U256)],
    ) -> i32 {
        if tick_map.is_empty() {
            return if zero_for_one { MIN_TICK } else { MAX_TICK };
        }

        if zero_for_one {
            tick_map
                .iter()
                .filter(|(tick, _, _)| *tick < current_tick)
                .map(|(tick, _, _)| *tick)
                .max()
                .unwrap_or(MIN_TICK)
        } else {
            tick_map
                .iter()
                .filter(|(tick, _, _)| *tick > current_tick)
                .map(|(tick, _, _)| *tick)
                .min()
                .unwrap_or(MAX_TICK)
        }
    }

    /// Обновляет ликвидность при пересечении тика
    /// # Аргументы
    /// * `current_liquidity` - Текущая ликвидность (u128 для совместимости)
    /// * `tick` - Пересечённый тик
    /// * `tick_map` - Карта тиков
    /// * `zero_for_one` - Направление свопа
    /// # Возвращает
    /// Новая ликвидность
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
            current_liquidity.saturating_sub(delta.unsigned_abs())
        } else {
            current_liquidity.saturating_add(delta.unsigned_abs())
        }
    }

    /// Создаёт снапшот графа для указанных пулов
    /// # Аргументы
    /// * `route_pools` - Пулы для включения в снапшот
    /// * `_reserved` - Зарезервированный параметр (не используется)
    /// * `event_id` - Идентификатор события
    /// # Возвращает
    /// Снапшот графа
    fn create_graph_snapshot(
        &self,
        route_pools: &[Address],
        borrow_pools: &[BorrowPoolInfo],
        event_id: u64,
    ) -> Arc<GraphSnapshotHolder> {
        debug!(
            "[ CREATE_GRAPH_SNAPSHOT ] 🆔 = {} Создание снимка графа для пулов: {:?}",
            event_id, route_pools
        );
        let mut pool_set: HashSet<Address> = HashSet::new();
        for address in route_pools {
            pool_set.insert(*address);
        }
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
                snapshot.pools.insert(addr, pool.clone());
                let mut tick_map: Vec<(i32, i128, U256)> =
                    pool.tick_map.iter().map(|(k, v)| (*k, v.0, v.1)).collect();
                tick_map.sort_by_key(|(t, _, _)| *t);
                snapshot.tick_maps.insert(addr, tick_map);
                snapshot
                    .token_decimals
                    .entry(Address::from_slice(&pool.uniswap_token_a.to_fixed_bytes()))
                    .or_insert(pool.uniswap_token_a_decimals);
                snapshot
                    .token_decimals
                    .entry(Address::from_slice(&pool.uniswap_token_b.to_fixed_bytes()))
                    .or_insert(pool.uniswap_token_b_decimals);
            }
        }

        debug!("[  CREATE_GRAPH_SNAPSHOT  ] Снимок для 🆔 = {} : пулов = {}, tick_maps = {}, decimals = {}", event_id, snapshot.pools.len(), snapshot.tick_maps.len(), snapshot.token_decimals.len());
        Arc::new(snapshot)
    }

    /// # Аргументы
    /// * `sqrt_price_current` - Текущая цена пула (Q64_96).
    /// * `liquidity` - Текущая ликвидность пула (U256).
    /// * `sqrt_price_target` - Целевая цена пула (Q64_96).
    /// * `fee_pips` - Комиссия пула в пунктах.
    /// * `amount_in_remaining` - Оставшаяся входная сумма.
    /// * `zero_for_one` - Направление свопа (true для A→B, false для B→A).
    /// * `current_tick` - Текущий тик пула.
    /// * `target_price_impact_bps` - Целевое ценовое воздействие в базисных пунктах.
    /// * `additional_fee` - Дополнительные комиссии (например, от флеш-займов).
    /// * `mint_liquidity` - Ликвидность, добавленная через mint.
    ///
    /// # Возвращает
    /// Кортеж `(amount_out, fee_earned, new_sqrt_price, new_tick)` с выходной суммой, комиссиями, новой ценой и тиком.
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
            debug!(
                "[COMPUTE_SWAP_STEP] Слишком большое воздействие: {} > {}",
                price_diff, limit
            );
            return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
        }

        let total_liquidity = liquidity.saturating_add(mint_liquidity);
        if total_liquidity.is_zero() {
            debug!("[COMPUTE_SWAP_STEP] Нулевая ликвидность в пуле");
            return (U256::zero(), U256::zero(), sqrt_price_current, current_tick);
        }

        let sqrt_price_next = if zero_for_one {
            sqrt_price_current
                .sub(Q64_96 { value: price_diff })
                .unwrap_or(sqrt_price_current)
        } else {
            sqrt_price_current
                .add(Q64_96 { value: price_diff })
                .unwrap_or(sqrt_price_current)
        };

        let amount_out_step = mul_div_u256(
            amount_in_remaining,
            total_liquidity,
            total_liquidity.saturating_add(U256::one()),
        );

        let (pool_fee, _, _, _) = self.aggregate_fees(
            amount_in_remaining,
            fee_pips,
            U256::zero(),
            U256::zero(),
            0,
            U256::zero(),
        );
        let fee_earned_step = pool_fee.saturating_add(additional_fee);

        let new_tick = if sqrt_price_next != sqrt_price_current {
            let new_tick_est = current_tick + if zero_for_one { -1 } else { 1 };
            new_tick_est.clamp(MIN_TICK, MAX_TICK)
        } else {
            current_tick
        };

        debug!(
            "[COMPUTE_SWAP_STEP] amount_out={}, fee_earned={}, new_price={}, new_tick={}",
            amount_out_step,
            fee_earned_step,
            sqrt_price_next.to_u256(),
            new_tick
        );

        (amount_out_step, fee_earned_step, sqrt_price_next, new_tick)
    }
}
