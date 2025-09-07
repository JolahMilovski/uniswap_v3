//! trade_simulator.rs
//! Модуль для симуляции арбитражных операций по путям Uniswap V3 с использованием флеш-займов Aave и возможных флеш-свапов Uniswap.
//!
//! # Основные возможности
//! - Обработка событий пулов через поток `PoolEventInfo`.
//! - Поиск оптимального размера флеш-займа из Aave для максимальной прибыли (многоуровневый алгоритм).
//! - Точная симуляция свапов по тиковой модели Uniswap V3 (без учёта газа и MEV).
//! - Фильтрация путей по минимальному порогу прибыли, заданному в `MIN_PROFIT_THRESHOLD_BY_TOKEN`.
//! - Поддержка дополнительного займа токенов через флеш-свапы из сторонних пулов с ограничением <90% их ликвидности.
//!
//! # Новый алгоритм поиска оптимальной суммы займа
//! 1. Проход: расчет для максимальной доступной суммы в Aave
//! 2. Проход: проверка на -5%, -10%, -15%, -20%, -25% от максимума
//! 3. Проход: детальный поиск вокруг лучшего значения из второго прохода 
//! 4. Проход: точный поиск с тиковой математикой в узком диапазоне
//!
//! # Логирование
//! - `warn!` — основные этапы расчетов: найденные интервалы, итерации, локальные максимумы.
//! - `debug!` — подробные записи: тиковая математика, шаги расчёта, промежуточные значения.
//! - `info!` — агрегированные данные: итоговые результаты.
//! - `error!` — ошибки и критические ситуации.

use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    path_builder::{ArbitragePath, BorrowPoolInfo, PathBuilder},
    uniswap_events::PoolEventInfo,
    uniswap_graph::{self, UniswapPool, UniversalGraph, Q64_96},
    uniswap_v3::tick_to_sqrt_price,
};

use arc_swap::ArcSwap;
use colored::Colorize;
use ethers::{types::{Address, U256}, utils::hex};
use lazy_static::lazy_static;
use num_traits::{SaturatingAdd, SaturatingSub};
use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use tokio::{sync::mpsc::Receiver as MpscReceiver};
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
}

/// ==== Константы ====
const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;

const SAFE_FSWAP_FRACTION_NUM: u128 = 9;
const SAFE_FSWAP_FRACTION_DEN: u128 = 10;

const AAVE_FLASH_FEE_NUM: u128 = 9;
const AAVE_FLASH_FEE_DEN: u128 = 10_000;

/// ==== Утилиты для работы с U256 / Q-форматами ====

#[inline]
fn pow10_u256(n: u32) -> U256 {
    let mut r = U256::one();
    let ten = U256::from(10u8);
    for _ in 0..n { r = r * ten; }
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
                debug!("[  MUL_DIV_U256] Успешное вычисление: ({} * {}) / {} = {}", a, b, c, result);
                result
            }
            None => {
                error!("[  MUL_DIV_U256] Переполнение при делении: prod={} / {}", prod, c);
                U256::zero()
            }
        },
        None => {
            let full = a.full_mul(b);
            let q = &full / c;
            match U256::try_from(q) {
                Ok(result) => {
                    debug!("[  MUL_DIV_U256] Успешное вычисление через full_mul: ({} * {}) / {} = {}", a, b, c, result);
                    result
                }
                Err(_) => {
                    error!("[  MUL_DIV_U256] Переполнение при умножении: a={} * b={}", a, b);
                    U256::zero()
                }
            }
        }
    }
}

/// 90% от значения, минус 1 wei.
#[inline]
fn cap_90pct(v: U256) -> U256 {
    let nine_tenths = mul_div_u256(v, U256::from(SAFE_FSWAP_FRACTION_NUM), U256::from(SAFE_FSWAP_FRACTION_DEN));
    if nine_tenths > U256::zero() { nine_tenths - U256::one() } else { U256::zero() }
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

#[derive(Clone)]
struct RouteState {
    hops: Vec<HopState>,
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
            "[{}] Инициализация симулятора: путей={}, пулов в путях={}",
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

        debug!("[  {}] ▶️ Запуск симулятора", "TRADE_SIMULATOR ⚡".green());

        while let Some(event) = simulator_rx.recv().await {

            let simulator = self.clone();

            tokio::spawn(async move {
                if let Err(e) = simulator.process_trade_event(event).await {
                    error!("[  {}] Ошибка process_trade_event: {:?}", "TRADE_SIMULATOR ⚡".green(), e);
                }
            });
        }
        warn!("[  {}] ▶️ Симулятор остановлен (входной канал закрыт)", "TRADE_SIMULATOR ⚡".green());
    }

    /// Создаёт снимок графа (pools, tick_maps, token_decimals)
    fn create_graph_snapshot(&self, route_pools: &[Address], borrow_pools: &[BorrowPoolInfo], event_id: &str) -> Arc<GraphSnapshotHolder> {
        
        warn!("[  CREATE_GRAPH_SNAPSHOT] 🆔 = {} Создание снимка графа для пулов: {:?}", event_id, route_pools);
        let mut pool_set: HashSet<Address> = HashSet::new();
        for address in route_pools { pool_set.insert(*address); }
        for borrow_pool_info in borrow_pools { pool_set.insert(borrow_pool_info.pool_address); }

        let graph = self.graph.load();

        let mut snapshot = GraphSnapshotHolder {
            pools: HashMap::new(),
            tick_maps: HashMap::new(),
            token_decimals: HashMap::new(),
        };

        for addr in pool_set {
            if let Some(pool) = graph.edges.get(&addr) {
                snapshot.pools.insert(addr, pool.clone());
                let mut tick_map: Vec<(i32, i128, U256)> = pool.tick_map.iter().map(|(k, v)| (*k, v.0, v.1)).collect();
                tick_map.sort_by_key(|(t, _, _)| *t);
                snapshot.tick_maps.insert(addr, tick_map);
                snapshot.token_decimals.entry(Address::from_slice(&pool.uniswap_token_a.to_fixed_bytes()))
                    .or_insert(pool.uniswap_token_a_decimals);
                snapshot.token_decimals.entry(Address::from_slice(&pool.uniswap_token_b.to_fixed_bytes()))
                    .or_insert(pool.uniswap_token_b_decimals);
            }
        }

        warn!("[  CREATE_GRAPH_SNAPSHOT] 🆔 = {} Снимок: пулов = {}, tick_maps = {}, decimals = {}", event_id, snapshot.pools.len(), snapshot.tick_maps.len(), snapshot.token_decimals.len());
        Arc::new(snapshot)
    }


    /// ОСНОВНАЯ ФУНКЦИЯ СИМУЛЯЦИИ СВАПА
    fn simulate_swap(
        &self,
        pool_addr: &Address,
        amount_in: U256, // Входная сумма в НАТИВНЫХ decimals токена
        zero_for_one: bool,
        event_id: &str,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Option<U256> {
        let pool = snapshot.pools.get(pool_addr)?;
        let tick_map = snapshot.tick_maps.get(pool_addr)?;

        // ОПРЕДЕЛЯЕМ DECIMALS ВХОДНОГО И ВЫХОДНОГО ТОКЕНОВ
        let (in_token_decimals, out_token_decimals) = if zero_for_one {
            (pool.uniswap_token_a_decimals, pool.uniswap_token_b_decimals)
        } else {
            (pool.uniswap_token_b_decimals, pool.uniswap_token_a_decimals)
        };

        debug!("[SWAP] {}/{}: {} ({}d) -> {} ({}d)", 
               event_id, path_index,
               if zero_for_one { "tokenA" } else { "tokenB" }, in_token_decimals,
               if zero_for_one { "tokenB" } else { "tokenA" }, out_token_decimals);

        // НОРМАЛИЗУЕМ ВХОДНУЮ СУММУ К 18 DECIMALS
        let amount_in_normalized = Self::normalize_amount(amount_in, in_token_decimals);

        // ВЫЧИСЛЯЕМ СВАП В 18 DECIMALS
        let amount_out_normalized = self.compute_amount_out(
            pool.uniswap_sqrt_price,
            pool.uniswap_liquidity.as_u128(),
            tick_map,
            pool.uniswap_tick_current,
            pool.uniswap_fee_tier,
            amount_in_normalized, // 18 decimals
            zero_for_one,
            event_id,
            path_index,
            *pool_addr,
        );

        // ДЕНОРМАЛИЗУЕМ РЕЗУЛЬТАТ К НАТИВНЫМ DECIMALS ВЫХОДНОГО ТОКЕНА
        let amount_out = Self::denormalize_amount(amount_out_normalized, out_token_decimals);

        debug!("[SWAP] Результат: {} -> {} (норм: {} -> {})",
               amount_in, amount_out, amount_in_normalized, amount_out_normalized);

        Some(amount_out)
    }

    /// ВЫЧИСЛЕНИЕ АРБИТРАЖНОЙ ПРИБЫЛИ С УЧЁТОМ DECIMALS
async fn compute_arbitrage_profit(
    &self,
    path: &ArbitragePath,
    amount: U256,
    event_id: &str,
    path_index: &str,
    snapshot: &Arc<GraphSnapshotHolder>,
) -> Option<(U256, U256, bool)> {
    debug!("[COMPUTE_ARBITRAGE_PROFIT] 🆔 ={} путь={} amount={}", event_id, path_index, amount);
    
    let base_token = path.tokens.first().copied().unwrap_or_default();
    let base_decimals = snapshot.token_decimals.get(&base_token).copied().unwrap_or(18);
    
    // Нормализуем входную сумму к 18 decimals
    let mut current_amount = Self::normalize_amount(amount, base_decimals);
    let mut used_fswap = false;
    let mut total_fswap_fee = U256::zero();

    for (i, pool_addr) in path.pools.iter().enumerate() {
        if !snapshot.pools.contains_key(pool_addr) {
            debug!("[COMPUTE_ARBITRAGE_PROFIT] пул {:?} отсутствует в snapshot", pool_addr);
            return None;
        }
        
        let pool = snapshot.pools.get(pool_addr).expect("checked exists");
        let token_in = path.tokens[i];
        let token_out = path.tokens[i + 1];
        
        // Получаем decimals для точного расчета
        let in_decimals = snapshot.token_decimals.get(&token_in).copied().unwrap_or(18);
        let out_decimals = snapshot.token_decimals.get(&token_out).copied().unwrap_or(18);
        
        // Денормализуем к нативным decimals пула
        let amount_in_denormalized = Self::denormalize_amount(current_amount, in_decimals);
        
        let zero_for_one = if pool.uniswap_token_a == token_in.into() {
            true
        } else if pool.uniswap_token_b == token_in.into() {
            false
        } else {
            debug!("[COMPUTE_ARBITRAGE_PROFIT] несоответствие токенов в пуле {:?}", pool_addr);
            return None;
        };

        // Симулируем свап с нативными decimals
        let amount_out_denormalized = self.simulate_swap(
            pool_addr,
            amount_in_denormalized,
            zero_for_one,
            event_id,
            path_index,
            snapshot,
        )?;

        // Нормализуем результат к 18 decimals для дальнейших вычислений
        let amount_out_normalized = Self::normalize_amount(amount_out_denormalized, out_decimals);

        if amount_out_normalized.is_zero() {
            debug!("[COMPUTE_ARBITRAGE_PROFIT] нулевой выход для пула {:?}", pool_addr);
            return None;
        }

        // Проверяем ликвидность для следующего хопа (если есть)
        if i < path.pools.len() - 1 {
            let next_token = path.tokens[i + 1];
            let next_pool_addr = path.pools[i + 1];
            
            // Денормализуем для проверки ликвидности в нативных decimals
            let next_token_decimals = snapshot.token_decimals.get(&next_token).copied().unwrap_or(18);
            let amount_out_denormalized_for_check = Self::denormalize_amount(amount_out_normalized, next_token_decimals);
            
            let can_borrow = self.compute_uniswap_borrow_amount(
                next_token,
                next_pool_addr,
                amount_out_denormalized_for_check,
                event_id,
                snapshot,
            ).await;

            if !can_borrow {
                debug!("[COMPUTE_ARBITRAGE_PROFIT] недостаточно ликвидности для следующего хопа, пул={:?}, токен={:?}", next_pool_addr, next_token);
                let (fswap_fee, covered) = self.estimate_flashswap_cover_cost(
                    next_token,
                    amount_out_denormalized_for_check,
                    &path.pools,
                    event_id,
                    path_index,
                    snapshot,
                );
                if !covered {
                    debug!("[COMPUTE_ARBITRAGE_PROFIT] флеш-свап не покрыл дефицит для токена {:?}", next_token);
                    return None;
                }
                used_fswap = true;
                total_fswap_fee = total_fswap_fee.saturating_add(fswap_fee);
                debug!("[COMPUTE_ARBITRAGE_PROFIT] флеш-свап успешен для токена {:?}, комиссия={}", next_token, fswap_fee);
            }
        }

        current_amount = amount_out_normalized;
        debug!("[COMPUTE_ARBITRAGE_PROFIT] хоп {}: amount_out={} (норм: {})", i, amount_out_denormalized, amount_out_normalized);
    }

    // Денормализуем финальную сумму к decimals базового токена для расчета прибыли
    let final_amount_denormalized = Self::denormalize_amount(current_amount, base_decimals);
    let amount_denormalized = Self::denormalize_amount(amount, base_decimals);

    let profit = if final_amount_denormalized > amount_denormalized {
        final_amount_denormalized.saturating_sub(amount_denormalized).saturating_sub(total_fswap_fee)
    } else {
        U256::zero()
    };

    debug!("[COMPUTE_ARBITRAGE_PROFIT] 🆔 ={} путь={} profit={}, final_amount={}, used_fswap={}, total_fswap_fee={}", 
           event_id, path_index, profit, final_amount_denormalized, used_fswap, total_fswap_fee);
    
    if profit.is_zero() {
        None
    } else {
        Some((profit, final_amount_denormalized, used_fswap))
    }
}
    /// НОРМАЛИЗАЦИЯ СУММЫ К 18 DECIMALS
    fn normalize_amount(amount: U256, decimals: u8) -> U256 {
        if decimals == 18 {
            amount
        } else if decimals > 18 {
            // Уменьшаем precision: USDT (6d) -> 18d: amount * 10^(18-6)
            amount * pow10_u256((18 - decimals) as u32)
        } else {
            // Увеличиваем precision: USDC (6d) -> 18d: amount * 10^(18-6)
            amount * pow10_u256((18 - decimals) as u32)
        }
    }

    /// ДЕНОРМАЛИЗАЦИЯ СУММЫ К ЦЕЛЕВЫМ DECIMALS
    fn denormalize_amount(amount: U256, target_decimals: u8) -> U256 {
        if target_decimals == 18 {
            amount
        } else if target_decimals > 18 {
            // Увеличиваем precision: 18d -> USDT (6d): amount / 10^(18-6)
            amount / pow10_u256((18 - target_decimals) as u32)
        } else {
            // Уменьшаем precision: 18d -> USDC (6d): amount / 10^(18-6)
            amount / pow10_u256((18 - target_decimals) as u32)
        }
    }


    /// Вычисляет предпочтения для займа из пула.
    pub fn calculate_borrow_preferences(
        &self,
        fee_tier: u32,
        sqrt_price_x96: Q64_96,
        token0: Address,
        token1: Address,
        event_id: &str,
        path_index: &str,
    ) -> (bool, Address) {
        debug!("[  TRADE_SIMULATOR] 🆔 = {} путь = {} Расчёт предпочтений для займа: fee_tier={}", event_id, path_index, fee_tier);
        let borrow_amount = U256::from(1_000_000_000_000_000_000u64); // 1 ETH в wei
        let fee_same = borrow_amount * U256::from(fee_tier) / U256::from(1_000_000);
        let fee_paired = borrow_amount * U256::from(fee_tier) * 2 / U256::from(1_000_000);
        let price_paired = (sqrt_price_x96.to_u256() * sqrt_price_x96.to_u256()) / U256::from(2).pow(U256::from(192));
        let fee_paired_converted = if price_paired.is_zero() {
            U256::max_value()
        } else {
            fee_paired / price_paired
        };
        let prefer_same_token = fee_same <= fee_paired_converted;
        let repay_token = if prefer_same_token { token0 } else { token1 };
        debug!(
            "[TRADE_SIMULATOR] 🆔 = {} путь = {} Расчёт предпочтений: prefer_same_token={}, repay_token={:?}",
            event_id, path_index, prefer_same_token, repay_token
        );
        (prefer_same_token, repay_token)
    }

    /// Оценивает стоимость флеш-свапа для покрытия дефицита token.
    fn estimate_flashswap_cover_cost(
        &self,
        needed_token: Address,
        needed_amount: U256,
        route_pools: &[Address],
        event_id: &str,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> (U256, bool) {
        debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} токен = {:?}, сумма = {}", event_id, path_index, needed_token, needed_amount);
        let candidates: Vec<BorrowPoolInfo> = self
            .route_builder
            .borrow_pools
            .get(&needed_token)
            .map(|v| v.iter().cloned().filter(|bp| !route_pools.contains(&bp.pool_address)).collect())
            .unwrap_or_default();

        let mut remaining = needed_amount;
        let mut total_fee_cost = U256::zero();

        for cand in candidates {
            if remaining.is_zero() { break; }
            if !snapshot.pools.contains_key(&cand.pool_address) {
                debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} пул {:?} отсутствует", event_id, path_index, cand.pool_address);
                continue;
            }
            let pool = snapshot.pools.get(&cand.pool_address).expect("checked exists");

            let (prefer_same_token, repay_token) = self.calculate_borrow_preferences(
                cand.fee_tier,
                pool.uniswap_sqrt_price,
                *pool.uniswap_token_a,
                *pool.uniswap_token_b,
                event_id,
                path_index,
            );

            let cap = if prefer_same_token && pool.uniswap_token_a == needed_token.into() {
                cap_90pct(pool.liquidity_token_a)
            } else if !prefer_same_token && pool.uniswap_token_b == needed_token.into() {
                cap_90pct(pool.liquidity_token_b)
            } else {
                U256::zero()
            };
            if cap.is_zero() {
                debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} нулевая ликвидность для токена в пуле {:?}", event_id, path_index, cand.pool_address);
                continue;
            }

            let take = remaining.min(cap);
            if take.is_zero() {
                debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} нулевая сумма для займа из пула {:?}", event_id, path_index, cand.pool_address);
                continue;
            }

            let fee_cost = mul_div_u256(take, U256::from(cand.fee_tier as u128), U256::from(1_000_000u128));
            total_fee_cost = total_fee_cost.saturating_add(fee_cost);
            remaining = remaining.saturating_sub(take);

            debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} пул = {:?}, взято = {}, комиссия = {}, repay_token = {:?}", 
                   event_id, path_index, cand.pool_address, take, fee_cost, repay_token);
        }

        debug!("[  ESTIMATE_FSWAP_COST] 🆔 = {} путь = {} итог: комиссия={}, покрыто={}", event_id, path_index, total_fee_cost, remaining.is_zero());
        (total_fee_cost, remaining.is_zero())
    }

    /// Проверяет возможность займа токена из пула Uniswap (90% cap)
    async fn compute_uniswap_borrow_amount(
        &self,
        token: Address,
        pool_address: Address,
        amount: U256,
        event_id: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> bool {
        debug!("[  COMPUTE_BORROW_AMOUNT_UNISWAP] 🆔 ={} токен={:?}, пул={:?}, сумма={}", event_id, token, pool_address, amount);

        if !snapshot.pools.contains_key(&pool_address) {
            debug!("[  COMPUTE_BORROW_AMOUNT_UNISWAP] 🆔 ={} пул {:?} отсутствует", event_id, pool_address);
            return false;
        }
        let pool = snapshot.pools.get(&pool_address).expect("checked exists");

        let (prefer_same_token, _repay_token) = self.calculate_borrow_preferences(
            pool.uniswap_fee_tier,
            pool.uniswap_sqrt_price,
            *pool.uniswap_token_a,
            *pool.uniswap_token_b,
            event_id,
            "",
        );

        let available = if prefer_same_token && pool.uniswap_token_a == token.into() {
            cap_90pct(pool.liquidity_token_a)
        } else if !prefer_same_token && pool.uniswap_token_b == token.into() {
            cap_90pct(pool.liquidity_token_b)
        } else {
            U256::zero()
        };

        let result = available >= amount;
        debug!("[  COMPUTE_BORROW_AMOUNT_UNISWAP] 🆔 = {} доступно = {}, требуется = {}, результат = {}", event_id, available, amount, result);
        result
        
    }

    /// Обрабатывает событие пула
    pub async fn process_trade_event(&self, event: PoolEventInfo) -> Result<(), String> {

        let event_id = &event.event_id.to_string();

        debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} Начало обработки события для пула {:?}", event_id, event.address);

        let pool_address = event.address;
        debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} Всего путей в route_builder: {}, pool_to_paths: {}", event_id, self.route_builder.paths.len(), self.route_builder.pool_to_paths.len());

        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
        if aave_liquidity.token_info.is_empty() {
            debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} пустая ликвидность Aave — пропуск", event_id);
            return Err("Empty Aave liquidity".to_string());
        }

        let route_indices = self
            .route_builder
            .pool_to_paths
            .get(&pool_address)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} Пул {} связан с путями: {:?}", event_id, pool_address, route_indices);

        // Собираем все пулы из маршрутов
        let mut route_pools: HashSet<Address> = HashSet::new();
        for route_index in &route_indices {
            if let Some(route) = self.route_builder.paths.get(*route_index) {
                route_pools.extend(route.pools.iter().copied());
            }
        }
        let route_pools: Vec<Address> = route_pools.into_iter().collect();

        let snapshot = self.create_graph_snapshot(&route_pools, &[], &event_id);

        let mut filtered_indices = Vec::new();

        for route_index in route_indices {

            let path_index = format!("{}-{}", event_id, route_index);
            if let Some(route) = self.route_builder.paths.get(route_index) {
                let base_token = route.tokens.first().copied().unwrap_or_default();
                if !aave_liquidity.token_info.contains_key(&base_token) {
                    debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} путь = {} база {:?} не в Aave — пропуск", event_id, path_index, base_token);
                    continue;
                }

                let mut ok = true;
                for p in &route.pools {
                    if !snapshot.pools.contains_key(p) {
                        debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} путь = {} пул {:?} отсутствует в snapshot — пропуск", event_id, path_index, p);
                        ok = false; break;
                    }
                }
                if snapshot.token_decimals.get(&base_token).is_none() {
                    debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} путь = {} decimals базового токена {:?} отсутствуют — пропуск", event_id, path_index, base_token);
                    ok = false;
                }

                if ok { filtered_indices.push(route_index); }
            }
        }

        debug!("[  PROCESS_TRADE_EVENT] 🆔 = {} найдено путей = {}", event_id, filtered_indices.len());

        let results = self.simulate_all_paths_max_profit(&filtered_indices, &event_id, &snapshot).await;
        
        let filtered_results = self.filter_by_min_profit_threshold(results);

        if filtered_results.is_empty() {
            info!("[  PROCESS_TRADE_EVENT] 🆔 = {} нет прибыльных путей", event_id);
            return Ok(());
        }
        let f_final = self.select_final_arbitrage_opportunities(filtered_results, &event_id);

        for result in &f_final {
            warn!("[  PROCESS_TRADE_EVENT] 🆔 = {} выбран путь = {} займ = {} прибыль = {} итог = {}", event_id, result.path_index, result.borrow_optimal, result.profit_net, result.final_amount);
        }

        Ok(())
    }

    /// Симулирует все пути с максимальной прибылью
    async fn simulate_all_paths_max_profit(
        &self,
        route_indices: &[usize],
        event_id: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Vec<PathSimulationResult> {
        let mut results = Vec::new();
        for route_index in route_indices {
            if let Some(route) = self.route_builder.paths.get(*route_index) {
                let path_index = format!("{}-{}", event_id, route_index);
                if let Some((borrow_optimal, profit_net, final_amount, used_fswap)) =
                    self.compute_aave_borrow_amount(&route, event_id, &path_index, snapshot).await
                {
                    results.push(PathSimulationResult {
                        path_index: *route_index,
                        base_token: route.tokens.first().copied().unwrap_or_default(),
                        borrow_optimal,
                        final_amount,
                        profit_net,
                        used_uniswap_flash_supplement: used_fswap,
                    });
                }
            }
        }
        results
    }

    /// Вычисляет оптимальную сумму займа из Aave.
    async fn compute_aave_borrow_amount(
        &self,
        path: &ArbitragePath,
        event_id: &str,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Option<(U256, U256, U256, bool)> {
        let base_token = path.tokens.first().copied().unwrap_or_default();
        warn!("[  COMPUTE_AAVE_BORROW_AMOUNT] 🆔 ={} путь={} начало расчета", event_id, path_index);

        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
        if let Some((_, virtual_balance)) = aave_liquidity.token_info.get(&base_token) {
            let max_borrow = *virtual_balance;
            warn!("[  COMPUTE_AAVE_BORROW_AMOUNT] доступно Aave: {} для {:?}", max_borrow, base_token);

            let res = self.multi_level_borrow_optimization(path, max_borrow, event_id, path_index, snapshot).await;
            if let Some((opt, profit, final_amount, used_fswap)) = res {
                let fee = mul_div_u256(opt, U256::from(AAVE_FLASH_FEE_NUM), U256::from(AAVE_FLASH_FEE_DEN));
                let profit_after_fee = profit.saturating_sub(fee);
                warn!("[  COMPUTE_AAVE_BORROW_AMOUNT] найден optimum = {} profit = {} fee = {} profit_after_fee = {}", opt, profit, fee, profit_after_fee);

                if profit_after_fee.is_zero() {
                    debug!("[  COMPUTE_AAVE_BORROW_AMOUNT] прибыль после комиссии нулевая");
                    None
                } else {
                    Some((opt, profit_after_fee, final_amount, used_fswap))
                }
            } else {
                debug!("[  COMPUTE_AAVE_BORROW_AMOUNT] оптимизация не нашла прибыльных вариантов");
                None
            }
        } else {
            debug!("[  COMPUTE_AAVE_BORROW_AMOUNT] токен {:?} отсутствует в Aave", base_token);
            None
        }
    }

    /// Многоуровневая оптимизация суммы займа по новому алгоритму
    async fn multi_level_borrow_optimization(
        &self,
        path: &ArbitragePath,
        max_borrow: U256,
        event_id: &str,
        path_index: &str,
        snapshot: &Arc<GraphSnapshotHolder>,
    ) -> Option<(U256, U256, U256, bool)> {
        let base_token = path.tokens.first().copied().unwrap_or_default();
        let base_decimals = match snapshot.token_decimals.get(&base_token) {
            Some(&d) => d,
            None => {
                warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} отсутствуют decimals для базового токена {:?} — отказ", event_id, path_index, base_token);
                return None;
            }
        };

        let min_borrow = pow10_u256(base_decimals as u32);
        if max_borrow < min_borrow {
            warn!("[  MULTI_LEVEL_OPTIMIZATION] max_borrow < min_borrow — отказ");
            return None;
        }

        warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} начало оптимизации, диапазон: {} - {}", event_id, path_index, min_borrow, max_borrow);

        // 1. Первый проход: расчет для максимальной суммы
        let mut best_amount = U256::zero();
        let mut best_profit = U256::zero();
        let mut best_final = U256::zero();
        let mut best_fswap = false;

        if let Some((profit, final_amt, fswap)) = self.compute_arbitrage_profit(path, max_borrow, event_id, path_index, snapshot).await {
            best_amount = max_borrow;
            best_profit = profit;
            best_final = final_amt;
            best_fswap = fswap;
            warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 1: сумма={}, прибыль={}", event_id, path_index, max_borrow, profit);
        }

        // 2. Второй проход: проверка на -5%, -10%, -15%, -20%, -25%
        let reductions = [5, 10, 15, 20, 25];
        let mut best_reduction = 0;
        
        for &reduction in &reductions {
            let amount = mul_div_u256(max_borrow, U256::from(100 - reduction), U256::from(100));
            if amount < min_borrow {
                continue;
            }
            
            if let Some((profit, final_amt, fswap)) = self.compute_arbitrage_profit(path, amount, event_id, path_index, snapshot).await {
                warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 2: сокращение {}%, сумма={}, прибыль={}", event_id, path_index, reduction, amount, profit);
                
                if profit > best_profit {
                    best_amount = amount;
                    best_profit = profit;
                    best_final = final_amt;
                    best_fswap = fswap;
                    best_reduction = reduction;
                }
            }
        }

        // Если не нашли лучший вариант во втором проходе, возвращаем результат первого прохода
        if best_profit.is_zero() {
            return if best_amount.is_zero() {
                None
            } else {
                Some((best_amount, best_profit, best_final, best_fswap))
            };
        }

        // 3. Третий проход: детальный поиск вокруг лучшего значения из второго прохода (±5%)
        let center_percent = 100 - best_reduction;
        let range_start = center_percent.saturating_sub(&5);
        let range_end = center_percent.saturating_add(&5);
        
        warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 3: поиск в диапазоне {}% - {}%", event_id, path_index, range_start, range_end);
        
        for percent in range_start..=range_end {
            if percent == 0 { continue; }
            
            let amount = mul_div_u256(max_borrow, U256::from(percent), U256::from(100));
            if amount < min_borrow {
                continue;
            }
            
            if let Some((profit, final_amt, fswap)) = self.compute_arbitrage_profit(path, amount, event_id, path_index, snapshot).await {
                warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 3: {}%, сумма={}, прибыль={}", event_id, path_index, percent, amount, profit);
                
                if profit > best_profit {
                    best_amount = amount;
                    best_profit = profit;
                    best_final = final_amt;
                    best_fswap = fswap;
                }
            }
        }

        // 4. Четвертый проход: точный поиск с тиковой математикой в узком диапазоне
        let center_amount = best_amount;
        let step = center_amount / U256::from(100); // 1% от центрального значения
        
        if step > U256::zero() {
            let range_start = center_amount.saturating_sub(step * U256::from(5));
            let range_end = center_amount.saturating_add(step * U256::from(5));
            
            warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 4: точный поиск в диапазоне {} - {}", event_id, path_index, range_start, range_end);
            
            let mut current = range_start;
            while current <= range_end {
                if current < min_borrow {
                    current = current.saturating_add(step);
                    continue;
                }
                
                if let Some((profit, final_amt, fswap)) = self.compute_arbitrage_profit(path, current, event_id, path_index, snapshot).await {
                    warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} проход 4: сумма={}, прибыль={}", event_id, path_index, current, profit);
                    
                    if profit > best_profit {
                        best_amount = current;
                        best_profit = profit;
                        best_final = final_amt;
                        best_fswap = fswap;
                    }
                }
                
                current = current.saturating_add(step);
            }
        }

        if best_profit.is_zero() {
            None
        } else {
            warn!("[  MULTI_LEVEL_OPTIMIZATION] 🆔 ={} путь={} оптимизация завершена: лучшая сумма={}, прибыль={}", event_id, path_index, best_amount, best_profit);
            Some((best_amount, best_profit, best_final, best_fswap))
        }
    }


    /// Находит следующий тик ниже текущего
fn find_next_tick_below(current_tick: i32, tick_map: &[(i32, i128, U256)]) -> (i32, bool) {
        tick_map
            .iter()
            .rev()
            .find(|(t, _, _)| *t < current_tick)
            .map(|(t, _, _)| (*t, true))
            .unwrap_or((MIN_TICK, false))
    }

    /// Находит следующий тик выше текущего
fn find_next_tick_above(current_tick: i32, tick_map: &[(i32, i128, U256)]) -> (i32, bool) {
        tick_map
            .iter()
            .find(|(t, _, _)| *t > current_tick)
            .map(|(t, _, _)| (*t, true))
            .unwrap_or((MAX_TICK, false))
    }

    /// Фильтрует результаты по минимальному порогу прибыли
fn filter_by_min_profit_threshold(&self, results: Vec<PathSimulationResult>) -> Vec<PathSimulationResult> {
        results
            .into_iter()
            .filter(|res| {
                let threshold = MIN_PROFIT_THRESHOLD_BY_TOKEN
                    .get(&res.base_token)
                    .copied()
                    .unwrap_or_else(|| {
                        let default = U256::from(100_000_000_000_000_000u128);
                        warn!("[  FILTER_BY_MIN_PROFIT] для токена {:?} не найден порог, используется {}", res.base_token, default);
                        default
                    });

                if res.profit_net >= threshold {
                    true
                } else {
                    debug!("[  FILTER_BY_MIN_PROFIT] путь {} отфильтрован: прибыль {} < порога {}", res.path_index, res.profit_net, threshold);
                    false
                }
            })
            .collect()
    }

    /// Выбирает финальные арбитражные возможности
fn select_final_arbitrage_opportunities(
        &self,
        results: Vec<PathSimulationResult>,
        event_id: &str,
    ) -> Vec<PathSimulationResult> {
        let mut grouped: HashMap<usize, PathSimulationResult> = HashMap::new();

        for result in results {
            let entry = grouped.entry(result.path_index).or_insert_with(|| result.clone());
            if result.profit_net > entry.profit_net {
                *entry = result;
            }
        }

        let mut final_results: Vec<PathSimulationResult> = grouped.into_values().collect();
        final_results.sort_by(|a, b| b.profit_net.cmp(&a.profit_net));

        warn!("[  SELECT_FINAL_ARBITRAGE] 🆔 ={} отобрано {} путей", event_id, final_results.len());
        final_results
    }

/// Обновляет ликвидность при переходе через тик (статический метод)
fn update_liquidity_on_tick_cross(
        liquidity_current: u128,
        tick: i32,
        tick_map: &[(i32, i128, U256)],
        zero_for_one: bool,
    ) -> u128 {
        if let Some((_, liq_delta, _)) = tick_map.iter().find(|(t, _, _)| *t == tick) {
            let d = liq_delta.unsigned_abs() as u128; // Явная конвертация в u128
            if zero_for_one {
                liquidity_current.saturating_sub(d)
            } else {
                liquidity_current.saturating_add(d)
            }
        } else {
            liquidity_current
        }
    }
   
fn input_to_next_tick_local(hop: HopState) -> Option<U256> {
    debug!("[INPUT_TO_NEXT_TICK] пул={:?}, tick={}, zero_for_one={}", hop.pool_addr, hop.tick_current, hop.zero_for_one);
    if hop.liquidity.is_zero() || hop.sqrt_price_x96.to_u256().is_zero() || hop.sqrt_target_x96.to_u256().is_zero() {
        debug!("[INPUT_TO_NEXT_TICK] нулевые параметры — возврат None");
        return None;
    }

    let q96 = Q64_96::from_u256(U256::from(1u128 << 96)).unwrap_or_default();
    let amount_in = if hop.zero_for_one {
        // Для zero_for_one: цена уменьшается, sqrt_price_x96 > sqrt_target_x96
        if hop.sqrt_price_x96 <= hop.sqrt_target_x96 {
            debug!("[INPUT_TO_NEXT_TICK] некорректные цены: sqrt_price_x96 <= sqrt_target_x96");
            return None;
        }
        let price_diff = hop.sqrt_price_x96.sub(hop.sqrt_target_x96).unwrap_or_default();
        let num = hop.liquidity * price_diff.to_u256();
        let denom = hop.sqrt_target_x96.mul(hop.sqrt_price_x96).unwrap_or_default().div(q96).unwrap_or_default();
        mul_div_u256(num, U256::one(), denom.to_u256().max(U256::one()))
    } else {
        // Для !zero_for_one: цена увеличивается, sqrt_price_x96 < sqrt_target_x96
        if hop.sqrt_price_x96 >= hop.sqrt_target_x96 {
            debug!("[INPUT_TO_NEXT_TICK] некорректные цены: sqrt_price_x96 >= sqrt_target_x96");
            return None;
        }
        let price_diff = hop.sqrt_target_x96.sub(hop.sqrt_price_x96).unwrap_or_default();
        let num = hop.liquidity * q96.to_u256();
        let denom = price_diff.to_u256();
        mul_div_u256(num, U256::one(), denom.max(U256::one()))
    };

    let fee = mul_div_u256(amount_in, U256::from(hop.fee_pips), U256::from(1_000_000u128));
    let amount_in_with_fee = amount_in.saturating_add(fee);

    debug!("[INPUT_TO_NEXT_TICK] результат: amount_in={}, fee={}, total={}", amount_in, fee, amount_in_with_fee);
    if amount_in_with_fee.is_zero() {
        None
    } else {
        Some(amount_in_with_fee)
    }
}

/// Выполняет шаг свопа в пуле, рассчитывая вход, выход и новую цену.
fn compute_swap_step(
    &self,
    sqrt_price_current: Q64_96,
    sqrt_price_target: Q64_96,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: u32,
    zero_for_one: bool,
    event_id: &str,
    path_index: &str,
    _tick_current: i32,
    _pool_addr: Address,
) -> (U256, U256, Q64_96) {
    if liquidity.is_zero() || amount_remaining.is_zero() || sqrt_price_current.to_u256().is_zero() || sqrt_price_target.to_u256().is_zero() {
        debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} нулевые входные параметры — возврат (0, 0, текущая цена)", event_id, path_index);
        return (U256::zero(), U256::zero(), sqrt_price_current);
    }
    if liquidity < U256::from(1000) || amount_remaining < U256::from(1000) {
        debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} ликвидность или остаток < 1000 — возврат (0, 0, текущая цена)", event_id, path_index);
        return (U256::zero(), U256::zero(), sqrt_price_current);
    }
    if sqrt_price_target.to_u256() == sqrt_price_current.to_u256() {
        debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} текущая и целевая цены равны — возврат (0, 0, текущая цена)", event_id, path_index);
        return (U256::zero(), U256::zero(), sqrt_price_current);
    }

    let max_in = Self::input_to_next_tick_local(HopState {
        sqrt_price_x96: sqrt_price_current,
        sqrt_target_x96: sqrt_price_target,
        liquidity,
        fee_pips,
        zero_for_one,
        tick_current: _tick_current,
        pool_addr: _pool_addr,
    }).unwrap_or(U256::zero());

    if max_in.is_zero() {
        debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} max_in=0 — возврат (0, 0, текущая цена)", event_id, path_index);
        return (U256::zero(), U256::zero(), sqrt_price_current);
    }

    let amount_in = amount_remaining.min(max_in);
    let fee = mul_div_u256(amount_in, U256::from(fee_pips), U256::from(1_000_000u128));
    let in_after_fee = amount_in.saturating_sub(fee);

    let new_price = if in_after_fee >= max_in && !max_in.is_zero() {
        sqrt_price_target
    } else {
        if zero_for_one {
            let q64 = Q64_96::from_u256(U256::from(1u128 << 96)).map_err(|e| format!("Q64_96 conversion error: {}", e)).unwrap_or_default();
            let in_after_fee_q96 = Q64_96::from_u256(in_after_fee).unwrap_or_default();
            let num = in_after_fee_q96
                .mul(sqrt_price_current).unwrap_or_default()
                .mul(sqrt_price_target).unwrap_or_default()
                .div(q64).unwrap_or_default();
            let denom_add = in_after_fee_q96
                .mul(sqrt_price_target).unwrap_or_default()
                .div(q64).unwrap_or_default();
            let denom = Q64_96::from_u256(liquidity).unwrap_or_default()
                .add(denom_add).unwrap_or_default();
            let sub = num.div(denom).unwrap_or_default();
            sqrt_price_current.sub(sub).unwrap_or_default()
        } else {
            let q96 = Q64_96::from_u256(U256::from(1u128 << 96)).map_err(|e| format!("Q64_96 conversion error: {}", e)).unwrap_or_default();
            let delta = Q64_96::from_u256(in_after_fee).unwrap_or_default()
                .mul(q96).unwrap_or_default()
                .div(Q64_96::from_u256(liquidity).unwrap_or_default()).unwrap_or_default();
            sqrt_price_current.add(delta).unwrap_or_default()
        }
    };

    let out = if zero_for_one {
        if new_price.to_u256().is_zero() {
            debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} новая цена = 0 — out=0", event_id, path_index);
            U256::zero()
        } else {
            let price_change = sqrt_price_current.sub(new_price).unwrap_or_default().to_u256();
            mul_div_u256(liquidity * price_change, U256::one(), new_price.to_u256())
        }
    } else {
        if new_price.to_u256() > sqrt_price_current.to_u256() {
            let numerator = liquidity * new_price.sub(sqrt_price_current).unwrap_or_default().to_u256();
            let denominator = mul_div_u256(
                new_price.to_u256() * sqrt_price_current.to_u256(),
                U256::one(),
                U256::from(1u128 << 96)
            );
            mul_div_u256(numerator, U256::one(), denominator.max(U256::one()))
        } else {
            debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} новая цена <= текущая — out=0", event_id, path_index);
            U256::zero()
        }
    };

    debug!("[COMPUTE_SWAP_STEP] 🆔 ={} путь={} шаг: in={}, out={}, new_price={}.{}, fee={}", 
           event_id, path_index, amount_in, out, new_price.integer_part(), new_price.fractional_part(), fee);
    (amount_in, out, new_price)
}

/// Находит следующий инициализированный тик
fn find_next_initialized_tick(
    tick_current: i32,
    zero_for_one: bool,
    tick_map: &[(i32, i128, U256)],
) -> i32 {
    if zero_for_one {
        let (next_tick, found) = TradeSimulator::find_next_tick_below(tick_current, tick_map);
        if found {
            next_tick
        } else {
            MIN_TICK
        }
    } else {
        let (next_tick, found) = TradeSimulator::find_next_tick_above(tick_current, tick_map);
        if found {
            next_tick
        } else {
            MAX_TICK
        }
    }
}

    /// Симулирует своп в одном пуле Uniswap V3, возвращая выходную сумму.
fn compute_amount_out(
        &self,
        sqrt_price_x96: Q64_96,
        liquidity: u128,
        tick_map: &[(i32, i128, U256)],
        current_tick: i32,
        fee_tier: u32,
        amount_in: U256,
        zero_for_one: bool,
        event_id: &str,
        path_index: &str,
        pool_addr: Address,
    ) -> U256 {
        if liquidity == 0 || amount_in.is_zero() || tick_map.is_empty() {
            debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} нулевая ликвидность, вход или tick_map — возврат 0", event_id, path_index);
            return U256::zero();
        }

        let mut sqrt_price_current = sqrt_price_x96;
        let mut liquidity_active = U256::from(liquidity);
        let mut amount_remaining = amount_in;
        let mut amount_out_total = U256::zero();
        let mut tick_current = current_tick;

        debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} старт: amount_in={}, tick={}, zero_for_one={}", event_id, path_index, amount_in, tick_current, zero_for_one);

        while !amount_remaining.is_zero() && !liquidity_active.is_zero() {
            let tick_next = Self::find_next_initialized_tick(tick_current, zero_for_one, tick_map);
            if (zero_for_one && tick_next <= MIN_TICK) || (!zero_for_one && tick_next >= MAX_TICK) {
                debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} достигнут предел тиков: tick_next={}", event_id, path_index, tick_next);
                break;
            }

            let sqrt_price_next = tick_to_sqrt_price(tick_next).unwrap_or(uniswap_graph::Q64_96 { value: U256::zero() });
            if sqrt_price_next.to_u256().is_zero() {
                debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} нулевая sqrt_price_next для tick={}", event_id, path_index, tick_next);
                break;
            }

            let (amt_in_step, amt_out_step, sp_new) = self.compute_swap_step(
                sqrt_price_current,
                sqrt_price_next,
                liquidity_active,
                amount_remaining,
                fee_tier,
                zero_for_one,
                event_id,
                path_index,
                tick_current,
                pool_addr,
            );

            if amt_in_step.is_zero() {
                debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} нулевой шаг входа — прерывание", event_id, path_index);
                break;
            }

            amount_remaining = amount_remaining.saturating_sub(amt_in_step);
            amount_out_total = amount_out_total.saturating_add(amt_out_step);
            sqrt_price_current = sp_new;

            let crossed = if zero_for_one { sp_new.to_u256() <= sqrt_price_next.to_u256() } else { sp_new.to_u256() >= sqrt_price_next.to_u256() };
            if crossed {
                liquidity_active = U256::from(Self::update_liquidity_on_tick_cross(liquidity_active.as_u128(), tick_next, tick_map, zero_for_one));
                tick_current = tick_next;
                debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} переход тика: tick={}, ликвидность={}", event_id, path_index, tick_current, liquidity_active);
                if liquidity_active.is_zero() {
                    debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} ликвидность исчерпана", event_id, path_index);
                    break;
                }
            }
        }

        debug!("[COMPUTE_AMOUNT_OUT] 🆔 ={} путь={} итог: amount_out={}", event_id, path_index, amount_out_total);
        amount_out_total
    }


}