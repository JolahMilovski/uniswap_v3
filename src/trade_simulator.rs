//! trade_simulator.rs
//!
//! Модуль для симуляции арбитражных операций по путям Uniswap V3 с использованием флеш-займов Aave и возможных флеш-свапов Uniswap.
//!
//! # Основные возможности
//! - Обработка событий пулов через поток `PoolEventInfo`.
//! - Поиск оптимального размера флеш-займа из Aave для максимальной прибыли (режим максимальной прибыли).
//! - Точная симуляция свапов по тиковой модели Uniswap V3 (без учёта газа и MEV).
//! - Фильтрация путей по минимальному порогу прибыли, заданному в `MIN_PROFIT_THRESHOLD_BY_TOKEN`.
//! - Поддержка дополнительного займа токенов через флеш-свапы из сторонних пулов с ограничением <90% их ликвидности.
//!
//! # Логирование
//! - `debug!` — подробные записи: тиковая математика, шаги расчёта, промежуточные значения.
//! - `info!` — агрегированные данные: найденные интервалы, итерации, локальные максимумы.
//! - `warn!` — итоговые статусы: прибыльный или неприбыльный путь, найденный максимум, отказы, недостаток ликвидности.
//!
//! # Разделение вычислений
//! - **GPU**: Чистые вычисления (`compute_swap_step`, `simulate_path_max_profit`, `simulate_all_paths_max_profit`) без фильтрации по порогам.
//! - **CPU**: Логика, фильтрация (`filter_by_min_profit_threshold`, `select_final_arbitrage_opportunities`), выполнение транзакций.

use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    path_builder::{ArbitragePath, PathBuilder, BorrowPoolInfo},
    uniswap_events::PoolEventInfo,
    uniswap_graph::{UniswapPool, UniversalGraph},
    uniswap_v3::tick_to_sqrt_price,
};

use arc_swap::ArcSwap;
use colored::Colorize;
use ethers::{types::{Address, U256}, utils::hex};
use lazy_static::lazy_static;
use std::{collections::{HashMap, HashSet}, env, sync::Arc};
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

            //Mai Stablecoin (MAI)
            m.insert(Address::from_slice(&hex::decode("3f56e0c36d275367b8c502090edf38289b3dea0d").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            //USD Coin (USDC)            
            m.insert(Address::from_slice(&hex::decode("af88d065e77c8cc2239327c5edb3a432268e5831").unwrap()), U256::from(100_000_000));
            //Bridged USDC (USDC.e)
            m.insert(Address::from_slice(&hex::decode("ff970a61a04b1ca14834a43f5de4533ebddb5cc8").unwrap()), U256::from(100_000_000));
            //Arbitrum (ARB)
            m.insert(Address::from_slice(&hex::decode("912ce59144191c1204e64559fe8253a0e49e6548").unwrap()), U256::from(345_000_000_000_000_000_000u128));
            //Renzo Restaked ETH (ezETH)
            m.insert(Address::from_slice(&hex::decode("2416092f143378750bb29b79ed961ab195cceea5").unwrap()), U256::from(38_000_000_000_000_000u128));
            //Wrapped Ether (WETH)
            m.insert(Address::from_slice(&hex::decode("82af49447d8a07e3bd95bd0d56f35241523fbab1").unwrap()), U256::from(38_000_000_000_000_000u128));
            //USD₮0 (USD₮0)
            m.insert(Address::from_slice(&hex::decode("fd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9").unwrap()), U256::from(100_000_000));
            //Wrapped BTC (WBTC)
            m.insert(Address::from_slice(&hex::decode("2f2a2543b76a4166549f7aab2e75bef0aefc5b0f").unwrap()), U256::from(100_000u128));
            //LUSD Stablecoin (LUSD)
            m.insert(Address::from_slice(&hex::decode("93b346b6bc2548da6a1e7d98e9a421b42541425b").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            //Wrapped eETH (weETH)
            m.insert(Address::from_slice(&hex::decode("35751007a407ca6feffe80b3cb397736d2cf4dbe").unwrap()), U256::from(38_489_000_000_000_000u128));
            //ChainLink Token (LINK)
            m.insert(Address::from_slice(&hex::decode("f97f4df75117a78c1a5a0dbb814af92458539fb4").unwrap()), U256::from(8_000_000_000_000_000_000u128));
            //Wrapped (wstETH)
            m.insert(Address::from_slice(&hex::decode("5979d7b546e38e414f7e9822514be443a4800529").unwrap()), U256::from(33_000_000_000_000_000u128));
            //KelpDao Restaked ETH (rsETH)
            m.insert(Address::from_slice(&hex::decode("4186bfc76e2e237523cbc30fd220fe055156b41f").unwrap()), U256::from(38_000_000_000_000_000u128));
            //GHO Token (GHO)
            m.insert(Address::from_slice(&hex::decode("7dff72693f6a4149b17e7c6314655f6a9f7c8b33").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            //Frax (FRAX)
            m.insert(Address::from_slice(&hex::decode("17fc002b466eec40dae837fc4be5c67993ddbd6f").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            //STASIS EURS Token (EURS)
            m.insert(Address::from_slice(&hex::decode("d22a58f79e9481d1a88e00c343885a588b34b68b").unwrap()), U256::from(10_000));
            //Dai Stablecoin (DAI)
            m.insert(Address::from_slice(&hex::decode("da10009cbd5d07dd0cecc66161fc93d7c9000da1").unwrap()), U256::from(100_000_000_000_000_000_000u128));
            //Rocket Pool ETH (rETH
            m.insert(Address::from_slice(&hex::decode("ec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8").unwrap()), U256::from(38_000_000_000_000_000u128));
            //Aave Token (AAVE)
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



/// Границы тиков Uniswap V3
const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;

/// Коэффициент безопасной доли для флеш-свапов Uniswap (< 90% ликвидности пула).
const SAFE_FSWAP_FRACTION_NUM: u128 = 9;
const SAFE_FSWAP_FRACTION_DEN: u128 = 10;

/// Комиссия Aave флеш-займа ~0.09% (9 / 10000) — фиксированная константа.
const AAVE_FLASH_FEE_NUM: u128 = 9;
const AAVE_FLASH_FEE_DEN: u128 = 10_000;

/// Минимальный снимок графа для GPU-вычислений.
#[derive(Clone)]
pub struct GraphSnapshot {
    /// Пул Uniswap V3 с данными о ликвидности и ценах.
    pools: HashMap<Address, UniswapPool>,
    /// Карта тиков в формате Vec для GPU-доступа (tick, delta_liquidity, sqrt_price).
    tick_maps: HashMap<Address, Vec<(i32, i128, U256)>>,
}

/// Результат симуляции по одному пути.
#[derive(Debug, Clone)]
pub struct PathSimulationResult {
    /// Индекс пути из PathBuilder.paths.
    path_index: usize,
    /// Стартовый токен пути.
    base_token: Address,
    /// Оптимальный размер займа из Aave.
    borrow_optimal: U256,
    /// Итоговая сумма после замыкания цикла (в базовом токене).
    final_amount: U256,
    /// Чистая прибыль (без учёта газа/MEV, с вычетом комиссий Aave и флеш-свапов).
    profit_net: U256,
    /// Использовался ли флеш-свап Uniswap для дополнительного займа.
    used_uniswap_flash_supplement: bool,
}

#[derive(Clone)]
pub struct TradeSimulator {
    route_builder: Arc<PathBuilder>,
    aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
    graph: Arc<ArcSwap<UniversalGraph>>,
}

impl TradeSimulator {
    /// Инициализирует симулятор арбитражных операций.
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

    /// Запускает основной цикл симулятора.
    pub async fn run(&mut self, mut simulator_rx: MpscReceiver<PoolEventInfo>) {
        debug!("[{}] ▶️ Запуск симулятора торговых операций", "TRADE_SIMULATOR ⚡".green());

        while let Some(event) = simulator_rx.recv().await {
            debug!(
                "[{}] ▶️ Получено событие пула: идентификатор={} адрес_пула={:?}",
                "TRADE_SIMULATOR ⚡".green(),
                event.event_id,
                event.address
            );

            if let Err(e) = self.process_trade_event(event).await {
                error!("[{}] ▶️ Ошибка обработки события пула: {:?}", "TRADE_SIMULATOR ⚡".green(), e);
            }
        }

        info!("[{}] ▶️ Входной канал закрыт — симулятор остановлен", "TRADE_SIMULATOR ⚡".green());
    }

    /// Обрабатывает событие пула Uniswap и ищет арбитражные возможности.
    pub async fn process_trade_event(&self, event: PoolEventInfo) -> Result<(), String> {
        let event_id = event.event_id.to_string();
        let pool_address = event.address;
        debug!(
            "[{}] ⚙️ 🆔 ={} пул={:?} начало обработки",
            "PROCESS_TRADE_EVENT".green(),
            event_id, pool_address
        );

        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
        if aave_liquidity.token_info.is_empty() {
            info!(
                "[{}] ⚙️ 🆔 ={} ликвидность Aave отсутствует — пропуск",
                "PROCESS_TRADE_EVENT".green(),
                event_id
            );
            return Ok(());
        }

        info!(
            "[{}] ⚙️ 🆔 ={} ликвидность Aave: токенов={}",
            "PROCESS_TRADE_EVENT".green(),
            event_id,
            aave_liquidity.token_info.len()
        );

        let route_indices = self
            .route_builder
            .pool_to_paths
            .get(&pool_address)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();

        let mut filtered_indices = Vec::new();
        for route_index in route_indices {
            let path_index = format!("{}-{}", event_id, route_index);
            if let Some(route) = self.route_builder.paths.get(route_index) {
                let base_token = route.tokens.first().copied().unwrap_or_default();
                if !aave_liquidity.token_info.contains_key(&base_token) {
                    debug!(
                        "[{}] ⚙️ 🆔 ={} путь={} базовый токен {:?} недоступен в Aave — пропуск",
                        "PROCESS_TRADE_EVENT".green(),
                        event_id, path_index, base_token
                    );
                    continue;
                }

                // Проверка ликвидности для промежуточных токенов
                let mut can_borrow_all = true;
                for (i, token) in route.tokens.iter().skip(1).take(route.tokens.len().saturating_sub(2)).enumerate() {
                    let pool_address = route.pools[i];
                    if !self.compute_uniswap_borrow_amount(*token, pool_address, U256::from(1_000_000), &event_id).await {
                        debug!(
                            "[{}] ⚙️ 🆔 ={} путь={} недостаточно ликвидности для токена {:?} в пуле {:?}",
                            "PROCESS_TRADE_EVENT".green(),
                            event_id, path_index, token, pool_address
                        );
                        can_borrow_all = false;
                        break;
                    }
                }

                if can_borrow_all {
                    filtered_indices.push(route_index);
                }
            }
        }

        warn!(
            "[{}] ⚙️ 🆔 ={} найдено путей={}",
            "PROCESS_TRADE_EVENT".green(),
            event_id, filtered_indices.len()
        );

        let results = self.simulate_all_paths_max_profit(&filtered_indices, &event_id).await;

        let filtered_results = self.filter_by_min_profit_threshold(results);
        if filtered_results.is_empty() {
            warn!(
                "[{}] ⚙️ 🆔 ={} нет прибыльных путей после фильтрации",
                "PROCESS_TRADE_EVENT".green(),
                event_id
            );
            return Ok(());
        }

        let final_opportunities = self.select_final_arbitrage_opportunities(filtered_results, &event_id);
        for opportunity in final_opportunities {
            warn!(
                "[{}] ⚙️ 🆔 ={} путь={} выбран для выполнения: сумма_займа={}, прибыль={}, итоговая_сумма={}, использован_флеш_свап={}",
                "PROCESS_TRADE_EVENT".green(),
                event_id, opportunity.path_index, opportunity.borrow_optimal, opportunity.profit_net,
                opportunity.final_amount, opportunity.used_uniswap_flash_supplement
            );
        }

        info!(
            "[{}] ⚙️ 🆔 ={} обработка события завершена",
            "PROCESS_TRADE_EVENT".green(),
            event_id
        );
        Ok(())
    }

    /// Создаёт снимок графа для пулов пути и кандидатов флеш-свапа.
    fn create_graph_snapshot(&self, route_pools: &[Address], borrow_pools: &[BorrowPoolInfo]) -> GraphSnapshot {
        let pool_set: HashSet<Address> = route_pools.iter().copied().chain(borrow_pools.iter().map(|bp| bp.pool_address)).collect();
        let graph = self.graph.load();
        let mut snapshot = GraphSnapshot {
            pools: HashMap::new(),
            tick_maps: HashMap::new(),
        };

        for addr in pool_set {
            if let Some(pool) = graph.edges.get(&addr) {
                snapshot.pools.insert(addr, pool.clone());
                let mut tick_map: Vec<(i32, i128, U256)> = pool.tick_map.iter()
                    .map(|(k, v)| (*k, v.0, v.1))
                    .collect();
                tick_map.sort_by_key(|(tick, _, _)| *tick); // Сортировка тиков для бинарного поиска
                snapshot.tick_maps.insert(addr, tick_map);
            }
        }

        snapshot
    }

    /// Вычисляет оптимальную сумму займа из Aave.
    async fn compute_aave_borrow_amount(
        &self,
        path: &ArbitragePath,
        event_id: &str,
        path_index: &str,
    ) -> Option<(U256, U256, U256, bool)> {
        let base_token = path.tokens.first().copied().unwrap_or_default();
        debug!(
            "[{}] 🆔 ={} путь={} базовый_токен={:?} 🗺️ начало расчёта",
            "COMPUTE_AAVE_BORROW_AMOUNT".green(),
            event_id, path_index, base_token
        );

        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
        if let Some((_, virtual_balance)) = aave_liquidity.token_info.get(&base_token) {
            let max_borrow = *virtual_balance;
            warn!(
                "[{}] 🆔 ={} путь={} 🗺️ доступная ликвидность в Aave: {} для токена {:?}",
                "COMPUTE_AAVE_BORROW_AMOUNT".green(),
                event_id, path_index, max_borrow, base_token
            );

            let optimal_result = self.optimize_borrow_amount(path, max_borrow, event_id, path_index).await;
            if let Some((optimal_amount, profit, final_amount, used_fswap)) = optimal_result {
                warn!(
                    "[{}] 🆔 ={} путь={} 🗺️ найден оптимальный результат: сумма_займа={}, прибыль={}, итоговая_сумма={}, использован_флеш_свап={}",
                    "COMPUTE_AAVE_BORROW_AMOUNT".green(),
                    event_id, path_index, optimal_amount, profit, final_amount, used_fswap
                );
                Some((optimal_amount, profit, final_amount, used_fswap))
            } else {
                info!(
                    "[{}] 🆔 ={} путь={} 🗺️ оптимизация не нашла прибыльных вариантов",
                    "COMPUTE_AAVE_BORROW_AMOUNT".green(),
                    event_id, path_index
                );
                None
            }
        } else {
            warn!(
                "[{}] 🆔 ={} путь={} 🗺️ токен {:?} отсутствует в ликвидности Aave",
                "COMPUTE_AAVE_BORROW_AMOUNT".green(),
                event_id, path_index, base_token
            );
            None
        }
    }

    /// Оценивает возможность дополнительного займа токена через флеш-свап.
    fn estimate_flashswap_cover_cost(
        &self,
        needed_token: Address,
        needed_amount: U256,
        route_pools: &[Address],
        event_id: &str,
        path_index: &str,
    ) -> (U256, bool) {
        let candidates: Vec<BorrowPoolInfo> = self
            .route_builder
            .borrow_pools
            .get(&needed_token)
            .map(|v| v.iter().cloned().filter(|bp| !route_pools.contains(&bp.pool_address)).collect())
            .unwrap_or_else(|| {
                debug!(
                    "[{}] 🆔 ={} путь={} токен {:?} 🔄 отсутствует в пулах для займа",
                    "estimate_flashswap_cover_cost".green(),
                    event_id, path_index, needed_token
                );
                Vec::new()
            });

        let snapshot = self.create_graph_snapshot(route_pools, &candidates);
        let mut remaining = needed_amount;
        let mut total_fee_cost = U256::zero();

        for cand in candidates {
            if remaining.is_zero() {
                break;
            }
            let Some(pool) = snapshot.pools.get(&cand.pool_address) else { continue; };

            let cap = if pool.uniswap_token_a == needed_token.into() {
                cap_90pct(pool.liquidity_token_a)
            } else if pool.uniswap_token_b == needed_token.into() {
                cap_90pct(pool.liquidity_token_b)
            } else {
                U256::zero()
            };
            if cap.is_zero() {
                continue;
            }

            let take = remaining.min(cap);
            if take.is_zero() {
                continue;
            }

            let fee_cost = mul_div_u256(take, U256::from(cand.fee_tier as u128), U256::from(1_000_000u128));
            total_fee_cost = total_fee_cost.saturating_add(fee_cost);

            warn!(
                "[{}] 🆔 ={} путь={} пул={:?} взято={} лимит={} 🔄 комиссия_в_пунктах={} стоимость_комиссии={}",
                "estimate_flashswap_cover_cost".green(),
                event_id, path_index, cand.pool_address, take, cap, cand.fee_tier, fee_cost
            );

            remaining = remaining.saturating_sub(take);
        }

        let covered = remaining.is_zero();
        if !covered {
            debug!(
                "[{}] 🔄 🆔 ={} путь={} не покрыто: остаток={}",
                "estimate_flashswap_cover_cost".green(),
                event_id, path_index, remaining
            );
        }
        (total_fee_cost, covered)
    }

    /// Вычисляет потенциальную прибыль от арбитража по заданному пути.
    async fn compute_arbitrage_profit(
        &self,
        path: &ArbitragePath,
        borrow_amount: U256,
        event_id: &str,
        path_index: &str,
    ) -> Option<(U256, U256, bool)> {
        debug!(
            "[{}]💰🆔 ={} путь={} сумма_займа={}",
            "COPMUTE_ARBOTRAGE_PROFIT".green(),
            event_id, path_index, borrow_amount
        );

        let snapshot = self.create_graph_snapshot(&path.pools, &[]);
        let mut current_amount = borrow_amount;
        let mut fswap_cost_accum = U256::zero();
        let mut used_uniswap_flash_supplement = false;

        for (i, token_in) in path.tokens.iter().enumerate().take(path.tokens.len().saturating_sub(1)) {
            let token_out = path.tokens[i + 1];
            let pool_addr = path.pools[i];
            let Some(pool) = snapshot.pools.get(&pool_addr) else {
                warn!(
                    "[{}]💰 🆔 ={} путь={} пул {:?} отсутствует в снимке",
                    "PROFIT".green(),
                    event_id, path_index, pool_addr
                );
                return None;
            };

            let zero_for_one = if pool.uniswap_token_a == (*token_in).into() {
                true // tokenA -> tokenB
            } else if pool.uniswap_token_b == (*token_in).into() {
                false // tokenB -> tokenA
            } else {
                warn!(
                    "[{}] 💰 🆔 ={} путь={} несоответствие токенов в пуле {:?}",
                    "PROFIT".green(),
                    event_id, path_index, pool_addr
                );
                return None;
            };

            debug!(
                "[{}] 🆔 ={} путь={} шаг={} пул={:?} 💰 входной_токен={:?} выходной_токен={:?} ноль_за_один={}",
                "PROFIT".green(),
                event_id, path_index, i, pool_addr, token_in, token_out, zero_for_one
            );

            let empty_vec = vec![];
            let tick_map = snapshot.tick_maps.get(&pool_addr).unwrap_or(&empty_vec);
            let amount_out = self.compute_amount_out(
                pool.uniswap_sqrt_price,
                pool.uniswap_liquidity.as_u128(),
                tick_map,
                pool.uniswap_tick_current,
                pool.uniswap_fee_tier,
                current_amount,
                zero_for_one,
                event_id,
                path_index,
            );

            if amount_out.is_zero() {
                let (cost, covered) = self.estimate_flashswap_cover_cost(token_out, current_amount, &path.pools, event_id, path_index);
                if covered && cost > U256::zero() {
                    fswap_cost_accum = fswap_cost_accum.saturating_add(cost);
                    used_uniswap_flash_supplement = true;
                    warn!(
                        "[{}] 💰 🆔 ={} путь={} шаг={} прерван — покрыто флеш-свапом, стоимость={}, сохранено_суммы={}",
                        "PROFIT".green(),
                        event_id, path_index, i, cost, current_amount
                    );
                    continue;
                } else {
                    debug!(
                        "[{}] 💰 🆔 ={} путь={} шаг={} прерван — флеш-свап недоступен, отмена",
                        "PROFIT".green(),
                        event_id, path_index, i
                    );
                    return None;
                }
            }

            debug!(
                "[{}] 💰 🆔 ={} путь={} шаг={} свап: вход={} -> выход={}",
                "PROFIT".green(),
                event_id, path_index, i, current_amount, amount_out
            );
            current_amount = amount_out;
        }

        let aave_fee = mul_div_u256(borrow_amount, U256::from(AAVE_FLASH_FEE_NUM), U256::from(AAVE_FLASH_FEE_DEN));
        if current_amount <= borrow_amount + aave_fee + fswap_cost_accum {
            warn!(
                "[{}] 💰 🆔 ={} путь={} нерентабельно: итог={} сумма_займа+комиссии={}",
                "PROFIT".green(),
                event_id, path_index, current_amount, borrow_amount + aave_fee + fswap_cost_accum
            );
            return None;
        }

        let profit = current_amount - borrow_amount - aave_fee - fswap_cost_accum;
        Some((profit, current_amount, used_uniswap_flash_supplement))
    }

    /// Оптимизирует сумму займа для пути с использованием бинарного поиска.
    async fn optimize_borrow_amount(
        &self,
        path: &ArbitragePath,
        max_borrow: U256,
        event_id: &str,
        path_index: &str,
    ) -> Option<(U256, U256, U256, bool)> {
        let mut low = U256::zero();
        let mut high = max_borrow;
        let mut optimal_amount = U256::zero();
        let mut max_profit = U256::zero();
        let mut final_amount = U256::zero();
        let mut used_fswap = false;

        // Бинарный поиск с 20 итерациями для повышения точности
        for _ in 0..20 {
            let mid = (low + high) / U256::from(2);
            let left = (low + mid) / U256::from(2);
            let right = (mid + high) / U256::from(2);

            let (profit_left, final_left, used_left) = self
                .compute_arbitrage_profit(path, left, event_id, path_index)
                .await
                .unwrap_or((U256::zero(), U256::zero(), false));

            let (profit_right, final_right, used_right) = self
                .compute_arbitrage_profit(path, right, event_id, path_index)
                .await
                .unwrap_or((U256::zero(), U256::zero(), false));

            if profit_left > profit_right {
                high = mid;
                if profit_left > max_profit {
                    max_profit = profit_left;
                    optimal_amount = left;
                    final_amount = final_left;
                    used_fswap = used_left;
                }
            } else {
                low = mid;
                if profit_right > max_profit {
                    max_profit = profit_right;
                    optimal_amount = right;
                    final_amount = final_right;
                    used_fswap = used_right;
                }
            }
        }

        if !max_profit.is_zero() {
            Some((optimal_amount, max_profit, final_amount, used_fswap))
        } else {
            None
        }
    }

    /// Симулирует все пути для максимальной прибыли.
    async fn simulate_all_paths_max_profit(
        &self,
        route_indices: &[usize],
        event_id: &str,
    ) -> Vec<PathSimulationResult> {
        let mut results = Vec::new();
        for route_index in route_indices {
            let path_index = format!("{}-{}", event_id, route_index);
            if let Some(route) = self.route_builder.paths.get(*route_index) {
                let base_token = route.tokens.first().copied().unwrap_or_default();
                if let Some((borrow_optimal, profit_net, final_amount, used_fswap)) =
                    self.compute_aave_borrow_amount(&route, event_id, &path_index).await
                {
                    results.push(PathSimulationResult {
                        path_index: *route_index,
                        base_token,
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

    /// Проверяет возможность займа токена из пула Uniswap.
    async fn compute_uniswap_borrow_amount(
        &self,
        token: Address,
        pool_address: Address,
        amount: U256,
        event_id: &str,
    ) -> bool {
        let snapshot = self.create_graph_snapshot(&[pool_address], &[]);
        let Some(pool) = snapshot.pools.get(&pool_address) else {
            debug!(
                "[{}]🎟️ 🆔 ={} пул={:?} отсутствует в снимке",
                "COMPUTE_BORROW_AMOUNT_UNISWAP".green(),
                event_id, pool_address
            );
            return false;
        };

        let available = if pool.uniswap_token_a == token.into() {
            cap_90pct(pool.liquidity_token_a)
        } else if pool.uniswap_token_b == token.into() {
            cap_90pct(pool.liquidity_token_b)
        } else {
            U256::zero()
        };

        debug!(
            "[{}] 🎟️ 🆔 ={} пул={:?} доступно={} требуется={}",
            "COMPUTE_BORROW_AMOUNT_UNISWAP".green(),
            event_id, pool_address, available, amount
        );
        available >= amount
    }

    /// Фильтрует пути по минимальному порогу прибыли.
    fn filter_by_min_profit_threshold(&self, results: Vec<PathSimulationResult>) -> Vec<PathSimulationResult> {
        results
            .into_iter()
            .filter(|result| {
                let threshold = MIN_PROFIT_THRESHOLD_BY_TOKEN.get(&result.base_token).copied().unwrap_or(U256::zero());
                if result.profit_net >= threshold {
                    warn!(
                        "[{}] путь={} прибыль={} итоговая_сумма={} использован_флеш_свап={} — выше порога {}",
                        "FILTER_BY_MIN_PROFIT_TRESHOLD".green(),
                        result.path_index, result.profit_net, result.final_amount, result.used_uniswap_flash_supplement, threshold
                    );
                    true
                } else {
                    debug!(
                        "[{}] путь={} прибыль={} итоговая_сумма={} использован_флеш_свап={} — ниже порога {}",
                        "FILTER_BY_MIN_PROFIT_TRESHOLD".green(),
                        result.path_index, result.profit_net, result.final_amount, result.used_uniswap_flash_supplement, threshold
                    );
                    false
                }
            })
            .collect()
    }

    /// Выбирает финальные арбитражные возможности.
    fn select_final_arbitrage_opportunities(
        &self,
        results: Vec<PathSimulationResult>,
        event_id: &str,
    ) -> Vec<PathSimulationResult> {
        let mut sorted_results = results;

        // Сортировка: главный критерий — прибыль (по убыванию),
        // вторичный — отсутствие флеш-свапа.
        sorted_results.sort_by(|a, b| {
            match b.profit_net.cmp(&a.profit_net) {
                std::cmp::Ordering::Equal => {
                    // Если прибыли равны → приоритет у варианта без флеш-свапа
                    a.used_uniswap_flash_supplement.cmp(&b.used_uniswap_flash_supplement)
                }
                other => other,
            }
        });

        // Оставляем только один лучший результат
        sorted_results.truncate(1);

        info!(
            "[{}] 🆔 ={} выбрано {} финальных арбитражных возможностей🥇",
            "SELECT_FINAL_ARBITRAGE_OPPORTUNITIES".green(),
            event_id,
            sorted_results.len()
        );

        for result in &sorted_results {
            warn!(
                "[{}] 🆔 ={} путь={} прибыль={} итоговая_сумма={} использован_флеш_свап={} 🥇",
                "SELECT_FINAL_ARBITRAGE_OPPORTUNITIES".green(),
                event_id,
                result.path_index,
                result.profit_net,
                result.final_amount,
                result.used_uniswap_flash_supplement
            );
        }

        sorted_results
    }


    /// GPU: Симулирует свап в одном пуле Uniswap V3.
    fn compute_amount_out(
        &self,
        sqrt_price_x96: U256,
        liquidity: u128,
        tick_map: &[(i32, i128, U256)],
        current_tick: i32,
        fee_tier: u32,
        amount_in: U256,
        zero_for_one: bool,
        event_id: &str,
        path_index: &str,
    ) -> U256 {
        info!(
            "[{}] 🆔 ={} путь={} входная_сумма={} ликвидность={} текущий_тик={} комиссия={}",
            "COMPUTE_AMOUNT_OUT".green(),
            event_id, path_index, amount_in, liquidity, current_tick, fee_tier
        );

        if liquidity == 0 || amount_in.is_zero() || tick_map.is_empty() {
            return U256::zero();
        }

        let mut sqrt_price_current = sqrt_price_x96;
        let mut liquidity_active = U256::from(liquidity);
        let mut amount_remaining = amount_in;
        let mut amount_out_total = U256::zero();
        let mut tick_current = current_tick;

        while !amount_remaining.is_zero() && !liquidity_active.is_zero() {
            let tick_next = Self::find_next_initialized_tick(tick_current, zero_for_one, tick_map);
            if (zero_for_one && tick_next <= MIN_TICK) || (!zero_for_one && tick_next >= MAX_TICK) {
                break;
            }

            let sp_next_u256 = tick_to_sqrt_price(tick_next).unwrap_or(U256::zero());
            let sqrt_price_next = sp_next_u256;

            let (amt_in_step, amt_out_step, sp_new) = self.compute_swap_step(
                sqrt_price_current,
                sqrt_price_next,
                liquidity_active,
                amount_remaining,
                fee_tier,
                zero_for_one,
                event_id,
                path_index,
            );

            if amt_in_step.is_zero() {
                break;
            }

            amount_remaining = amount_remaining.saturating_sub(amt_in_step);
            amount_out_total = amount_out_total.saturating_add(amt_out_step);
            sqrt_price_current = sp_new;

            let crossed = if zero_for_one { sp_new <= sqrt_price_next } else { sp_new >= sqrt_price_next };
            if crossed {
                liquidity_active = U256::from(Self::update_liquidity_on_tick_cross(liquidity_active.as_u128(), tick_next, tick_map, zero_for_one));
                tick_current = tick_next;
                if liquidity_active.is_zero() { break; }
            }
        }

        amount_out_total
    }

    /// Находит следующий инициализированный тик с использованием бинарного поиска.
    fn find_next_initialized_tick(
        tick_current: i32,
        zero_for_one: bool,
        tick_map: &[(i32, i128, U256)],
    ) -> i32 {
        if zero_for_one {
            // Поиск ближайшего меньшего тика (tick_map отсортирован по возрастанию)
            match tick_map.binary_search_by_key(&tick_current, |(t, _, _)| *t) {
                Ok(idx) => tick_map.get(idx.saturating_sub(1)).map(|(t, _, _)| *t).unwrap_or(MIN_TICK),
                Err(idx) => tick_map.get(idx.saturating_sub(1)).map(|(t, _, _)| *t).unwrap_or(MIN_TICK),
            }
        } else {
            // Поиск ближайшего большего тика
            match tick_map.binary_search_by_key(&tick_current, |(t, _, _)| *t) {
                Ok(idx) => tick_map.get(idx + 1).map(|(t, _, _)| *t).unwrap_or(MAX_TICK),
                Err(idx) => tick_map.get(idx).map(|(t, _, _)| *t).unwrap_or(MAX_TICK),
            }
        }
    }

    /// GPU: Вычисляет один шаг свапа до следующего тика.
    /// Вычисляет один шаг свапа до следующего тика.
    fn compute_swap_step(
        &self,
        sqrt_price_current: U256,
        sqrt_price_target: U256,
        liquidity: U256,
        amount_remaining: U256,
        fee_pips: u32,
        zero_for_one: bool,
        event_id: &str,
        path_index: &str,
    ) -> (U256, U256, U256) {
        if liquidity.is_zero() || amount_remaining.is_zero() || sqrt_price_current.is_zero() || sqrt_price_target.is_zero() {
            // Изменение: Добавлено логирование для некорректных входных данных
            debug!(
                "[{}] 🆔 ={} 🚶🚶🚶 путь={} некорректные входные данные: ликвидность={}, вход={}, текущая_цена={}, цель_цена={}",
                "COMPUTE_SWAP_STEP".green(),
                event_id, path_index, liquidity, amount_remaining, sqrt_price_current, sqrt_price_target
            );
            return (U256::zero(), U256::zero(), sqrt_price_current);
        }

        // Изменение: Добавлена проверка на низкую ликвидность
        if liquidity < U256::from(1000) {
            debug!(
                "[{}] 🆔 ={} 🚶🚶🚶 путь={} низкая ликвидность: {}",
                "COMPUTE_SWAP_STEP".green(),
                event_id, path_index, liquidity
            );
            return (U256::zero(), U256::zero(), sqrt_price_current);
        }

        // Изменение: Добавлена проверка на маленькую входную сумму
        if amount_remaining < U256::from(1000) {
            debug!(
                "[{}] 🆔 ={} 🚶🚶🚶 путь={} входная сумма слишком мала: {}",
                "COMPUTE_SWAP_STEP".green(),
                event_id, path_index, amount_remaining
            );
            return (U256::zero(), U256::zero(), sqrt_price_current);
        }

        // Изменение: Добавлена проверка на совпадение текущей и целевой цены
        if sqrt_price_target == sqrt_price_current {
            debug!(
                "[{}] 🆔 ={} 🚶🚶🚶 путь={} некорректная цель цены: текущая={}, цель={}",
                "COMPUTE_SWAP_STEP".green(),
                event_id, path_index, sqrt_price_current, sqrt_price_target
            );
            return (U256::zero(), U256::zero(), sqrt_price_current);
        }

        let q96 = U256::from(1u128 << 96);

        let max_in = if zero_for_one {
            if sqrt_price_target < sqrt_price_current {
                mul_div_u256(
                    liquidity.saturating_mul(sqrt_price_current.saturating_sub(sqrt_price_target)),
                    q96,
                    sqrt_price_current.saturating_mul(sqrt_price_target)
                )
            } else {
                // Изменение: Добавлено логирование для нулевого max_in
                debug!(
                    "[{}] 🆔 ={} 🚶🚶🚶 путь={} некорректная цель цены для zero_for_one: текущая={}, цель={}",
                    "COMPUTE_SWAP_STEP".green(),
                    event_id, path_index, sqrt_price_current, sqrt_price_target
                );
                U256::zero()
            }
        } else {
            if sqrt_price_target > sqrt_price_current {
                mul_div_u256(
                    liquidity.saturating_mul(sqrt_price_target.saturating_sub(sqrt_price_current)),
                    q96,
                    U256::one()
                )
            } else {
                // Изменение: Добавлено логирование для нулевого max_in
                debug!(
                    "[{}] 🆔 ={} 🚶🚶🚶 путь={} некорректная цель цены для !zero_for_one: текущая={}, цель={}",
                    "COMPUTE_SWAP_STEP".green(),
                    event_id, path_index, sqrt_price_current, sqrt_price_target
                );
                U256::zero()
            }
        };

        // Изменение: Добавлена проверка на нулевой max_in
        if max_in.is_zero() {
            debug!(
                "[{}] 🆔 ={} 🚶🚶🚶 путь={} максимальный вход равен 0: ликвидность={}, текущая_цена={}, цель_цена={}",
                "COMPUTE_SWAP_STEP".green(),
                event_id, path_index, liquidity, sqrt_price_current, sqrt_price_target
            );
            return (U256::zero(), U256::zero(), sqrt_price_current);
        }

        let amount_in = amount_remaining.min(max_in);
        let fee = mul_div_u256(amount_in, U256::from(fee_pips as u128), U256::from(1_000_000u128));
        let in_after_fee = amount_in.saturating_sub(fee);

        let new_price = if in_after_fee >= max_in && !max_in.is_zero() {
            sqrt_price_target
        } else {
            if zero_for_one {
                let num = mul_div_u256(
                    in_after_fee.saturating_mul(sqrt_price_current),
                    sqrt_price_target,
                    q96
                );
                let denom_add = mul_div_u256(in_after_fee, sqrt_price_target, q96);
                let denom = liquidity.saturating_add(denom_add).max(U256::one());
                sqrt_price_current.saturating_sub(mul_div_u256(num, U256::one(), denom))
            } else {
                let delta = mul_div_u256(in_after_fee, q96, liquidity);
                sqrt_price_current.saturating_add(delta)
            }
        };

        let out = if zero_for_one {
            if new_price.is_zero() {
                // Изменение: Добавлено логирование для нулевой новой цены
                debug!(
                    "[{}] 🆔 ={} 🚶🚶🚶 путь={} нулевая новая цена для zero_for_one",
                    "COMPUTE_SWAP_STEP".green(),
                    event_id, path_index
                );
                U256::zero()
            } else {
                let price_change = sqrt_price_current.saturating_sub(new_price);
                mul_div_u256(price_change, liquidity, new_price)
            }
        } else {
            if new_price > sqrt_price_current {
                let numerator = liquidity.saturating_mul(new_price.saturating_sub(sqrt_price_current));
                let denominator = mul_div_u256(new_price, sqrt_price_current, q96);
                mul_div_u256(numerator, U256::one(), denominator.max(U256::one()))
            } else {
                // Изменение: Добавлено логирование для некорректной новой цены
                debug!(
                    "[{}] 🆔 ={} 🚶🚶🚶 путь={} некорректная новая цена для !zero_for_one: новая={}, текущая={}",
                    "COMPUTE_SWAP_STEP".green(),
                    event_id, path_index, new_price, sqrt_price_current
                );
                U256::zero()
            }
        };

        // Изменение: Обновлён стиль логирования
        debug!(
            "[{}] 🆔 ={} 🚶🚶🚶 путь={} вход={} выход={} новая_цена={}",
            "COMPUTE_SWAP_STEP".green(),
            event_id, path_index, amount_in, out, new_price
        );

        (amount_in, out, new_price)
    }
    /// Обновляет ликвидность при пересечении тика.
    fn update_liquidity_on_tick_cross(
        liquidity_current: u128,
        tick: i32,
        tick_map: &[(i32, i128, U256)],
        zero_for_one: bool,
    ) -> u128 {
        if let Some((_, liq_delta, _)) = tick_map.iter().find(|(t, _, _)| *t == tick) {
            let d = liq_delta.unsigned_abs();
            if zero_for_one {
                liquidity_current.saturating_sub(d)
            } else {
                liquidity_current.saturating_add(d)
            }
        } else {
            liquidity_current
        }
    }
}

/// Вычисляет 90% от значения с уменьшением на 1 wei.
#[inline]
fn cap_90pct(v: U256) -> U256 {
    let nine_tenths = mul_div_u256(v, U256::from(SAFE_FSWAP_FRACTION_NUM), U256::from(SAFE_FSWAP_FRACTION_DEN));
    if nine_tenths > U256::zero() { nine_tenths - U256::one() } else { U256::zero() }
}

/// Безопасное умножение и деление для U256.
#[inline]
fn mul_div_u256(a: U256, b: U256, c: U256) -> U256 {
    if c.is_zero() {
        return U256::zero();
    }
    let product = a.checked_mul(b).unwrap_or(U256::zero());
    product.checked_div(c).unwrap_or(U256::zero())
}