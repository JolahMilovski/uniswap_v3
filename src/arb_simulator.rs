use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    arb_scanner::calculate_aave_borrow_amount,
    path_builder::PathBuilder,
    uniswap_events::PoolEventInfo,
    uniswap_graph::{UniswapPool, UniversalGraph},
};
use arc_swap::ArcSwap;
use ethers::types::Address;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc::Receiver as MpscReceiver;
use tracing::{debug, error, info, warn};

/// Симулятор арбитража
#[derive(Clone)]
pub struct ArbitrageSimulator {
    path_builder: Arc<PathBuilder>,
    aave_liquidity_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
    graph: Arc<ArcSwap<UniversalGraph>>,
    event_counter: Arc<AtomicUsize>,
}

impl ArbitrageSimulator {
    pub fn new(
        path_builder: Arc<PathBuilder>,
        aave_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<ArcSwap<UniversalGraph>>,
    ) -> Self {
        ArbitrageSimulator {
            path_builder,
            aave_liquidity_rx: aave_rx,
            graph,
            event_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn run_for_event(
        &self,
        events: HashMap<Address, PoolEventInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event_id = self.event_counter.fetch_add(1, Ordering::SeqCst);

        info!(
            "[UNISWAP_SIMULATOR event: {}] Запуск симуляции для события. Количество событий: {}. Пулы: {:?}", 
            event_id, events.len(), events.keys().collect::<Vec<_>>()
        );

        if events.is_empty() {
            warn!("[UNISWAP_SIMULATOR event: {}] События пусты, симуляция пропущена", event_id);
            return Ok(());
        }

        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
        debug!(
            "[UNISWAP_SIMULATOR event: {}] Текущая ликвидность Aave: {} токенов", 
            event_id, aave_liquidity.token_info.len()
        );

        if aave_liquidity.token_info.is_empty() {
            warn!("[UNISWAP_SIMULATOR event: {}] Ликвидность Aave пуста, симуляция пропущена", event_id);
            return Ok(());
        }

        let mut cached_paths = HashMap::new();

        for (pool_address, event) in events {
            debug!(
                "[UNISWAP_SIMULATOR event: {}] Обработка пула {:?}. Event_id: {}. Текущий тик: {}", 
                event_id, pool_address, event.event_id, event.current_tick
            );

            let path_indices = self.path_builder.pool_to_paths.get(&pool_address)
                .map(|entry| entry.value().clone())
                .unwrap_or_else(Vec::new);
            cached_paths.insert(pool_address, path_indices.clone());

            debug!(
                "[UNISWAP_SIMULATOR event: {}] Найдено {} путей для пула {:?}", 
                event_id, path_indices.len(), pool_address
            );

            for &path_index in &path_indices {
                debug!(
                    "[UNISWAP_SIMULATOR event: {} index: {}] Обработка пути с индексом {}", 
                    event_id, path_index, path_index
                );

                if let Some(path) = self.path_builder.paths.get(path_index) {
                    debug!(
                        "[UNISWAP_SIMULATOR event: {} index: {}] Путь найден: пулы = {:?}, токены = {:?}", 
                        event_id, path_index, path.pools, path.tokens
                    );

                    let mut pool_path: Vec<UniswapPool> = Vec::new();
                    for pool_addr in &path.pools {
                        if let Some(pool) = self.graph.load().edges.get(pool_addr) {
                            debug!(
                                "[UNISWAP_SIMULATOR event: {} index: {}] Пул добавлен в путь: {:?}", 
                                event_id, path_index, pool_addr
                            );
                            pool_path.push(pool.clone());
                        } else {
                            warn!(
                                "[UNISWAP_SIMULATOR event: {} index: {}] Пул {:?} не найден в графе. Пропуск пути", 
                                event_id, path_index, pool_addr
                            );
                            continue;
                        }
                    }

                    if pool_path.is_empty() {
                        warn!(
                            "[UNISWAP_SIMULATOR event: {} index: {}] Путь пуст после обработки: {:?}", 
                            event_id, path_index, path.pools
                        );
                        continue;
                    }

                    let start_token = path.tokens.first().copied().unwrap_or_default();
                    debug!(
                        "[UNISWAP_SIMULATOR event: {} index: {}] Стартовый токен: {:?}", 
                        event_id, path_index, start_token
                    );

                    if aave_liquidity.token_info.get(&start_token).is_none() {
                        warn!(
                            "[UNISWAP_SIMULATOR event: {} index: {}] Ликвидность для флеш-лоана недоступна для токена {:?}", 
                            event_id, path_index, start_token
                        );
                        continue;
                    }

                    let result = calculate_aave_borrow_amount(
                        event_id,
                        &pool_path,
                        start_token,
                        &aave_liquidity,
                        path_index,
                    );

                    match result {
                        Ok(start_amount) => {
                            info!(
                                "[UNISWAP_SIMULATOR event: {} index: {}] 💰 Рассчитана сумма заимствования: {} для токена {:?}", 
                                event_id, path_index, start_amount, start_token
                            );
                        }
                        Err(err) => {
                            error!(
                                "[UNISWAP_SIMULATOR event: {} index: {}] Ошибка расчета суммы заимствования: {:?}", 
                                event_id, path_index, err
                            );
                        }
                    }
                } else {
                    warn!(
                        "[UNISWAP_SIMULATOR event: {} index: {}] Путь с индексом {} не найден", 
                        event_id, path_index, path_index
                    );
                }
            }
        }

        debug!("[UNISWAP_SIMULATOR event: {}] Завершена обработка события", event_id);
        Ok(())
    }
}

pub struct SimulationRunner {
    simulator_rx: MpscReceiver<PoolEventInfo>,
    simulator: ArbitrageSimulator,
}

impl SimulationRunner {
    pub fn new(
        simulator_rx: MpscReceiver<PoolEventInfo>,
        path_builder: Arc<PathBuilder>,
        aave_rx: tokio::sync::watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<ArcSwap<UniversalGraph>>,
    ) -> Self {
        let simulator = ArbitrageSimulator::new(path_builder, aave_rx, graph);
        info!(
            "[UNISWAP_SIMULATION_RUNNER] Создан SimulationRunner. Размер канала simulator_rx: {}", 
            simulator_rx.capacity()
        );
        SimulationRunner {
            simulator_rx,
            simulator,
        }
    }

    pub async fn run(&mut self) {
        info!(
            "[UNISWAP_SIMULATION_RUNNER] Запуск обработки симуляций. Количество путей: {}. Пулов в графе: {}", 
            self.simulator.path_builder.paths.len(), 
            self.simulator.graph.load().edges.len()
        );

        while let Some(event) = self.simulator_rx.recv().await {
            debug!(
                "[UNISWAP_SIMULATION_RUNNER_DEBUG] Получено событие ID {} для пула {:?}. Текущий тик: {}", 
                event.event_id, event.address, event.current_tick
            );

            let simulator = self.simulator.clone();
            let event_map = HashMap::from([(event.address, event.clone())]);

            debug!(
                "[UNISWAP_SIMULATION_RUNNER_DEBUG] Запуск симуляции для события ID {}. Пул: {:?}", 
                event.event_id, event.address
            );

            tokio::spawn(async move {
                if let Err(e) = simulator.run_for_event(event_map).await {
                    error!(
                        "[UNISWAP_SIMULATION_RUNNER_ERROR] Ошибка симуляции для события ID {}: {:?}", 
                        event.event_id, e
                    );
                } else {
                    debug!(
                        "[UNISWAP_SIMULATION_RUNNER_DEBUG] Симуляция для события ID {} завершена успешно", 
                        event.event_id
                    );
                }
            });
        }

        error!(
            "[UNISWAP_SIMULATION_RUNNER_ERROR] Канал simulator_rx закрыт или пуст. Завершение симуляций. Количество путей: {}", 
            self.simulator.path_builder.paths.len()
        );
    }
}
