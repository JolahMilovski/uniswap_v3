use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,  
    arb_scanner::{simulate_path_swap, MIN_PROFIT_THRESHOLD_BY_TOKEN}, 
    path_builder::PathBuilder,                  
    uniswap_events::PoolEventInfo,             
    uniswap_graph::{UniswapPool, UniversalGraph},
};
use ethers::types::Address;           
use log::{debug, error, info, warn};          
use std::sync::Arc;                           
use tokio::sync::{mpsc, watch};               

pub struct ArbitrageSimulator {
    path_builder: Arc<PathBuilder>,            
    aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>,
    graph: Arc<UniversalGraph>,               
    event_rx: mpsc::Receiver<PoolEventInfo>,  
}

impl ArbitrageSimulator {
    pub fn new(
        path_builder: Arc<PathBuilder>,
        aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<UniversalGraph>,
        event_rx: mpsc::Receiver<PoolEventInfo>,
    ) -> Self {
        debug!("[ARB_SIMULATOR_DEBUG] Инициализация ArbitrageSimulator: path_builder_len={:?}, graph_edges={}", 
            path_builder.paths.len(), graph.edges.len());
        Self {
            path_builder,
            aave_liquidity_rx,
            graph,
            event_rx,
        }
    }

    pub async fn run(&mut self) {
        info!("[ARB_SIMULATOR] Запуск симулятора арбитража");
        while let Some(event) = self.event_rx.recv().await {
            info!("[ARB_SIMULATOR] Получено событие пула: address={:?}, block_number={}", 
                event.address, event.block_number);
            let pool_address = event.address;
            let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
            debug!("[ARB_SIMULATOR_DEBUG] Текущая ликвидность Aave: tokens={}", 
                aave_liquidity.token_info.len());

            if let Some(path_indices) = self.path_builder.pool_to_paths.get(&pool_address) {
                debug!("[ARB_SIMULATOR_DEBUG] Найдено {} путей для пула {:?}", 
                    path_indices.value().len(), pool_address);
                for &path_index in path_indices.value() {
                    debug!("[ARB_SIMULATOR_DEBUG] Обработка пути с индексом {}", path_index);
                    if let Some(path) = self.path_builder.paths.get(path_index) {
                        debug!("[ARB_SIMULATOR_DEBUG] Путь найден: pools={:?}, tokens={:?}", 
                            path.pools, path.tokens);
                        let mut pool_path: Vec<UniswapPool> = Vec::new();
                        for pool_addr in &path.pools {
                            if let Some(pool) = self.graph.edges.get(pool_addr) {
                                debug!("[ARB_SIMULATOR_DEBUG] Пул добавлен в путь: {:?}", pool_addr);
                                pool_path.push(pool.clone());
                            } else {
                                warn!("[ARB_SIMULATOR] Пул не найден в графе: {:?}", pool_addr);
                                continue;
                            }
                        }

                        if pool_path.len() != path.pools.len() {
                            warn!("[ARB_SIMULATOR] Неполный путь: pools_found={}, pools_expected={}", 
                                pool_path.len(), path.pools.len());
                            continue;
                        }
                        debug!("[ARB_SIMULATOR_DEBUG] Путь полный: пулов={}", pool_path.len());

                        let start_token: Address = path.tokens[0];
                        debug!("[ARB_SIMULATOR_DEBUG] Начальный токен: {:?}", start_token);
                        
                        let start_amount = match MIN_PROFIT_THRESHOLD_BY_TOKEN.get(&start_token) {
                            Some(&amount) => {
                                debug!("[ARB_SIMULATOR_DEBUG] Начальная сумма для токена {:?}: {}", 
                                    start_token, amount);
                                amount
                            }
                            None => {
                                warn!("[ARB_SIMULATOR] Токен {:?} не найден в MIN_PROFIT_THRESHOLD_BY_TOKEN", 
                                    start_token);
                                continue;
                            }
                        };

                        debug!("[ARB_SIMULATOR_DEBUG] Запуск симуляции для пути: start_amount={}", start_amount);
                        match simulate_path_swap(&pool_path, start_amount, start_token, &aave_liquidity) {
                            Ok(Some((final_amount, outputs))) => {
                                info!(
                                    "[ARB_SIMULATOR] Успешный арбитраж для пути {:?}: Прибыль = {}, Промежуточные выходы = {:?}", 
                                    path.pools, final_amount.saturating_sub(start_amount), outputs
                                );
                                debug!("[ARB_SIMULATOR_DEBUG] Детали успешной симуляции: final_amount={}, outputs={:?}", 
                                    final_amount, outputs);
                            }
                            Ok(None) => {
                                info!("[ARB_SIMULATOR] Арбитраж для пути {:?} не превысил порог прибыли", 
                                    path.pools);
                                debug!("[ARB_SIMULATOR_DEBUG] Симуляция завершена без прибыли: путь={:?}", 
                                    path.pools);
                            }
                            Err(e) => {
                                error!("[ARB_SIMULATOR] Ошибка симуляции для пути {:?}: {}", 
                                    path.pools, e);
                                debug!("[ARB_SIMULATOR_DEBUG] Ошибка симуляции: error={}", e);
                            }
                        }
                    } else {
                        warn!("[ARB_SIMULATOR] Путь с индексом {} не найден", path_index);
                    }
                }
            } else {
                info!("[ARB_SIMULATOR] Нет путей для пула {:?}", pool_address);
                debug!("[ARB_SIMULATOR_DEBUG] Пулы не связаны с путями: {:?}", pool_address);
            }
        }
        warn!("[ARB_SIMULATOR] Канал событий закрыт, симулятор завершён");
        debug!("[ARB_SIMULATOR_DEBUG] Завершение работы симулятора");
    }
}