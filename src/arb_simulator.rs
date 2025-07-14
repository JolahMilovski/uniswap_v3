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
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, warn};

/// Симулятор арбитража
pub struct ArbitrageSimulator {
    /// Построитель путей арбитража с предвычисленными маршрутами
    path_builder: Arc<PathBuilder>,
    /// Приемник watch-канала для получения актуальной информации о ликвидности Aave
    aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>,
    /// Граф всех пулов Uniswap для поиска путей обмена
    graph: Arc<ArcSwap<UniversalGraph>>,
    /// Приемник broadcast-канала для получения событий от пулов
    event_rx: BroadcastReceiver<(HashMap<Address, PoolEventInfo>, Arc<Notify>)>,
    /// Атомарный счетчик для уникальной идентификации каждого обрабатываемого события
    event_counter: Arc<AtomicUsize>,
    /// Уведомитель для синхронизации
    notify: Arc<Notify>,
}

impl ArbitrageSimulator {
    /// Создание нового экземпляра симулятора
    pub fn new(
        path_builder: Arc<PathBuilder>,
        aave_rx: watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<ArcSwap<UniversalGraph>>,
        event_rx: BroadcastReceiver<(HashMap<Address, PoolEventInfo>, Arc<Notify>)>,
        notify: Arc<Notify>,
    ) -> Self {
        ArbitrageSimulator {
            path_builder,
            aave_liquidity_rx: aave_rx,
            graph,
            event_rx,
            event_counter: Arc::new(AtomicUsize::new(0)),
            notify,
        }
    }

    /// Основной цикл выполнения симулятора
    pub async fn run(&mut self) {
        info!("[  UNISWAP_SIMULATOR  ] Запуск симулятора арбитража");

        // Основной цикл обработки событий от пулов
        while let Ok((events, _notify)) = self.event_rx.recv().await {
            // Присваиваем уникальный ID каждому событию для отслеживания
            let event_id = self.event_counter.fetch_add(1, Ordering::SeqCst);

            // Обрабатываем каждое событие из HashMap
            for (pool_address, _event) in events {
                // Получаем актуальную информацию о ликвидности Aave
                let aave_liquidity = self.aave_liquidity_rx.borrow().clone();

                debug!(
                    "[  UNISWAP_SIMULATOR   event: {} ] Текущая ликвидность Aave: {} токенов",
                    event_id,
                    aave_liquidity.token_info.len()
                );

                // Ищем все предвычисленные пути, которые включают данный пул
                if let Some(path_indices) = self.path_builder.pool_to_paths.get(&pool_address) {
                    debug!(
                        "[  UNISWAP_SIMULATOR   event: {} ] Найдено {} путей для пула {:?}",
                        event_id,
                        path_indices.value().len(),
                        pool_address
                    );

                    // Обрабатываем каждый найденный путь
                    for &path_index in path_indices.value() {
                        debug!("[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Обработка пути с индексом {}", event_id, path_index, path_index);

                        // Получаем детали пути по индексу
                        if let Some(path) = self.path_builder.paths.get(path_index) {
                            debug!("[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Путь найден: пулы={:?}, токены={:?}", event_id, path_index, path.pools, path.tokens);

                            // Строим вектор объектов пулов для расчета
                            let mut pool_path: Vec<UniswapPool> = Vec::new();
                            for pool_addr in &path.pools {
                                if let Some(pool) = self.graph.load().edges.get(pool_addr) {
                                    debug!("[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Пул добавлен в путь: {:?}", event_id, path_index, pool_addr);
                                    pool_path.push(pool.clone());
                                } else {
                                    warn!("[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Пул {:?} не найден в графе", event_id, path_index, pool_addr);
                                    continue;
                                }
                            }

                            // Определяем стартовый токен (первый в цепочке)
                            let start_token = path.tokens.first().copied().unwrap_or_default();

                            // Проверяем наличие ликвидности Aave для стартового токена
                            let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
                            if aave_liquidity.token_info.get(&start_token).is_none() {
                                error!(
                                    "[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Ликвидность для флеш-лоана недоступна для токена {:?}", 
                                    event_id, path_index, start_token
                                );
                                continue;
                            }

                            // Рассчитываем необходимую сумму заимствования из Aave
                            let result = calculate_aave_borrow_amount(
                                event_id,
                                &pool_path,
                                start_token,
                                &aave_liquidity,
                                path_index,
                            );

                            // Анализируем результат расчета
                            match result {
                                Ok(start_amount) => {
                                    warn!(
                                        "[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] 💰 Рассчитана сумма заимствования: {} {} для пути {:?}", 
                                        event_id, path_index,
                                        start_amount,
                                        start_token,
                                        path.tokens
                                    );
                                }
                                Err(err) => {
                                    warn!(
                                        "[  UNISWAP_SIMULATOR   event: {}  индекс_пути:{}] Ошибка расчета суммы заимствования: {} для пути {:?}", 
                                        event_id, path_index, err, path.tokens
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // Для данного пула не найдено предвычисленных путей
                    warn!(
                        "[СИМУЛЯТОР_АРБИТРАЖА event: {} ] Нет путей для пула {:?}",
                        event_id, pool_address
                    );
                }
            }

            // Уведомляем о завершении обработки события
            self.notify.notify_one();
        }
    }
}
