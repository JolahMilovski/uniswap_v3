// Импорт необходимых модулей и типов данных
use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,  // Структура для хранения ликвидности Aave
    arb_scanner::simulate_path_swap,            // Функция для симуляции свопов по пути
    path_builder::PathBuilder,                  // Структура для построения путей арбитража
    uniswap_events::PoolEventInfo,             // Информация о событиях пула Uniswap
    uniswap_graph::{UniswapPool, UniversalGraph}, // Структуры для работы с графом пулов
};
use ethers::types::U256;                       // Тип для работы с большими числами
use log::{error, info, warn};                  // Макросы для логирования
use std::sync::Arc;                            // Атомарный счетчик ссылок
use tokio::sync::{mpsc, watch};               // Каналы для асинхронной коммуникации

// Структура симулятора арбитража
pub struct ArbitrageSimulator {
    path_builder: Arc<PathBuilder>,            // Построитель путей (общий доступ)
    aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>, // Приемник данных о ликвидности
    graph: Arc<UniversalGraph>,               // Граф пулов (общий доступ)
    event_rx: mpsc::Receiver<PoolEventInfo>,  // Приемник событий пула
}

impl ArbitrageSimulator {
    // Конструктор симулятора
    pub fn new(
        path_builder: Arc<PathBuilder>,
        aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>,
        graph: Arc<UniversalGraph>,
        event_rx: mpsc::Receiver<PoolEventInfo>,
    ) -> Self {
        Self {
            path_builder,
            aave_liquidity_rx,
            graph,
            event_rx,
        }
    }

    // Основной метод работы симулятора
    pub async fn run(&mut self) {
        // Обработка входящих событий пула
        while let Some(event) = self.event_rx.recv().await {
            let pool_address = event.address;
            let aave_liquidity = self.aave_liquidity_rx.borrow().clone();

            // Поиск путей, связанных с текущим пулом
            if let Some(path_indices) = self.path_builder.pool_to_paths.get(&pool_address) {
                // Перебор всех индексов путей
                for &path_index in path_indices.value() {
                    if let Some(path) = self.path_builder.paths.get(path_index) {
                        // Формирование вектора пулов для пути
                        let mut pool_path: Vec<UniswapPool> = Vec::new();
                        for pool_addr in &path.pools {
                            if let Some(pool) = self.graph.edges.get(pool_addr) {
                                pool_path.push(pool.clone());
                            } else {
                                info!(
                                    "[ARB_SIMULATION][{:?}] Пул не найден в графе, пропускаем путь",
                                    pool_addr
                                );
                                continue;
                            }
                        }

                        // Проверка полноты пути
                        if pool_path.len() != path.pools.len() {
                            info!("[ARB_SIMULATION] Неполный путь, пропускаем: {:?}", path);
                            continue;
                        }

                        // Установка начальных параметров для симуляции
                        let start_token = path.tokens[0];
                        let start_amount = U256::from(1_000_000_000_000_000_000u128); // 1 токен

                        // Симуляция свопов по пути
                        match simulate_path_swap(&pool_path, start_amount, start_token, &aave_liquidity) {
                            Ok(Some((final_amount, outputs))) => {
                                info!(
                                    "[ARB_SIMULATION] Успешный арбитраж для пути {:?}: Прибыль = {}, Промежуточные выходы = {:?}",
                                    path.pools,
                                    final_amount.saturating_sub(start_amount),
                                    outputs
                                );
                            }
                            Ok(None) => {
                                info!("[ARB_SIMULATION] Арбитраж для пути {:?} не превысил порог прибыли", path.pools);
                            }
                            Err(e) => {
                                error!("[ARB_SIMULATION] Ошибка симуляции для пути {:?}: {}", path.pools, e);
                            }
                        }
                    }
                }
            } else {
                info!("[ARB_SIMULATION] Нет путей для пула {:?}", pool_address);
            }
        }
        warn!("[ARB_SIMULATOR] Канал событий закрыт, симулятор завершён");
    }
}