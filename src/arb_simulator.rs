use crate::{
    aave_v3_flash_monitor::AaveTokenLiquidity,
    arb_scanner::calculate_aave_borrow_amount,
    path_builder::PathBuilder,
    uniswap_events::PoolEventInfo,
    uniswap_graph::{UniswapPool, UniversalGraph},
};

use tracing::{debug, error, info, warn};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::{mpsc, watch};

/// # Симулятор арбитража
/// 
/// Основная структура, которая координирует процесс поиска и симуляции
/// арбитражных возможностей в реальном времени.
/// 
/// ## Компоненты:
/// - `path_builder` - Построитель путей для арбитража между токенами
/// - `aave_liquidity_rx` - Приемник данных о ликвидности токенов в Aave
/// - `graph` - Граф пулов Uniswap для навигации между токенами
/// - `event_rx` - Приемник событий от пулов Uniswap
/// - `event_counter` - Атомарный счетчик для отслеживания обработанных событий
pub struct ArbitrageSimulator {
    /// Построитель путей арбитража с предвычисленными маршрутами
    path_builder: Arc<PathBuilder>,
    
    /// Приемник watch-канала для получения актуальной информации о ликвидности Aave
    aave_liquidity_rx: watch::Receiver<AaveTokenLiquidity>,
    
    /// Граф всех пулов Uniswap для поиска путей обмена
    graph: Arc<UniversalGraph>,
    
    /// Приемник mpsc-канала для получения событий от пулов
    event_rx: mpsc::Receiver<PoolEventInfo>,
    
    /// Атомарный счетчик для уникальной идентификации каждого обрабатываемого события
    event_counter: Arc<AtomicUsize>,
}

impl ArbitrageSimulator {
    /// # Создание нового экземпляра симулятора
    /// 
    /// ## Параметры:
    /// - `path_builder` - Разделяемый построитель путей арбитража
    /// - `aave_liquidity_rx` - Приемник для мониторинга ликвидности Aave
    /// - `graph` - Граф пулов Uniswap
    /// - `event_rx` - Приемник событий от пулов
    /// 
    /// ## Возвращает:
    /// Новый экземпляр `ArbitrageSimulator`
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
            event_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// # Основной цикл выполнения симулятора
    /// 
    /// Этот асинхронный метод запускает бесконечный цикл обработки событий:
    /// 
    /// ## Алгоритм работы:
    /// 1. **Ожидание события** - Получает событие от пула Uniswap
    /// 2. **Инкремент счетчика** - Присваивает уникальный ID событию
    /// 3. **Получение ликвидности** - Читает актуальные данные Aave
    /// 4. **Поиск путей** - Находит все предвычисленные пути для данного пула
    /// 5. **Расчет суммы заимствования**:
    ///    - Построение цепочки пулов
    ///    - Определение стартового токена
    ///    - Расчет необходимой суммы заимствования из Aave для достижения минимальной прибыли
    /// 6. **Логирование результатов** - Выводит информацию о необходимой сумме заимствования
    /// 
    /// ## Обработка ошибок:
    /// - Предупреждения для отсутствующих пулов в графе
    /// - Логирование ошибок расчета
    /// - Информирование об отсутствии путей для пула
    pub async fn run(&mut self) {
        info!("[ UNISWAP_ARB_SIMULATOR ] Запуск симулятора арбитража");

        // Основной цикл обработки событий от пулов
        while let Some(event) = self.event_rx.recv().await {
            // Присваиваем уникальный ID каждому событию для отслеживания
            let event_id = self.event_counter.fetch_add(1, Ordering::SeqCst);
            
            // Извлекаем адрес пула из события
            let pool_address = event.address;
            
            // Получаем актуальную информацию о ликвидности Aave
            let aave_liquidity = self.aave_liquidity_rx.borrow().clone();

            debug!(
                "[ UNISWAP_ARB_SIMULATOR event:{} ] Текущая ликвидность Aave: {} токенов",
                event_id, aave_liquidity.token_info.len()
            );

            // Ищем все предвычисленные пути, которые включают данный пул
            if let Some(path_indices) = self.path_builder.pool_to_paths.get(&pool_address) {
                debug!(
                    "[ UNISWAP_ARB_SIMULATOR event:{} ] Найдено {} путей для пула {:?}",
                    event_id,
                    path_indices.value().len(),
                    pool_address
                );

                // Обрабатываем каждый найденный путь
                for &path_index in path_indices.value() {
                    debug!("[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Обработка пути с индексом {}", event_id, path_index, path_index);

                    // Получаем детали пути по индексу
                    if let Some(path) = self.path_builder.paths.get(path_index) {
                        debug!("[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Путь найден: pools={:?}, tokens={:?}", event_id, path_index, path.pools, path.tokens);

                        // Строим вектор объектов пулов для расчета
                        let mut pool_path: Vec<UniswapPool> = Vec::new();
                        for pool_addr in &path.pools {
                            if let Some(pool) = self.graph.edges.get(pool_addr) {
                                debug!("[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Пул добавлен в путь: {:?}", event_id, path_index, pool_addr);
                                pool_path.push(pool.clone());
                            } else {
                                warn!("[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Пул {:?} не найден в графе", event_id, path_index, pool_addr);
                                continue;
                            }
                        }

                        // Определяем стартовый токен (первый в цепочке)
                        let start_token = path.tokens.first().copied().unwrap_or_default();

                        // Проверяем наличие ликвидности Aave для стартового токена
                        let aave_liquidity = self.aave_liquidity_rx.borrow().clone();
                        if aave_liquidity.token_info.get(&start_token).is_none() {
                            error!(
                                "[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Ликвидность для флеш-лоана недоступна для токена {:?}",
                                event_id, path_index, start_token
                            );
                            continue;
                        }

                        // Рассчитываем необходимую сумму заимствования из Aave
                        let result = calculate_aave_borrow_amount
                            (event_id,
                            &pool_path,
                            start_token,
                            &aave_liquidity,
                            path_index);

                        // Анализируем результат расчета
                        match result {
                            Ok(start_amount) => {
                                warn!(
                                    "[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] 💰 Рассчитана сумма заимствования: {} {} для пути {:?}", 
                                    event_id, path_index,
                                    start_amount,
                                    start_token,
                                    path.tokens
                                );
                            }
                            Err(err) => {
                                warn!(
                                    "[ UNISWAP_ARB_SIMULATOR event:{} path_index:{}] Ошибка расчета суммы заимствования: {} для пути {:?}", 
                                    event_id, path_index, err, path.tokens
                                );
                            }
                        }
                    }
                }
            } else {
                // Для данного пула не найдено предвычисленных путей
                warn!(
                    "[ UNISWAP_ARB_SIMULATOR event:{}] Нет путей для пула {:?} ",
                    event_id, pool_address
                );
            }
        }
    }
}