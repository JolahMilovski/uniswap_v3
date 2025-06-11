use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use colored::Colorize;
use dashmap::DashMap;
use ethers::types::Address;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{aave_v3_flash_monitor::AaveTokenLiquidity, uniswap_graph::UniversalGraph};

/// Структура представляющая путь арбитража
/// Содержит последовательность токенов и пулов для выполнения арбитражной сделки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitragePath {
    /// Последовательность адресов токенов в пути арбитража
    pub tokens: Vec<Address>,
    /// Последовательность адресов пулов, через которые проходит арбитраж
    pub pools: Vec<Address>,
}

/// Основная структура для построения и управления путями арбитража
/// Отвечает за поиск всех возможных циклических путей арбитража в графе Uniswap
#[derive(Debug, Serialize, Deserialize)]
pub struct PathBuilder {
    /// Список всех найденных путей арбитража
    pub paths: Vec<ArbitragePath>,
    /// Индекс для быстрого поиска путей по адресу пула
    /// Ключ - адрес пула, значение - индексы путей в векторе paths
    pub pool_to_paths: DashMap<Address, Vec<usize>>,
    /// Множество адресов токенов, доступных для flash loan в Aave
    aave_tokens: HashSet<Address>,
}

impl PathBuilder {
    /// Создает новый экземпляр PathBuilder
    ////// # Аргументы
    /// * `aave_rx` - Receiver для получения обновлений о ликвидности токенов в Aave
    ////// # Возвращает
    /// Новый экземпляр PathBuilder с пустыми путями и токенами из Aave
    pub fn new(aave_rx: watch::Receiver<AaveTokenLiquidity>) -> Self {
        info!("[{}] Начало создания нового экземпляра PathBuilder",  "UNISWAP_PATH_BUILDER".green());
        let aave_tokens_borrow = aave_rx.borrow().token_address.clone();
        info!("[{}] Загружено {} токенов из Aave для flash loan", "UNISWAP_PATH_BUILDER".green(), aave_tokens_borrow.len());
        let result = Self {
            aave_tokens: aave_tokens_borrow,
            paths: Vec::new(),
            pool_to_paths: DashMap::new(),
        };
        info!("[{}] Экземпляр PathBuilder успешно создан", "UNISWAP_PATH_BUILDER".green());
        result
    }

    /// Основная функция для построения всех возможных путей арбитража
    /// Очищает предыдущие результаты и строит новые пути на основе текущего графа
    /// # Аргументы
    /// * `graph` - Граф Uniswap, содержащий информацию о пулах и токенах
    /// # Логика работы
    /// 1. Очищает предыдущие пути и индексы
    /// 2. Строит список связей между токенами
    /// 3. Для каждого токена из Aave ищет циклические пути арбитража
    /// 4. Сохраняет результаты в JSON файл
    pub fn build_all_paths(&mut self, graph: Arc<UniversalGraph>) {
        info!("[{}] Начало построения всех арбитражных путей", "UNISWAP_PATH_BUILDER".green());
        // Очищаем предыдущие результаты
        info!("[{}] Очистка предыдущих путей (было {} путей)", "UNISWAP_PATH_BUILDER".green(), self.paths.len());
        self.paths.clear();
        info!("[{}] Очистка предыдущих индексов pool_to_paths (было {} записей)", "UNISWAP_PATH_BUILDER".green(), self.pool_to_paths.len());
        self.pool_to_paths.clear();

        // Строим граф связей между токенами
        info!("[{}] Построение списка связей между токенами", "UNISWAP_PATH_BUILDER".green(),);

        let related_list = self.build_related_list(&graph);

        info!("[{}] Построено {} связей между токенами", "UNISWAP_PATH_BUILDER".green(), related_list.len());

        let aave_tokens = self.aave_tokens.clone();

        info!("[{}] Начало поиска путей для {} токенов Aave","UNISWAP_PATH_BUILDER".green(), aave_tokens.len());

        // Для каждого токена из Aave ищем циклические пути арбитража
        for start_token in aave_tokens {
            info!("[{}] Поиск путей для начального токена {:?}", "UNISWAP_PATH_BUILDER".green(), start_token);
            let mut visited_pools = HashSet::new();
            let mut current_path = vec![start_token];
            let mut current_pools = Vec::new();

            // Запускаем рекурсивный поиск путей с 2-4 хопами
            info!("[{}] Запуск рекурсивного поиска путей для токена {:?}", "UNISWAP_PATH_BUILDER".green(), start_token);
            self.search_paths_dual_hops(
                start_token,
                start_token,
                &related_list,
                &mut visited_pools,
                &mut current_path,
                &mut current_pools,
                0,
            );
            info!("[{}] Завершён поиск путей для токена {:?}", "UNISWAP_PATH_BUILDER".green(), start_token);
        }

        info!("[{}] Найдено {} арбитражных путей","UNISWAP_PATH_BUILDER".green(), self.paths.len());
        // Сохраняем найденные пути в JSON файл
        info!("[{}] Сохранение путей в файл arbitrage_paths.json", "UNISWAP_PATH_BUILDER".green());
        if let Err(e) = self.save_to_json("arbitrage_paths.json") {
            info!("[{}] Ошибка сохранения путей в JSON: {:?}","UNISWAP_PATH_BUILDER".green(), e);
        } else {
            info!("[{}] Пути успешно сохранены в arbitrage_paths.json","UNISWAP_PATH_BUILDER".green());
        }
    }

    /// Строит список связей между токенами на основе графа пулов Uniswap
    ///
    /// # Аргументы
    /// * `graph` - Граф Uniswap с информацией о пулах
    ///
    /// # Возвращает
    /// DashMap где ключ - адрес токена, значение - вектор кортежей (связанный_токен, адрес_пула)
    ///
    /// # Логика
    /// Для каждого пула в графе создает двунаправленные связи между токенами
    /// Это позволяет быстро найти все токены, с которыми можно обменять данный токен
    fn build_related_list(
        &self,
        graph: &UniversalGraph,
    ) -> HashMap<Address, Vec<(Address, Address)>> {
        info!("[{}] Начало построения списка связей между токенами","UNISWAP_PATH_BUILDER".green());
        let mut related_list = HashMap::new();
        let pool_count = graph.nodes.len();
        info!("[{}] Обработка {} пулов из графа", "UNISWAP_PATH_BUILDER".green(), pool_count);

        // Проходим по всем пулам в графе
        for (index, entry) in graph.nodes.iter().enumerate() {
            let pool_address = *entry.key();
            let (token0, token1) = *entry.value();
            info!("[{}] Пул {}: {:?}, токены: ({:?}, {:?})","UNISWAP_PATH_BUILDER".green(),index + 1, pool_address, token0, token1);

            // Добавляем связь token0 -> token1 через данный пул
            related_list
                .entry(token0)
                .or_insert_with(Vec::new)
                .push((token1, pool_address));
          //  info!("[{}] Добавлена связь: токен {:?} -> токен {:?}", "UNISWAP_PATH_BUILDER".green(),token0, token1);

            // Добавляем обратную связь token1 -> token0 через тот же пул
            related_list
                .entry(token1)
                .or_insert_with(Vec::new)
                .push((token0, pool_address));
          //  info!("[{}] Добавлена обратная связь: токен {:?} -> токен {:?}", "UNISWAP_PATH_BUILDER".green(),token1, token0);
        }

        info!("[{}] Список связей построен, содержит {} токенов","UNISWAP_PATH_BUILDER".green(), related_list.len());

        related_list

    }

    /// Рекурсивная функция для поиска циклических путей арбитража
    /// Ищет пути длиной 3-4 хопа, которые начинаются и заканчиваются одним токеном
    ///
    /// # Аргументы
    /// * `start_token` - Начальный токен (должен совпадать с конечным для замыкания цикла)
    /// * `current_token` - Текущий токен в процессе поиска
    /// * `related_list` - Граф связей между токенами
    /// * `visited_pools` - Множество уже посещенных пулов (для избежания циклов)
    /// * `current_path` - Текущий путь токенов
    /// * `current_pools` - Текущий путь пулов
    /// * `current_hops` - Количество текущих хопов (переходов между токенами)
    ///
    /// # Логика работы
    /// 1. Проверяет, является ли текущий путь валидным арбитражем (3-4 хопа, замкнутый цикл)
    /// 2. Если да - сохраняет путь и обновляет индексы
    /// 3. Если нет - продолжает рекурсивный поиск по связанным токенам
    /// 4. Использует backtracking для исследования всех возможных путей
    fn search_paths_dual_hops(
        &mut self,
        start_token: Address,
        current_token: Address,
        related_list: &HashMap<Address, Vec<(Address, Address)>>,
        visited_pools: &mut HashSet<Address>,
        current_path: &mut Vec<Address>,
        current_pools: &mut Vec<Address>,
        current_hops: usize,
    ) {
        info!(
            "[{}] Поиск путей: текущий токен {:?}, хопы: {}, путь: {:?}, пулы: {:?}","UNISWAP_PATH_BUILDER".green(),
            current_token, current_hops, current_path, current_pools
        );

        // Проверяем, нашли ли мы валидный циклический путь арбитража
        if (current_hops == 3 || current_hops == 4)
            && current_token == start_token
            && !current_pools.is_empty()
        {            
            // Создаем новый путь арбитража
            let path = ArbitragePath {
                tokens: current_path.clone(),
                pools: current_pools.clone(),
            };

            // Добавляем путь в список и получаем его индекс
            let path_index = self.paths.len();
            self.paths.push(path);

            // Обновляем индекс pool_to_paths для быстрого поиска путей по пулам
            for pool in current_pools.iter() {
                self.pool_to_paths
                    .entry(*pool)
                    .or_insert_with(Vec::new)
                    .push(path_index);
            }

            // Если достигли максимальной длины пути (4 хопа), прекращаем поиск
            if current_hops == 4 {
                return;
            }
        }

        // Продолжаем поиск по связанным токенам
        if let Some(connections) = related_list.get(&current_token) {
            for (next_token, pool_address) in connections {
                // Пропускаем уже посещенные пулы (избегаем циклов)
                if visited_pools.contains(pool_address) {
                    continue;
                }

                // На 3-м хопе можем идти только к начальному токену (замыкание цикла)
                if current_hops == 3 && *next_token != start_token {
                    continue;
                }

                // Добавляем текущий пул и токен к пути
                visited_pools.insert(*pool_address);
                current_path.push(*next_token);
                current_pools.push(*pool_address);

                // Рекурсивно продолжаем поиск
                self.search_paths_dual_hops(
                    start_token,
                    *next_token,
                    related_list,
                    visited_pools,
                    current_path,
                    current_pools,
                    current_hops + 1,
                );

                // Backtracking: убираем текущий пул и токен из пути
                visited_pools.remove(pool_address);
                current_path.pop();
                current_pools.pop();
            }
        } else {
            info!("[{}] Нет связанных токенов для {:?}", "UNISWAP_PATH_BUILDER".green(), current_token);
        }
    }

    /// Сохраняет текущее состояние PathBuilder в JSON файл
    ///
    /// # Аргументы
    /// * `file_path` - Путь к файлу для сохранения
    ///
    /// # Возвращает
    /// Result с ошибкой в случае проблем с записью файла
    ///
    /// # Использование
    /// Позволяет сохранить найденные пути арбитража для последующего анализа
    /// или использования другими компонентами системы
    pub fn save_to_json(
        &self,
        file_path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("[{}] Начало сохранения PathBuilder в JSON файл {:?}", "UNISWAP_PATH_BUILDER".green(), file_path.as_ref());
        let file = File::create(file_path.as_ref())?;
        serde_json::to_writer_pretty(&file, self)?;
        info!("[{}] Сохранение в JSON завершено успешно", "UNISWAP_PATH_BUILDER".green());
        Ok(())
    }
}