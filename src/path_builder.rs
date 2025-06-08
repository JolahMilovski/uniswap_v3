use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use dashmap::DashMap;
use ethers::types::Address;
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
    ///
    /// # Аргументы
    /// * `aave_rx` - Receiver для получения обновлений о ликвидности токенов в Aave
    ///
    /// # Возвращает
    /// Новый экземпляр PathBuilder с пустыми путями и токенами из Aave
    pub fn new(aave_rx: watch::Receiver<AaveTokenLiquidity>) -> Self {
        // Получаем текущий список токенов, доступных в Aave для flash loan
        let aave_tokens_borrow = aave_rx.borrow().token_address.clone();
        Self {
            aave_tokens: aave_tokens_borrow,
            paths: Vec::new(),
            pool_to_paths: DashMap::new(),
        }
    }

    /// Основная функция для построения всех возможных путей арбитража
    /// Очищает предыдущие результаты и строит новые пути на основе текущего графа
    ///
    /// # Аргументы
    /// * `graph` - Граф Uniswap, содержащий информацию о пулах и токенах
    ///
    /// # Логика работы
    /// 1. Очищает предыдущие пути и индексы
    /// 2. Строит список связей между токенами
    /// 3. Для каждого токена из Aave ищет циклические пути арбитража
    /// 4. Сохраняет результаты в JSON файл
    pub fn build_all_paths(&mut self, graph: &Arc<UniversalGraph>) {
        // Очищаем предыдущие результаты
        self.paths.clear();
        self.pool_to_paths.clear();

        // Строим граф связей между токенами
        let related_list = self.build_related_list(&graph);
        let aave_tokens = self.aave_tokens.clone();

        // Для каждого токена из Aave ищем циклические пути арбитража
        for start_token in aave_tokens {
            let mut visited_pools = HashSet::new();
            let mut current_path = vec![start_token];
            let mut current_pools = Vec::new();

            // Запускаем рекурсивный поиск путей с 2-4 хопами
            self.search_paths_dual_hops(
                start_token,
                start_token,
                &related_list,
                &mut visited_pools,
                &mut current_path,
                &mut current_pools,
                0,
            );
        }

        // Сохраняем найденные пути в JSON файл
        self.save_to_json("arbitrage_paths.json").unwrap();
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
        let mut related_list = HashMap::new();

        // Проходим по всем пулам в графе
        for entry in graph.nodes.iter() {
            let pool_address = *entry.key();
            let (token0, token1) = *entry.value();

            // Добавляем связь token0 -> token1 через данный пул
            related_list
                .entry(token0)
                .or_insert_with(Vec::new)
                .push((token1, pool_address));

            // Добавляем обратную связь token1 -> token0 через тот же пул
            related_list
                .entry(token1)
                .or_insert_with(Vec::new)
                .push((token0, pool_address));
        }

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
        let file = File::create(file_path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}
