use crate::{aave_v3_flash_monitor::AaveTokenLiquidity, uniswap_graph::UniversalGraph};
use arc_swap::ArcSwap;
use colored::Colorize;
use dashmap::DashMap;
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// Структура представляющая путь арбитража
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitragePath {
    /// Последовательность токенов в пути арбитража
    pub tokens: Vec<Address>,
    /// Последовательность пулов для обмена между токенами
    pub pools: Vec<Address>,
}

/// Информация о пуле для заимствования
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowPoolInfo {
    /// Адрес пула
    pub pool_address: Address,
    /// Уровень комиссии пула (например, 3000 для 0.3%)
    pub fee_tier: u32,
    /// Максимальная ликвидность в пуле (max(liquidity_token_a, liquidity_token_b))
    pub max_liquidity: U256,
}

/// Основная структура для построения и управления путями арбитража
#[derive(Debug, Serialize, Deserialize)]
pub struct PathBuilder {
    /// Найденные арбитражные пути
    pub paths: Vec<ArbitragePath>,
    /// Отображение пулов на индексы путей, в которых они участвуют
    pub pool_to_paths: DashMap<Address, Vec<usize>>,
    /// Пуллы для заимствования, сгруппированные по токенам
    pub borrow_pools: DashMap<Address, Vec<BorrowPoolInfo>>,
    /// Токены, доступные для флеш-займов в Aave
    aave_tokens: HashSet<Address>,
    /// Флаг завершения построения путей
    #[serde(skip)]
    pub is_paths_built: Arc<AtomicBool>,
    /// Для дедупликации путей
    #[serde(skip)]
    unique_paths: HashSet<(Vec<Address>, Vec<Address>)>,
}

impl PathBuilder {
    /// Создает новый экземпляр PathBuilder
pub fn new(
        aave_rx: watch::Receiver<AaveTokenLiquidity>,
        is_paths_built: Arc<AtomicBool>,
    ) -> Self {
        debug!(
            "[{}] Создание нового экземпляра PathBuilder",
            "UNISWAP_PATH_BUILDER 🧬".green()
        );
        let aave_tokens_borrow = aave_rx.borrow().aave_token_address.clone();
        
        warn!(
            "[{}] Загружено {} токенов из Aave для флеш-займов: {:?}",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            aave_tokens_borrow.len(),
            aave_tokens_borrow
        );

        Self {
            aave_tokens: aave_tokens_borrow,
            paths: Vec::new(),
            pool_to_paths: DashMap::new(),
            borrow_pools: DashMap::new(),
            is_paths_built,
            unique_paths: HashSet::new(),
        }
    }

    /// Основная функция построения всех возможных арбитражных путей
    pub fn build_all_paths(&mut self, graph: Arc<ArcSwap<UniversalGraph>>) {
        warn!(
            "[{}] Начало построения арбитражных путей",
            "UNISWAP_PATH_BUILDER 🧬".green()
        );
        
        // Очистка предыдущих данных
        self.clear_previous_data();

        if graph.load().nodes.is_empty() || graph.load().edges.is_empty() {
            error!(
                "[{}] Обнаружен пустой граф: nodes.len() = {}, edges.len() = {}",
                "UNISWAP_PATH_BUILDER 🧬".green(),
                graph.load().nodes.len(),
                graph.load().edges.len()
            );
            return;
        }

        // Построение связей между токенами
        let related_list = self.build_related_list(&graph);
        
        // Поиск путей для каждого токена Aave
        for start_token in self.aave_tokens.clone() {
            self.find_paths_for_token(start_token, &related_list);
        }

        self.finalize_path_construction();
    }

    /// Очищает предыдущие данные перед построением новых путей
    fn clear_previous_data(&mut self) {
        debug!(
            "[{}] Очистка предыдущих данных (было {} путей)",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            self.paths.len()
        );
        self.paths.clear();
        self.pool_to_paths.clear();
        self.borrow_pools.clear();
        self.unique_paths.clear();
    }


    /// Регистрирует найденный арбитражный путь
    fn register_arbitrage_path(&mut self, tokens: &[Address], pools: &[Address]) {
        let key = (tokens.to_vec(), pools.to_vec());
        if self.unique_paths.contains(&key) {
            debug!(
                "[{}] Дубликат пути {:?}, пропуск",
                "UNISWAP_PATH_BUILDER 🧬".green(),
                tokens
            );
            return;
        }
        self.unique_paths.insert(key);

        let path = ArbitragePath {
            tokens: tokens.to_vec(),
            pools: pools.to_vec(),
        };

        let path_index = self.paths.len();
        self.paths.push(path);

        // Индексация пулов
        for pool in pools {
            self.pool_to_paths
                .entry(*pool)
                .or_default()
                .push(path_index);
        }

        // Валидация доступности займа для промежуточных токенов
        for &token in &tokens[1..tokens.len() - 1] {
            if !self.borrow_pools.contains_key(&token) {
                warn!(
                    "[{}] Нет доступных пулов для займа промежуточного токена {:?}",
                    "UNISWAP_PATH_BUILDER 🧬".green(),
                    token
                );
            }
        }

        debug!(
            "[{}] Зарегистрирован новый путь #{}: токены {:?}, пулы {:?}",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            path_index,
            tokens,
            pools
        );
    }

    /// Обрабатывает отдельный пул, добавляя связи и информацию для займа
fn process_pool(
    &mut self,
    pool_address: Address,
    pool: &crate::uniswap_graph::UniswapPool,
    related_list: &mut HashMap<Address, Vec<(Address, Address)>>,
) {
    let token0 = *pool.uniswap_token_a;
    let token1 = *pool.uniswap_token_b;
    let fee_tier = pool.uniswap_fee_tier;
    let max_liquidity = pool.liquidity_token_a.max(pool.liquidity_token_b);

    debug!(
        "[{}] Обработка пула {:?}: токены ({:?}, {:?}), комиссия {}, max_liquidity {}",
        "UNISWAP_PATH_BUILDER 🧬".green(),
        pool_address,
        token0,
        token1,
        fee_tier,
        max_liquidity
    );

    // Добавление связей между токенами
    self.add_token_relation(related_list, token0, token1, pool_address);
    self.add_token_relation(related_list, token1, token0, pool_address);

    // Добавление информации о пуле для займа
    self.add_borrow_pool(token0, pool_address, fee_tier, max_liquidity);
    self.add_borrow_pool(token1, pool_address, fee_tier, max_liquidity);
}

/// Добавляет связь между токенами в граф
fn add_token_relation(
    &mut self, // Changed from &self to &mut self
    related_list: &mut HashMap<Address, Vec<(Address, Address)>>,
    from_token: Address,
    to_token: Address,
    pool_address: Address,
) {
    related_list
        .entry(from_token)
        .or_default()
        .push((to_token, pool_address));
}

/// Сортирует пулы для займа по максимальной ликвидности (убывание) и минимальной комиссии (возрастание)
fn sort_borrow_pools(&mut self) {
    for mut entry in self.borrow_pools.iter_mut() {
        entry.value_mut().sort_by(|a, b| {
            b.max_liquidity
                .cmp(&a.max_liquidity)
                .then_with(|| a.fee_tier.cmp(&b.fee_tier))
        });
        debug!(
            "[{}] Отсортированы пулы для токена {:?} по max_liquidity (descending) и fee_tier (ascending)",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            entry.key()
        );
    }
}

/// Строит граф связей между токенами и индексирует пулы для заимствования
fn build_related_list(
    &mut self,
    graph: &Arc<ArcSwap<UniversalGraph>>,
) -> HashMap<Address, Vec<(Address, Address)>> {
    debug!(
        "[{}] Построение графа связей между токенами",
        "UNISWAP_PATH_BUILDER 🧬".green()
    );
    let mut related_list = HashMap::new();

    for entry in graph.load().nodes.iter() {
        let pool_address = *entry.key();
        if let Some(pool) = graph.load().edges.get(&pool_address) {
            self.process_pool(pool_address, &pool, &mut related_list);
        }
    }

    // Сортировка пулов по максимальной ликвидности и минимальной комиссии
    self.sort_borrow_pools();
    warn!(
        "[{}] Построено {} связей между токенами",
        "UNISWAP_PATH_BUILDER 🧬".green(),
        related_list.len()
    );
    related_list
}

/// Находит оптимальный пул для займа указанного токена (топ по max_liquidity)
pub fn find_optimal_borrow_pool(
    &self,
    token: Address,
    _path: &[Address],
) -> Option<BorrowPoolInfo> {
    let pools = self.borrow_pools.get(&token)?;
    // Уже отсортировано по max_liquidity descending в build_related_list
    pools.iter().next().cloned()
}

/// Завершает процесс построения путей
fn finalize_path_construction(&mut self) {
    warn!(
        "[{}] Построение путей завершено. Найдено {} арбитражных путей",
        "UNISWAP_PATH_BUILDER 🧬".green(),
        self.paths.len()
    );

    if !self.paths.is_empty() {
        info!(
            "[{}] Сохранение путей в файл arbitrage_paths.json",
            "UNISWAP_PATH_BUILDER 🧬".green()
        );
        if let Err(e) = self.save_paths_to_json() {
            warn!(
                "[{}] Ошибка сохранения путей: {:?}",
                "UNISWAP_PATH_BUILDER 🧬".green(),
                e
            );
        } else {
            info!(
                "[{}] Пути успешно сохранены",
                "UNISWAP_PATH_BUILDER 🧬".green()
            );
        }
        self.is_paths_built.store(true, Ordering::SeqCst);
        debug!(
            "[{}] Флаг is_paths_built установлен в true",
            "UNISWAP_PATH_BUILDER 🧬".green()
        );
    } else {
        warn!(
            "[{}] Арбитражные пути не найдены",
            "UNISWAP_PATH_BUILDER 🧬".green()
        );
    }
}

/// Добавляет пул в список доступных для займа
fn add_borrow_pool(
    &mut self,
    token: Address,
    pool_address: Address,
    fee_tier: u32,
    max_liquidity: U256,
) {
    self.borrow_pools
        .entry(token)
        .or_default()
        .push(BorrowPoolInfo {
            pool_address,
            fee_tier,
            max_liquidity,
        });
}


    /// Находит все пути для конкретного начального токена
    fn find_paths_for_token(
        &mut self,
        start_token: Address,
        related_list: &HashMap<Address, Vec<(Address, Address)>>,
    ) {
        debug!(
            "[{}] Поиск путей для начального токена {:?}",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            start_token
        );

        let mut visited_pools = HashSet::new();
        let mut current_path = vec![start_token];
        let mut current_pools = Vec::new();

        self.search_paths_dual_hops(
            start_token,
            start_token,
            related_list,
            &mut visited_pools,
            &mut current_path,
            &mut current_pools,
            0,
        );

        debug!(
            "[{}] Завершен поиск путей для токена {:?}",
            "UNISWAP_PATH_BUILDER 🧬".green(),
            start_token
        );
    }

    /// Рекурсивный поиск путей с ограничением в 4 хопа
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
        // Ограничение глубины поиска
        if current_hops >= 4 {
            return;
        }

        // Проверка завершения цикла
        if (current_hops == 3 || current_hops == 4)
            && current_token == start_token
            && !current_pools.is_empty()
        {
            self.register_arbitrage_path(current_path, current_pools);
            return;
        }

        // Обработка связей текущего токена
        if let Some(connections) = related_list.get(&current_token) {
            for (next_token, pool_address) in connections {
                if !self.should_visit_pool(current_hops, *next_token, start_token, *pool_address, visited_pools) {
                    continue;
                }

                debug!(
                    "[{}] Переход от {:?} к {:?} через пул {:?}",
                    "UNISWAP_PATH_BUILDER 🧬".green(),
                    current_token,
                    next_token,
                    pool_address
                );

                // Рекурсивный поиск
                visited_pools.insert(*pool_address);
                current_path.push(*next_token);
                current_pools.push(*pool_address);

                self.search_paths_dual_hops(
                    start_token,
                    *next_token,
                    related_list,
                    visited_pools,
                    current_path,
                    current_pools,
                    current_hops + 1,
                );

                visited_pools.remove(pool_address);
                current_path.pop();
                current_pools.pop();
            }
        }
    }


    /// Определяет, следует ли посещать указанный пул
    fn should_visit_pool(
        &self,
        current_hops: usize,
        next_token: Address,
        start_token: Address,
        pool_address: Address,
        visited_pools: &HashSet<Address>,
    ) -> bool {
        !visited_pools.contains(&pool_address)
            && !(current_hops == 3 && next_token != start_token)
    }


    /// Сохраняет пути в JSON файл
    fn save_paths_to_json(&self) -> Result<(), Box<dyn std::error::Error>> {
        let file_path = "arbitrage_paths.json";
        let file = File::create(file_path)?;
        serde_json::to_writer_pretty(&file, self)?;
        Ok(())
    }
}