use crate::{aave_v3_flash_monitor::AaveTokenLiquidity, uniswap_graph::UniversalGraph};
use arc_swap::ArcSwap;
use colored::Colorize;
use dashmap::DashMap;
use ethers::types::Address;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::{atomic::{AtomicBool, Ordering}, Arc},
};
use tokio::sync::watch;
use tracing::{debug, error, info, trace, warn};

/// Структура представляющая путь арбитража
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitragePath {
    pub tokens: Vec<Address>,
    pub pools: Vec<Address>,
}

/// Основная структура для построения и управления путями арбитража
#[derive(Debug, Serialize, Deserialize)]
pub struct PathBuilder {
    pub paths: Vec<ArbitragePath>,
    pub pool_to_paths: DashMap<Address, Vec<usize>>,
    aave_tokens: HashSet<Address>,
     #[serde(skip)] // Исключаем из сериализации/десериализации
    pub is_paths_built: Arc<AtomicBool>,
}

impl PathBuilder {
    /// Создает новый экземпляр PathBuilder
       pub fn new(aave_rx: watch::Receiver<AaveTokenLiquidity>, is_paths_built: Arc<AtomicBool>) -> Self {
        debug!(
            "[{}] Начало создания нового экземпляра PathBuilder",
            "UNISWAP_PATH_BUILDER".green()
        );
        let aave_tokens_borrow = aave_rx.borrow().token_address.clone();
        warn!(
            "[{}] Загружено {} токенов из Aave для flash loan: {:?}",
            "UNISWAP_PATH_BUILDER".green(),
            aave_tokens_borrow.len(),
            aave_tokens_borrow
        );
        let result = Self {
            aave_tokens: aave_tokens_borrow,
            paths: Vec::new(),
            pool_to_paths: DashMap::new(),
            is_paths_built,
        };
        info!(
            "[{}] Экземпляр PathBuilder успешно создан",
            "UNISWAP_PATH_BUILDER".green()
        );
        result
    }

    /// Основная функция для построения всех возможных путей арбитража
     pub fn build_all_paths(&mut self, graph: Arc<ArcSwap<UniversalGraph>>) {
        info!("[{}] Начало построения всех арбитражных путей", "UNISWAP_PATH_BUILDER".green());
        debug!("[{}] Очистка предыдущих путей (было {} путей)", 
            "UNISWAP_PATH_BUILDER".green(), self.paths.len());
        self.paths.clear();
        debug!("[{}] Очистка предыдущих индексов pool_to_paths (было {} записей)", 
            "UNISWAP_PATH_BUILDER".green(), self.pool_to_paths.len());
        self.pool_to_paths.clear();

        if graph.load().nodes.is_empty() || graph.load().edges.is_empty() {
            error!("[{}] Граф пуст: nodes.len() = {}, edges.len() = {}. Пути не будут построены", 
                "UNISWAP_PATH_BUILDER".green(), graph.load().nodes.len(), graph.load().edges.len());
            return;
        }

        let related_list = self.build_related_list(&graph);
        warn!("[{}] Построено {} связей между токенами", 
            "UNISWAP_PATH_BUILDER".green(), related_list.len());

        if related_list.is_empty() {
            warn!("[{}] Список связей пуст, пути не будут построены", 
                "UNISWAP_PATH_BUILDER".green());
            return;
        }

        let aave_tokens = self.aave_tokens.clone();
        debug!("[{}] Начало поиска путей для {} токенов Aave", 
            "UNISWAP_PATH_BUILDER".green(), aave_tokens.len());

        for start_token in aave_tokens {
            debug!("[{}] Поиск путей для начального токена {:?}", 
                "UNISWAP_PATH_BUILDER".green(), start_token);
            let mut visited_pools = HashSet::new();
            let mut current_path = vec![start_token];
            let mut current_pools = Vec::new();
            self.search_paths_dual_hops(
                start_token,
                start_token,
                &related_list,
                &mut visited_pools,
                &mut current_path,
                &mut current_pools,
                0,
            );
            debug!("[{}] Завершён поиск путей для токена {:?}", 
                "UNISWAP_PATH_BUILDER".green(), start_token);
        }

        info!("[{}] Найдено {} арбитражных путей", 
            "UNISWAP_PATH_BUILDER".green(), self.paths.len());
        
        if !self.paths.is_empty() {
            info!("[{}] Сохранение путей в файл arbitrage_paths.json", 
                "UNISWAP_PATH_BUILDER".green());
            if let Err(e) = self.save_to_json("arbitrage_paths.json") {
                warn!("[{}] Ошибка сохранения путей в JSON: {:?}", 
                    "UNISWAP_PATH_BUILDER".green(), e);
            } else {
                info!("[{}] Пути успешно сохранены в arbitrage_paths.json", 
                    "UNISWAP_PATH_BUILDER".green());
                self.is_paths_built.store(true, Ordering::SeqCst);
                info!("[{}] Флаг is_paths_built установлен в true", 
                    "UNISWAP_PATH_BUILDER".green());
            }
        } else {
            warn!("[{}] Не найдено путей, пропуск сохранения в JSON", 
                "UNISWAP_PATH_BUILDER".green());
        }
    }

    /// Строит список связей между токенами на основе графа пулов Uniswap
fn build_related_list(&self, graph: &Arc<ArcSwap<UniversalGraph>>) -> HashMap<Address, Vec<(Address, Address)>> {
        debug!("[{}] Начало построения списка связей между токенами", 
            "UNISWAP_PATH_BUILDER".green());
        debug!("[{}] Граф содержит {} узлов и {} рёбер", 
            "UNISWAP_PATH_BUILDER".green(), graph.load().nodes.len(), graph.load().edges.len());
        let mut related_list = HashMap::new();
        let pool_count = graph.load().nodes.len();
        debug!("[{}] Обработка {} пулов из графа", 
            "UNISWAP_PATH_BUILDER".green(), pool_count);

        for entry in graph.load().nodes.iter() {
            let pool_address = *entry.key();
            if let Some(pool) = graph.load().edges.get(&pool_address) {
                let token0 = *pool.uniswap_token_a;
                let token1 = *pool.uniswap_token_b;
                trace!("[{}] Пул {}: {:?}, токены: ({:?}, {:?})", 
                    "UNISWAP_PATH_BUILDER".green(), pool_count, pool_address, token0, token1);

                related_list
                    .entry(token0)
                    .or_insert_with(Vec::new)
                    .push((token1, pool_address));
                debug!("[{}] Добавлена связь: токен {:?} -> токен {:?}", 
                    "UNISWAP_PATH_BUILDER".green(), token0, token1);

                related_list
                    .entry(token1)
                    .or_insert_with(Vec::new)
                    .push((token0, pool_address));
                debug!("[{}] Добавлена обратная связь: токен {:?} -> токен {:?}", 
                    "UNISWAP_PATH_BUILDER".green(), token1, token0);
            } else {
                warn!("[{}] Пул {:?} отсутствует в edges, пропуск", 
                    "UNISWAP_PATH_BUILDER".green(), pool_address);
            }
        }

        warn!("[{}] Список связей построен, содержит {} токенов", 
            "UNISWAP_PATH_BUILDER".green(), related_list.len());
        related_list
    }

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
        debug!(
            "[{}] Поиск путей: текущий токен {:?}, хопы: {}, путь: {:?}, пулы: {:?}",
            "UNISWAP_PATH_BUILDER".green(),
            current_token,
            current_hops,
            current_path,
            current_pools
        );

        if current_hops >= 4 {
            return;
        }

        if (current_hops == 3 || current_hops == 4)
            && current_token == start_token
            && !current_pools.is_empty()
        {
            let path = ArbitragePath {
                tokens: current_path.clone(),
                pools: current_pools.clone(),
            };
            let path_index = self.paths.len();
            self.paths.push(path);
            for pool in current_pools.iter() {
                self.pool_to_paths
                    .entry(*pool)
                    .or_insert_with(Vec::new)
                    .push(path_index);
            }
            debug!(
                "[{}] Найден путь #{}: токены={:?}, пулы={:?}",
                "UNISWAP_PATH_BUILDER".green(),
                path_index,
                current_path,
                current_pools
            );
            if current_hops == 4 {
                return;
            }
        }

        if let Some(connections) = related_list.get(&current_token) {
            for (next_token, pool_address) in connections {
                if visited_pools.contains(pool_address) {
                    continue;
                }
                if current_hops == 3 && *next_token != start_token {
                    continue;
                }
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
        } else {
            debug!(
                "[{}] Нет связанных токенов для {:?}",
                "UNISWAP_PATH_BUILDER".green(),
                current_token
            );
        }
    }

    pub fn save_to_json(
        &self,
        file_path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "[{}] Начало сохранения PathBuilder в JSON файл {:?}",
            "UNISWAP_PATH_BUILDER".green(),
            file_path.as_ref()
        );
        let file = File::create(file_path.as_ref())?;
        serde_json::to_writer_pretty(&file, self)?;
        info!(
            "[{}] Сохранение в JSON завершено успешно",
            "UNISWAP_PATH_BUILDER".green()
        );
        Ok(())
    }


    
}
