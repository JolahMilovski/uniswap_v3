use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use ethers::types::Address;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{aave_v3_flash_monitor::AaveTokenLiquidity, uniswap_graph::UniversalGraph};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitragePath {
    pub tokens: Vec<Address>,
    pub pools: Vec<Address>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PathBuilder {
    pub paths: Vec<ArbitragePath>,
    pub pool_to_paths: HashMap<Address, Vec<usize>>,
    aave_tokens: HashSet<Address>,
}

impl PathBuilder {    
    pub fn new(aave_rx: watch::Receiver<AaveTokenLiquidity>) -> Self {
        let aave_tokens_borrow = aave_rx.borrow().token_address.clone();
        Self {
            aave_tokens: aave_tokens_borrow,
            paths: Vec::new(),
            pool_to_paths: HashMap::new(),
        }
    }

    pub fn build_all_paths(&mut self, graph: &Arc<UniversalGraph>) {
        self.paths.clear();
        self.pool_to_paths.clear();

        let related_list = self.build_related_list(&graph);
        let aave_tokens = self.aave_tokens.clone();

        for start_token in aave_tokens {
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
        }

        self.save_to_json("arbitrage_paths.json").unwrap();
    }

    fn build_related_list(&self, graph: &UniversalGraph) -> HashMap<Address, Vec<(Address, Address)>> {
        let mut related_list = HashMap::new();

        for entry in graph.nodes.iter() {
            let pool_address = *entry.key();
            let (token0, token1) = *entry.value();

            related_list.entry(token0)
                .or_insert_with(Vec::new)
                .push((token1, pool_address));

            related_list.entry(token1)
                .or_insert_with(Vec::new)
                .push((token0, pool_address));
        }

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

            if current_hops == 4 {
                return; // больше не углубляемся
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
        }
    }

    pub fn save_to_json(&self, file_path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(file_path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}
