use dashmap::DashMap;
use ethers::types::{Address, U512};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UniversalGraph {
    /// Ключ: адрес пула  
    /// Значение: (токен0, токен1)
    pub nodes: DashMap<Address, (Address, Address)>,
    /// Ключ: адрес пула  
    /// Значение: структура пула
    pub edges: DashMap<Address, UniswapPool>,
}

#[derive(Serialize, Deserialize)]
struct UniversalGraphSnapshot {
    nodes: HashMap<Address, (Address, Address)>, // адрес пула : адреса токенов
    edges: HashMap<Address, UniswapPool>, //адрес пула : данные пула
}

#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct UniswapPool {
    // Основные параметры пула
    pub uniswap_pool_address: Address,
    pub uniswap_dex: String,
    // ТОКЕН А
    pub uniswap_token_a: Address,
    pub uniswap_token_a_decimals: u8,
    pub uniswap_token_a_symbol: String,
    // ТОКЕН B
    pub uniswap_token_b: Address,
    pub uniswap_token_b_decimals: u8,
    pub uniswap_token_b_symbol: String,
    // Ликвидность
    pub uniswap_liquidity: U512,
    // Цена, тики, комиссии
    pub uniswap_sqrt_price: U512,
    pub uniswap_current_price: U512,
    pub uniswap_tick_current: i32,
    pub uniswap_tick_lower: i32,
    pub uniswap_tick_upper: i32,
    pub uniswap_tick_spacing: i32,
    pub uniswap_max_liquidity_per_tick: U512,
    pub uniswap_fee_tier: u32,
    #[serde(skip_serializing_if = "DashMap::is_empty",serialize_with = "serialize_tick_map")]
    pub tick_map: DashMap<i32, (i128, U512)>, // Изменили на DashMap
    pub is_active: bool,
}

impl UniversalGraph {

    pub fn new() -> Self {
        UniversalGraph {
            nodes: DashMap::new(),
            edges: DashMap::new(),
        }
    }

    pub fn add_pool(
        &self, // Изменили на &self, так как DashMap уже обеспечивает внутреннюю синхронизацию
        uniswap_pool_address: Address,
        uniswap_dex: String,
        uniswap_token_a: Address,
        uniswap_token_a_decimals: u8,
        uniswap_token_a_symbol: String,
        uniswap_token_b: Address,
        uniswap_token_b_decimals: u8,
        uniswap_token_b_symbol: String,
        uniswap_liquidity: U512,
        uniswap_sqrt_price: U512,
        uniswap_current_price: U512,
        uniswap_tick_current: i32,
        uniswap_tick_lower: i32,
        uniswap_tick_upper: i32,
        uniswap_tick_spacing: i32,
        uniswap_max_liquidity_per_tick: U512,
        uniswap_fee_tier: u32,
        tick_map: DashMap<i32, (i128, U512)>,
        is_active: bool,
    ) {
        self.nodes
            .insert(uniswap_pool_address, (uniswap_token_a, uniswap_token_b));

        let dash_tick_map = DashMap::new();
        for (tick, data) in tick_map {
            dash_tick_map.insert(tick, data);
        }

        self.edges.insert(
            uniswap_pool_address,
            UniswapPool {
                uniswap_pool_address,
                uniswap_dex,
                uniswap_token_a,
                uniswap_token_a_decimals,
                uniswap_token_a_symbol,
                uniswap_token_b,
                uniswap_token_b_decimals,
                uniswap_token_b_symbol,
                uniswap_liquidity,
                uniswap_sqrt_price,
                uniswap_current_price,
                uniswap_tick_current,
                uniswap_tick_lower,
                uniswap_tick_upper,
                uniswap_tick_spacing,
                uniswap_max_liquidity_per_tick,
                uniswap_fee_tier,
                tick_map: dash_tick_map,
                is_active,
            },
        );
    }
      
    pub fn upsert_pool(&self, new_pool: UniswapPool) {

        if let Some(mut existing_pool) = self.edges.get_mut(&new_pool.uniswap_pool_address) {
            existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
            existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
            existing_pool.uniswap_current_price = new_pool.uniswap_current_price;
            existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
            existing_pool.is_active = new_pool.is_active;

            // Обновляем tick_map
            existing_pool.tick_map.clear();
            for (tick, data) in new_pool.tick_map.into_iter() {
                existing_pool.tick_map.insert(tick, data);
            }

            debug!(
                "UNISAWP_GRAPH_Обновлен пул: {:?}",
                new_pool.uniswap_pool_address
            );
            self.nodes.insert(new_pool.uniswap_pool_address, (new_pool.uniswap_token_a, new_pool.uniswap_token_b));
        }
        
        else
        
        {
            let pool_address = new_pool.uniswap_pool_address;
            let token_a = new_pool.uniswap_token_a;
            let token_b = new_pool.uniswap_token_b;

            self.edges.insert(pool_address, new_pool);
            self.nodes.insert(pool_address, (token_a, token_b));

        }
    }

    pub fn save_to_bin(&self, path: &str) -> io::Result<()> {
        let snapshot = self.snapshot();
        let serialized =
            bincode::serialize(&snapshot).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = File::create(path)?;
        file.write_all(&serialized)?;
        Ok(())
    }
    


    pub fn load_from_bin(path: &str) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        bincode::deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn save_to_bin_json(&self, path: &str) -> std::io::Result<()> {
        let snapshot = self.snapshot();
        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    fn snapshot(&self) -> UniversalGraphSnapshot {
        UniversalGraphSnapshot {
            nodes: self
                .nodes
                .iter()
                .map(
                    |r: dashmap::mapref::multiple::RefMulti<
                        '_,
                        ethers::types::H160,
                        (ethers::types::H160, ethers::types::H160),
                    >| (*r.key(), *r.value()),
                )
                .collect(),
            edges: self
                .edges
                .iter()
                .map(|r| (*r.key(), r.value().clone()))
                .collect(),
        }
    }
}

fn serialize_tick_map<S>(
    tick_map: &DashMap<i32, (i128, U512)>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(tick_map.len()))?;
    for entry in tick_map.iter() {
        map.serialize_entry(
            &entry.key().to_string(),
            &(entry.value().0.to_string(), entry.value().1.to_string()),
        )?;
    }
    map.end()
}
