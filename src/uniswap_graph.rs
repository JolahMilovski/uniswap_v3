use dashmap::DashMap;
use ethers::types::{Address, U512};
use im::OrdMap;
use log::debug;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fs::{File, rename};
use std::io::{self, Read, Write};
use std::path::Path;


#[derive(Serialize, Deserialize)]
struct UniversalGraphSnapshot {
    nodes: HashMap<Address, (Address, Address)>, // адрес пула : адреса токенов
    edges: HashMap<Address, UniswapPool>, // адрес пула : данные пула
}


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UniversalGraph {
    /// Ключ: адрес пула  
    /// Значение: (токен0, токен1)
    pub nodes: DashMap<Address, (Address, Address)>,
    /// Ключ: адрес пула  
    /// Значение: структура пула
    pub edges: DashMap<Address, UniswapPool>,
}


#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct UniswapPool {
    pub uniswap_pool_address: Address,
    pub uniswap_dex: String,
    pub uniswap_token_a: Address,
    pub uniswap_token_a_decimals: u8,
    pub uniswap_token_a_symbol: String,
    pub uniswap_token_b: Address,
    pub uniswap_token_b_decimals: u8,
    pub uniswap_token_b_symbol: String,
      #[serde(serialize_with = "serialize_u512")]
    pub uniswap_liquidity: U512,
      #[serde(serialize_with = "serialize_u512")]
    pub uniswap_sqrt_price: U512,
      #[serde(serialize_with = "serialize_u512")]
    pub uniswap_current_price: U512,
    pub uniswap_tick_current: i32,
    pub uniswap_tick_lower: i32,
    pub uniswap_tick_upper: i32,
    pub uniswap_tick_spacing: i32,
      #[serde(serialize_with = "serialize_u512")]
    pub uniswap_max_liquidity_per_tick: U512,
    pub uniswap_fee_tier: u32,
    #[serde(serialize_with = "serialize_tick_map")]
    pub tick_map: OrdMap<i32, (i128, U512)>,
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
        &self,
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
        tick_map: OrdMap<i32, (i128, U512)>,
        is_active: bool,
    ) {
        self.nodes
            .insert(uniswap_pool_address, (uniswap_token_a, uniswap_token_b));

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
                tick_map,
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
            existing_pool.tick_map = existing_pool.tick_map.clone().union(new_pool.tick_map.clone());

            debug!("UNISAWP_GRAPH_Обновлен пул: {:?}", new_pool.uniswap_pool_address);
            self.nodes.insert(new_pool.uniswap_pool_address, (new_pool.uniswap_token_a, new_pool.uniswap_token_b));
        } else {
            let pool_address = new_pool.uniswap_pool_address;
            let token_a = new_pool.uniswap_token_a;
            let token_b = new_pool.uniswap_token_b;

            self.edges.insert(pool_address, new_pool);
            self.nodes.insert(pool_address, (token_a, token_b));
        }
    }

    pub fn save_graph_to_json(&self, path: &str) -> std::io::Result<()> {
        let snapshot = self.snapshot();
        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Записываем во временный файл для атомарности
        let temp_path = format!("{}.tmp", path);
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        // Атомарно заменяем оригинальный файл
        rename(temp_path, path)?;
        Ok(())
    }

    pub fn update_pool_json(&self, pool_address: Address, path: &str) -> io::Result<()> {
        // Загружаем текущий JSON, если он существует
        let mut snapshot = if Path::new(path).exists() {
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        } else {
            UniversalGraphSnapshot {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            }
        };

        // Обновляем данные для пула
        if let Some(pool) = self.edges.get(&pool_address) {
            snapshot.edges.insert(pool_address, pool.clone());
            snapshot.nodes.insert(pool_address, self.nodes.get(&pool_address).map(|v| *v.value()).unwrap_or_default());
        }

        // Сериализуем обновлённый снимок
        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Записываем во временный файл для атомарности
        let temp_path = format!("{}.tmp", path);
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        // Атомарно заменяем оригинальный файл
        rename(temp_path, path)?;
        Ok(())
    }

    fn snapshot(&self) -> UniversalGraphSnapshot {
        UniversalGraphSnapshot {
            nodes: self
                .nodes
                .iter()
                .map(|r| (*r.key(), *r.value()))
                .collect(),
            edges: self
                .edges
                .iter()
                .map(|r| (*r.key(), r.value().clone()))
                .collect(),
        }
    }
}


// Сериализатор для U512 в десятичную строку
fn serialize_u512<S>(value: &U512, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}
/*
// Сериализатор для i32 в десятичное число
fn serialize_i32<S>(value: &i32, serializer: S) -> Result<S::Ok, S::Error>
where
S: Serializer,
{
    serializer.serialize_i32(*value)``
}

// Сериализатор для u32 в десятичное число
fn serialize_u32<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
S: Serializer,
{
    serializer.serialize_u32(*value)
}

// Сериализатор для u8 в десятичное число
fn serialize_u8<S>(value: &u8, serializer: S) -> Result<S::Ok, S::Error>
where
S: Serializer,
{
    serializer.serialize_u8(*value)
}
*/

// Сериализатор для OrdMap
fn serialize_tick_map<S>(value: &OrdMap<i32, (i128, U512)>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;
    
    let mut map = serializer.serialize_map(Some(value.len()))?;
    for (k, (net, gross)) in value.iter() {
        map.serialize_entry(
            &k.to_string(), // Ключ как строка
            &(net.to_string(), gross.to_string()) // Значения как строки
        )?;
    }
    map.end()
}