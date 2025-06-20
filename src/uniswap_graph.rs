use dashmap::DashMap;
use ethers::types::{Address, U256};
use im::OrdMap;
use log::{debug, warn};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs, DisplayFromStr};
use std::collections::{HashMap, HashSet};
use std::fs::{File, rename};
use std::io::{self, Read, Write};
use std::path::Path;
use std::collections::BTreeMap;

struct OrdMapAsBTreeMap;

impl SerializeAs<OrdMap<i32, (i128, U256)>> for OrdMapAsBTreeMap {
    fn serialize_as<S>(source: &OrdMap<i32, (i128, U256)>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let btree_map: BTreeMap<i32, (i128, String)> = source
            .iter()
            .map(|(k, (v1, v2))| (*k, (*v1, v2.to_string())))
            .collect();
        btree_map.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, OrdMap<i32, (i128, U256)>> for OrdMapAsBTreeMap {
    fn deserialize_as<D>(deserializer: D) -> Result<OrdMap<i32, (i128, U256)>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let btree_map: BTreeMap<i32, (i128, String)> =
            BTreeMap::deserialize(deserializer)?;
        let ord_map = btree_map
            .into_iter()
            .map(|(k, (v1, v2))| {
                let u256 = U256::from_dec_str(&v2).map_err(serde::de::Error::custom)?;
                Ok((k, (v1, u256)))
            })
            .collect::<Result<OrdMap<i32, (i128, U256)>, D::Error>>()?;
        Ok(ord_map)
    }
}

#[derive(Serialize, Deserialize)]
struct UniversalGraphSnapshot {
    nodes: HashMap<Address, (Address, Address)>, // адрес пула : адреса токенов
    edges: HashMap<Address, UniswapPool>,        // адрес пула : данные пула
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

#[serde_as]
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
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_liquidity: U256, // uint128 в Uniswap V3
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_sqrt_price: U256, // uint160 в Uniswap V3
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_current_price: U256, // uint256 в Uniswap V3
    pub uniswap_tick_current: i32,
    pub uniswap_tick_lower: i32,
    pub uniswap_tick_upper: i32,
    pub uniswap_tick_spacing: i32,
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_max_liquidity_per_tick: U256, // uint128 в Uniswap V3
    pub uniswap_fee_tier: u32,
    #[serde_as(as = "OrdMapAsBTreeMap")]
    pub tick_map: OrdMap<i32, (i128, U256)>, // liquidity_gross: uint128 в Uniswap V3
    pub is_active: bool,
}

impl UniversalGraph {

    pub fn new() -> Self {
        debug!("UNISWAP_GRAPH: Создание нового универсального графа");
        
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
        uniswap_liquidity: U256,
        uniswap_sqrt_price: U256,
        uniswap_current_price: U256,
        uniswap_tick_current: i32,
        uniswap_tick_lower: i32,
        uniswap_tick_upper: i32,
        uniswap_tick_spacing: i32,
        uniswap_max_liquidity_per_tick: U256,
        uniswap_fee_tier: u32,
        tick_map: OrdMap<i32, (i128, U256)>,
        is_active: bool,
    ) {
        debug!(
            "UNISWAP_GRAPH: Добавление пула {:?} с токенами {} ({:?}) и {} ({:?})",
            uniswap_pool_address,
            uniswap_token_a_symbol,
            uniswap_token_a,
            uniswap_token_b_symbol,
            uniswap_token_b
        );

        // Проверка диапазона для uint128
        if uniswap_liquidity > U256::from(u128::MAX) {
            warn!(
                "UNISWAP_GRAPH: Ликвидность пула превышает uint128: {}, пропуск пула",
                uniswap_liquidity
            );
            return;
        }
        if uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            warn!(
                "UNISWAP_GRAPH: Максимальная ликвидность на тик превышает uint128: {}, пропуск пула",
                uniswap_max_liquidity_per_tick
            );
            return;
        }

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

        debug!("UNISWAP_GRAPH: Пул {:?} успешно добавлен", uniswap_pool_address);
    }

    pub fn upsert_pool(&self, new_pool: UniswapPool) {
        debug!(
            "UNISWAP_GRAPH: Обновление/вставка пула {:?}",
            new_pool.uniswap_pool_address
        );

        // Проверка диапазона для uint128
        if new_pool.uniswap_liquidity > U256::from(u128::MAX) {
            warn!(
                "UNISWAP_GRAPH: Ликвидность пула превышает uint128: {}, пропуск пула",
                new_pool.uniswap_liquidity
            );
            return;
        }
        if new_pool.uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            warn!(
                "UNISWAP_GRAPH: Максимальная ликвидность на тик превышает uint128: {}, пропуск пула",
                new_pool.uniswap_max_liquidity_per_tick
            );
            return;
        }

        if let Some(mut existing_pool) = self.edges.get_mut(&new_pool.uniswap_pool_address) {
            debug!(
                "UNISWAP_GRAPH: Найден существующий пул {:?}, обновляем данные",
                new_pool.uniswap_pool_address
            );

            existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
            existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
            existing_pool.uniswap_current_price = new_pool.uniswap_current_price;
            existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
            existing_pool.is_active = new_pool.is_active;
            existing_pool.tick_map = existing_pool
                .tick_map
                .clone()
                .union(new_pool.tick_map.clone());

            debug!(
                "UNISWAP_GRAPH: Обновлен пул: {:?}",
                new_pool.uniswap_pool_address
            );
            self.nodes.insert(
                new_pool.uniswap_pool_address,
                (new_pool.uniswap_token_a, new_pool.uniswap_token_b),
            );
        } else {
            debug!(
                "UNISWAP_GRAPH: Пул {:?} не найден, создаем новый",
                new_pool.uniswap_pool_address
            );

            let pool_address = new_pool.uniswap_pool_address;
            let token_a = new_pool.uniswap_token_a;
            let token_b = new_pool.uniswap_token_b;

            self.edges.insert(pool_address, new_pool);
            self.nodes.insert(pool_address, (token_a, token_b));

            debug!("UNISWAP_GRAPH: Новый пул {:?} успешно создан", pool_address);
        }
    }

    pub fn save_graph_to_json(&self, path: &str) -> std::io::Result<()> {
        debug!("UNISWAP_GRAPH: Начало сохранения графа в JSON файл: {}", path);

        let snapshot = self.snapshot();
        debug!(
            "UNISWAP_GRAPH: Создан снимок графа с {} узлами и {} ребрами",
            snapshot.nodes.len(),
            snapshot.edges.len()
        );

        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!("UNISWAP_GRAPH: Ошибка сериализации JSON: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!("UNISWAP_GRAPH: Запись во временный файл: {}", temp_path);

        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "UNISWAP_GRAPH: Атомарная замена файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!("UNISWAP_GRAPH: Граф успешно сохранен в файл: {}", path);
        Ok(())
    }

    pub fn get_pool_addresses(&self) -> HashSet<Address> {
        debug!("UNISWAP_GRAPH: Получение адресов всех пулов");
        let addresses: HashSet<Address> = self.nodes.iter().map(|r| *r.key()).collect();
        debug!("UNISWAP_GRAPH: Найдено {} адресов пулов", addresses.len());
        addresses
    }

    pub fn update_pool_json(&self, pool_address: Address, path: &str) -> io::Result<()> {
        debug!(
            "UNISWAP_GRAPH: Обновление JSON для пула {:?} в файле {}",
            pool_address, path
        );

        let mut snapshot = if Path::new(path).exists() {
            debug!("UNISWAP_GRAPH: Файл {} существует, загружаем текущие данные", path);
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents).map_err(|e| {
                debug!("UNISWAP_GRAPH: Ошибка десериализации существующего JSON: {}", e);
                io::Error::new(io::ErrorKind::Other, e)
            })?
        } else {
            debug!("UNISWAP_GRAPH: Файл {} не существует, создаем новый снимок", path);
            UniversalGraphSnapshot {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            }
        };

        if let Some(pool) = self.edges.get(&pool_address) {
            debug!("UNISWAP_GRAPH: Найден пул {:?} для обновления", pool_address);
            snapshot.edges.insert(pool_address, pool.clone());
            snapshot.nodes.insert(
                pool_address,
                self.nodes
                    .get(&pool_address)
                    .map(|v| *v.value())
                    .unwrap_or_default(),
            );
            debug!("UNISWAP_GRAPH: Данные пула {:?} обновлены в снимке", pool_address);
        } else {
            debug!("UNISWAP_GRAPH: Пул {:?} не найден в графе", pool_address);
        }

        debug!("UNISWAP_GRAPH: Сериализация обновленного снимка");
        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!("UNISWAP_GRAPH: Ошибка сериализации обновленного JSON: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!(
            "UNISWAP_GRAPH: Запись обновленных данных во временный файл: {}",
            temp_path
        );

        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "UNISWAP_GRAPH: Атомарная замена обновленного файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!(
            "UNISWAP_GRAPH: JSON для пула {:?} успешно обновлен в файле {}",
            pool_address, path
        );
        Ok(())
    }

    fn snapshot(&self) -> UniversalGraphSnapshot {
        debug!("UNISWAP_GRAPH: Создание снимка текущего состояния графа");
        let nodes_count = self.nodes.len();
        let edges_count = self.edges.len();

        let snapshot = UniversalGraphSnapshot {
            nodes: self.nodes.iter().map(|r| (*r.key(), *r.value())).collect(),
            edges: self
                .edges
                .iter()
                .map(|r| (*r.key(), r.value().clone()))
                .collect(),
        };

        debug!(
            "UNISWAP_GRAPH: Снимок создан с {} узлами и {} ребрами",
            nodes_count, edges_count
        );
        snapshot
    }
}