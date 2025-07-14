use dashmap::DashMap;
use ethers::types::{Address, U256};
use im::OrdMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, DisplayFromStr, SerializeAs};
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fs::{rename, File};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};

struct ArcAsInner;

impl<T: Serialize> SerializeAs<Arc<T>> for ArcAsInner {
    fn serialize_as<S>(source: &Arc<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        source.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> DeserializeAs<'de, Arc<T>> for ArcAsInner {
    fn deserialize_as<D>(deserializer: D) -> Result<Arc<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Arc::new(T::deserialize(deserializer)?))
    }
}

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
        let btree_map: BTreeMap<i32, (i128, String)> = BTreeMap::deserialize(deserializer)?;
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
    nodes: HashMap<Address, (Address, Address)>,
    edges: HashMap<Address, UniswapPool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UniversalGraph {
    pub nodes: DashMap<Address, (Address, Address)>,
    pub edges: DashMap<Address, UniswapPool>,
}

#[serde_as]
#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct UniswapPool {
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_pool_address: Arc<Address>,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_dex: Arc<String>,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a: Arc<Address>,
    pub uniswap_token_a_decimals: u8,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a_symbol: Arc<String>,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b: Arc<Address>,
    pub uniswap_token_b_decimals: u8,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b_symbol: Arc<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_liquidity: U256,
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_sqrt_price: U256,
    pub uniswap_tick_current: i32,
    pub uniswap_tick_lower: i32,
    pub uniswap_tick_upper: i32,
    pub uniswap_tick_spacing: i32,
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_max_liquidity_per_tick: U256,
    pub uniswap_fee_tier: u32,
    #[serde_as(as = "OrdMapAsBTreeMap")]
    pub tick_map: OrdMap<i32, (i128, U256)>,
    pub is_active: bool,
}

impl UniversalGraph {
    pub fn new() -> Self {
        debug!("[UNISWAP_GRAPH] Создание нового универсального графа");
        UniversalGraph {
            nodes: DashMap::new(),
            edges: DashMap::new(),
        }
    }

    pub fn upsert_pool(&self, new_pool: UniswapPool) {
        debug!(
            "[UNISWAP_GRAPH] Обновление/вставка пула {:?}",
            new_pool.uniswap_pool_address
        );

        if new_pool.uniswap_liquidity > U256::from(u128::MAX) {
            warn!(
                "[UNISWAP_GRAPH] Ликвидность пула превышает uint128: {}, пропуск пула",
                new_pool.uniswap_liquidity
            );
            return;
        }
        if new_pool.uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            warn!(
                "[UNISWAP_GRAPH] Максимальная ликвидность на тик превышает uint128: {}, пропуск пула",
                new_pool.uniswap_max_liquidity_per_tick
            );
            return;
        }

        let pool_address = *new_pool.uniswap_pool_address;
        let token_a = *new_pool.uniswap_token_a;
        let token_b = *new_pool.uniswap_token_b;

        if let Some(mut existing_pool) = self.edges.get_mut(&pool_address) {
            debug!(
                "[UNISWAP_GRAPH] Найден существующий пул {:?}, обновляем данные",
                pool_address
            );
            existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
            existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
            existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
            existing_pool.is_active = new_pool.is_active;
            existing_pool.tick_map = existing_pool
                .tick_map
                .clone()
                .union(new_pool.tick_map.clone());
            debug!("[UNISWAP_GRAPH] Обновлен пул: {:?}", pool_address);
            self.nodes.insert(pool_address, (token_a, token_b));
        } else {
            debug!(
                "[UNISWAP_GRAPH] Пул {:?} не найден, создаем новый",
                pool_address
            );

            self.edges.insert(pool_address, new_pool);
            self.nodes.insert(pool_address, (token_a, token_b));
            debug!(
                "[UNISWAP_GRAPH] Новый пул {:?} успешно создан",
                pool_address
            );
        }
    }

    pub fn save_graph_to_json(&self, path: &str) -> std::io::Result<()> {
        debug!(
            "[UNISWAP_GRAPH] Начало сохранения графа в JSON файл: {}",
            path
        );
        let snapshot = self.snapshot();
        debug!(
            "[UNISWAP_GRAPH] Создан снимок графа с {} узлами и {} ребрами",
            snapshot.nodes.len(),
            snapshot.edges.len()
        );

        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!("[UNISWAP_GRAPH] Ошибка сериализации JSON: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!("[UNISWAP_GRAPH] Запись во временный файл: {}", temp_path);
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "[UNISWAP_GRAPH] Атомарная замена файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!("[UNISWAP_GRAPH] Граф успешно сохранен в файл: {}", path);
        Ok(())
    }

    pub fn get_pool_addresses(&self) -> HashSet<Address> {
        debug!("[UNISWAP_GRAPH] Получение адресов всех пулов");
        let addresses: HashSet<Address> = self.nodes.iter().map(|r| *r.key()).collect();
        debug!("[UNISWAP_GRAPH] Найдено {} адресов пулов", addresses.len());
        addresses
    }

    pub fn update_pool_json(&self, pool_address: Address, path: &str) -> io::Result<()> {
        debug!(
            "[UNISWAP_GRAPH] Обновление JSON для пула {:?} в файле {}",
            pool_address, path
        );

        let mut snapshot = if Path::new(path).exists() {
            debug!(
                "[UNISWAP_GRAPH] Файл {} существует, загружаем текущие данные",
                path
            );
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents).map_err(|e| {
                debug!(
                    "[UNISWAP_GRAPH] Ошибка десериализации существующего JSON: {}",
                    e
                );
                io::Error::new(io::ErrorKind::Other, e)
            })?
        } else {
            debug!(
                "[UNISWAP_GRAPH] Файл {} не существует, создаем новый снимок",
                path
            );
            UniversalGraphSnapshot {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            }
        };

        if let Some(pool) = self.edges.get(&pool_address) {
            debug!(
                "[UNISWAP_GRAPH] Найден пул {:?} для обновления",
                pool_address
            );
            snapshot.edges.insert(pool_address, pool.clone());
            snapshot.nodes.insert(
                pool_address,
                self.nodes
                    .get(&pool_address)
                    .map(|v| *v.value())
                    .unwrap_or_default(),
            );
            debug!(
                "[UNISWAP_GRAPH] Данные пула {:?} обновлены в снимке",
                pool_address
            );
        } else {
            debug!("[UNISWAP_GRAPH] Пул {:?} не найден в графе", pool_address);
        }

        debug!("[UNISWAP_GRAPH] Сериализация обновленного снимка");
        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!(
                "[UNISWAP_GRAPH] Ошибка сериализации обновленного JSON: {}",
                e
            );
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!(
            "[UNISWAP_GRAPH] Запись обновленных данных во временный файл: {}",
            temp_path
        );

        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "[UNISWAP_GRAPH] Атомарная замена обновленного файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!(
            "[UNISWAP_GRAPH] JSON для пула {:?} успешно обновлен в файле {}",
            pool_address, path
        );
        Ok(())
    }

    fn snapshot(&self) -> UniversalGraphSnapshot {
        debug!("[UNISWAP_GRAPH] Создание снимка текущего состояния графа");
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
            "[UNISWAP_GRAPH] Снимок создан с {} узлами и {} ребрами",
            nodes_count, edges_count
        );
        snapshot
    }
}
