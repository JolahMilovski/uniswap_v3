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
use std::time::Instant;
use tracing::{debug, warn, error};

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
        debug!("[ UNISWAP_GRAPH_new ] Создание нового универсального графа");
        UniversalGraph {
            nodes: DashMap::new(),
            edges: DashMap::new(),
        }
    }

    pub async fn upsert_pool(&self, new_pool: UniswapPool) -> Result<(), String> {
        let start = Instant::now();
        debug!(
            "[UNISWAP_GRAPH_upsert_pool] Начало обновления/вставки пула {:?}, fee: {}, liquidity: {}, sqrt_price: {}, tick_current: {}, tick_map_size: {}",
            new_pool.uniswap_pool_address,
            new_pool.uniswap_fee_tier,
            new_pool.uniswap_liquidity,
            new_pool.uniswap_sqrt_price,
            new_pool.uniswap_tick_current,
            new_pool.tick_map.len()
        );

        // Проверка входных данных
        if new_pool.uniswap_pool_address.is_zero() {
            error!("[UNISWAP_GRAPH_upsert_pool] Ошибка: адрес пула нулевой");
            return Err("Адрес пула нулевой".to_string());
        }
        if new_pool.tick_map.is_empty() {
            warn!("[UNISWAP_GRAPH_upsert_pool] Предупреждение: tick_map пустой для пула {:?}", new_pool.uniswap_pool_address);
        }

        debug!(
            "[UNISWAP_GRAPH_upsert_pool] Подробные данные пула: token_a: {:?}, token_b: {:?}, tick_lower: {}, tick_upper: {}, tick_spacing: {}, max_liquidity_per_tick: {}, is_active: {}",
            new_pool.uniswap_token_a,
            new_pool.uniswap_token_b,
            new_pool.uniswap_tick_lower,
            new_pool.uniswap_tick_upper,
            new_pool.uniswap_tick_spacing,
            new_pool.uniswap_max_liquidity_per_tick,
            new_pool.is_active
        );

        if new_pool.uniswap_liquidity > U256::from(u128::MAX) {
            warn!(
                "[UNISWAP_GRAPH_upsert_pool] Ликвидность пула превышает uint128: {}, пропуск пула",
                new_pool.uniswap_liquidity
            );
            return Err(format!("Ликвидность пула превышает uint128: {}", new_pool.uniswap_liquidity));
        }
        debug!(
            "[UNISWAP_GRAPH_upsert_pool] Проверка ликвидности пройдена: {} <= {}",
            new_pool.uniswap_liquidity,
            U256::from(u128::MAX)
        );

        if new_pool.uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            warn!(
                "[UNISWAP_GRAPH_upsert_pool] Максимальная ликвидность на тик превышает uint128: {}, пропуск пула",
                new_pool.uniswap_max_liquidity_per_tick
            );
            return Err(format!("Максимальная ликвидность на тик превышает uint128: {}", new_pool.uniswap_max_liquidity_per_tick));
        }
        debug!(
            "[UNISWAP_GRAPH_upsert_pool] Проверка max_liquidity_per_tick пройдена: {} <= {}",
            new_pool.uniswap_max_liquidity_per_tick,
            U256::from(u128::MAX)
        );

        let pool_address = *new_pool.uniswap_pool_address;
        let token_a = *new_pool.uniswap_token_a;
        let token_b = *new_pool.uniswap_token_b;

        debug!("[UNISWAP_GRAPH_upsert_pool] Извлечены pool_address: {:?}, token_a: {:?}, token_b: {:?}",
            pool_address,
            token_a,
            token_b
        );

        debug!("[UNISWAP_GRAPH_upsert_pool] Текущее количество пулов в edges: {}, узлов в nodes: {}", 
            self.edges.len(), self.nodes.len());
        debug!("[UNISWAP_GRAPH_upsert_pool] Уступаем управление другим задачам");
        tokio::task::yield_now().await;
        debug!("[UNISWAP_GRAPH_upsert_pool] Попытка доступа к edges для пула {:?}", pool_address);
        let get_mut_start = Instant::now();
        match tokio::time::timeout(std::time::Duration::from_secs(5), async {
            self.edges.get_mut(&pool_address)
        }).await {
            Ok(Some(mut existing_pool)) => {
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Доступ к edges получен за {:?}, найден существующий пул {:?}",
                    get_mut_start.elapsed(),
                    pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Текущие данные пула: liquidity: {}, sqrt_price: {}, tick_current: {}, tick_map_size: {}, is_active: {}",
                    existing_pool.uniswap_liquidity,
                    existing_pool.uniswap_sqrt_price,
                    existing_pool.uniswap_tick_current,
                    existing_pool.tick_map.len(),
                    existing_pool.is_active
                );

                existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Обновлена ликвидность: {}",
                    existing_pool.uniswap_liquidity
                );

                existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Обновлена sqrt_price: {}",
                    existing_pool.uniswap_sqrt_price
                );

                existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Обновлен текущий тик: {}",
                    existing_pool.uniswap_tick_current
                );

                existing_pool.is_active = new_pool.is_active;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Обновлен статус is_active: {}",
                    existing_pool.is_active
                );

                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Начало объединения tick_map, старый размер: {}, новый размер: {}",
                    existing_pool.tick_map.len(),
                    new_pool.tick_map.len()
                );
                let union_start = Instant::now();
                existing_pool.tick_map = existing_pool.tick_map.clone().union(new_pool.tick_map.clone());
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Завершено объединение tick_map за {:?}, итоговый размер: {}",
                    union_start.elapsed(),
                    existing_pool.tick_map.len()
                );

                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Обновлен пул: {:?}", pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Попытка вставки в nodes: pool_address: {:?}, tokens: ({:?}, {:?})",
                    pool_address,
                    token_a,
                    token_b
                );
                match self.nodes.insert(pool_address, (token_a, token_b)) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Обновлена запись в nodes для пула {:?}", pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Новая запись в nodes для пула {:?}", pool_address
                    ),
                }
                debug!("[UNISWAP_GRAPH_upsert_pool] Завершена вставка в nodes");
            }
            Ok(None) => {
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Пул {:?} не найден, создаем новый",
                    pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Вставка нового пула в edges: {:?}", pool_address
                );
                match self.edges.insert(pool_address, new_pool.clone()) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Обновлена запись в edges для пула {:?}", pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Новая запись в edges для пула {:?}", pool_address
                    ),
                }
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Вставка в nodes: pool_address: {:?}, tokens: ({:?}, {:?})",
                    pool_address,
                    token_a,
                    token_b
                );
                match self.nodes.insert(pool_address, (token_a, token_b)) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Обновлена запись в nodes для пула {:?}", pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] Новая запись в nodes для пула {:?}", pool_address
                    ),
                }
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] Новый пул {:?} успешно создан",
                    pool_address
                );
                // Проверяем, что пул действительно добавлен
                if self.edges.contains_key(&pool_address) {
                    debug!("[UNISWAP_GRAPH_upsert_pool] Подтверждено: пул {:?} присутствует в edges", pool_address);
                } else {
                    error!("[UNISWAP_GRAPH_upsert_pool] Ошибка: пул {:?} не был добавлен в edges", pool_address);
                    return Err(format!("Пул {:?} не был добавлен в edges", pool_address));
                }
                // Сохраняем граф после добавления пула
                if let Err(e) = self.save_graph_to_json("uniswap_graph_snapshot.json") {
                    error!("[UNISWAP_GRAPH_upsert_pool] Ошибка сохранения графа: {}", e);
                    return Err(format!("Ошибка сохранения графа: {}", e));
                }
                debug!("[UNISWAP_GRAPH_upsert_pool] Граф сохранен после добавления пула {:?}", pool_address);
            }
            Err(e) => {
                error!(
                    "[UNISWAP_GRAPH_upsert_pool] Тайм-аут при доступе к edges для пула {:?}: {}",
                    pool_address, e
                );
                return Err(format!("Тайм-аут при доступе к edges: {}", e));
            }
        }

        let duration = start.elapsed();
        debug!(
            "[UNISWAP_GRAPH_upsert_pool] Завершено выполнение upsert_pool для {:?}: время выполнения: {:?}",
            pool_address, duration
        );
        Ok(())
    }

    pub fn save_graph_to_json(&self, path: &str) -> std::io::Result<()> {
        debug!(
            "[ UNISWAP_GRAPH save_graph_to_json ] Начало сохранения графа в JSON файл: {}",
            path
        );
        let snapshot = self.snapshot();
        debug!(
            "[ UNISWAP_GRAPH save_graph_to_json] Создан снимок графа с {} узлами и {} ребрами",
            snapshot.nodes.len(),
            snapshot.edges.len()
        );

        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!("[ UNISWAP_GRAPH save_graph_to_json ] Ошибка сериализации JSON: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!("[ UNISWAP_GRAPH save_graph_to_json] Запись во временный файл: {}", temp_path);
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "[ UNISWAP_GRAPH save_graph_to_json ] Атомарная замена файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!("[ UNISWAP_GRAPH save_graph_to_json ] Граф успешно сохранен в файл: {}", path);
        Ok(())
    }

    pub fn get_pool_addresses(&self) -> HashSet<Address> {
        debug!("[ UNISWAP_GRAPH get_pool_addresses ] Получение адресов всех пулов");
        let addresses: HashSet<Address> = self.nodes.iter().map(|r| *r.key()).collect();
        debug!("[ UNISWAP_GRAPH get_pool_addresses] Найдено {} адресов пулов", addresses.len());
        addresses
    }

    pub fn update_pool_json(&self, pool_address: Address, path: &str) -> io::Result<()> {
        debug!(
            "[ UNISWAP_GRAPH ] Обновление JSON для пула {:?} в файле {}",
            pool_address, path
        );

        let mut snapshot = if Path::new(path).exists() {
            debug!(
                "[ UNISWAP_GRAPH ] Файл {} существует, загружаем текущие данные",
                path
            );
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            serde_json::from_str(&contents).map_err(|e| {
                debug!(
                    "[ UNISWAP_GRAPH ] Ошибка десериализации существующего JSON: {}",
                    e
                );
                io::Error::new(io::ErrorKind::Other, e)
            })?
        } else {
            debug!(
                "[ UNISWAP_GRAPH ] Файл {} не существует, создаем новый снимок",
                path
            );
            UniversalGraphSnapshot {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            }
        };

        if let Some(pool) = self.edges.get(&pool_address) {
            debug!(
                "[ UNISWAP_GRAPH ] Найден пул {:?} для обновления",
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
                "[ UNISWAP_GRAPH ] Данные пула {:?} обновлены в снимке",
                pool_address
            );
        } else {
            debug!("[ UNISWAP_GRAPH ] Пул {:?} не найден в графе", pool_address);
        }

        debug!("[ UNISWAP_GRAPH ] Сериализация обновленного снимка");
        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!(
                "[ UNISWAP_GRAPH ] Ошибка сериализации обновленного JSON: {}",
                e
            );
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!(
            "[ UNISWAP_GRAPH ] Запись обновленных данных во временный файл: {}",
            temp_path
        );

        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "[ UNISWAP_GRAPH ] Атомарная замена обновленного файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!(
            "[ UNISWAP_GRAPH ] JSON для пула {:?} успешно обновлен в файле {}",
            pool_address, path
        );
        Ok(())
    }

    fn snapshot(&self) -> UniversalGraphSnapshot {
        debug!("[ UNISWAP_GRAPH snapshot ] Создание снимка текущего состояния графа");
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
            "[ UNISWAP_GRAPH snapshot ] Снимок создан с {} узлами и {} ребрами",
            nodes_count, edges_count
        );
        snapshot
    }
}