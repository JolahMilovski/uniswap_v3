use dashmap::DashMap;
use ethers::types::{Address, U256};
use im::OrdMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, DisplayFromStr, SerializeAs};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{rename, File};
use std::io::{Write};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, warn};
use std::collections::hash_map::RandomState;

// Структура для сериализации снимка графа в JSON
#[derive(Serialize, Deserialize)]
struct UniversalGraphSnapshot {
    nodes: HashMap<Address, (Address, Address)>,
    edges: HashMap<Address, UniswapPool>,
}

// Основная структура графа Uniswap V3
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UniversalGraph {
    pub nodes: DashMap<Address, (Address, Address), RandomState>, // Карта адресов пулов и их токенов (token_a, token_b)
    pub edges: DashMap<Address, UniswapPool, RandomState>,        // Карта адресов пулов и их данных
}

// Структура пула Uniswap V3 с добавленными полями ликвидности токенов
#[serde_as]
#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct UniswapPool {
    #[serde_as(as = "ArcAsInner")]
    pub graph_pool_address: Arc<Address>, // Адрес пула
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_dex: Arc<String>, 
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a: Arc<Address>, // Адрес первого токена (token0)
    pub uniswap_token_a_decimals: u8, // Десятичные разряды token0
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a_symbol: Arc<String>, // Символ token0
    #[serde_as(as = "DisplayFromStr")]
    pub liquidity_token_a: U256, // Доступная ликвидность token0
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b: Arc<Address>, // Адрес второго токена (token1)
    pub uniswap_token_b_decimals: u8, // Десятичные разряды token1
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b_symbol: Arc<String>, // Символ token1
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_liquidity: U256, // Общая ликвидность пула
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_sqrt_price: U256, // Квадратный корень текущей цены
    #[serde_as(as = "DisplayFromStr")]
    pub liquidity_token_b: U256, // Доступная ликвидность token1
    pub uniswap_tick_current: i32,    // Текущий тик
    pub uniswap_tick_lower: i32,      // Нижний тик пула
    pub uniswap_tick_upper: i32,      // Верхний тик пула
    pub uniswap_tick_spacing: i32,    // Шаг тиков
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_max_liquidity_per_tick: U256, // Максимальная ликвидность на тик
    pub uniswap_fee_tier: u32,        // Уровень комиссии пула (bps: 100, 500, 3000, 10000)
    #[serde_as(as = "OrdMapAsBTreeMap")]
    pub tick_map: OrdMap<i32, (i128, U256)>, // Карта тиков (тик -> (delta_liquidity, amount))
    pub is_active: bool,              // Активность пула
}

// Сериализация и десериализация Arc<T> для сохранения данных в JSON
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

// Сериализация и десериализация OrdMap<i32, (i128, U256)> как BTreeMap для JSON
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

impl UniversalGraph {
    /// Создает новый пустой граф с кастомным количеством шардов.
    ///
    /// Аргумент `shard_count` рекомендуется устанавливать равным ожидаемому
    /// количеству пулов (pool_count) для поведения "1 пул = 1 шард".
    ///
    /// Примечание: если shard_count != реальному количеству пулов — шардирование
    /// всё ещё будет работать, но в некоторых шардах могут оказаться несколько пулов.
    ///
    /// Проверка: shard_count должен быть степенью двойки для корректной работы DashMap.
    /// Это необходимо, так как DashMap использует битовый сдвиг для выбора шарда
    /// на основе хеша ключа. Если shard_count не степень двойки, распределение ключей
    /// по шардам становится неравномерным, что приводит к дисбалансу нагрузки
    /// и снижению производительности. Проверка использует битовую операцию
    /// `shard_count & (shard_count - 1) == 0`, которая подтверждает, что число
    /// имеет ровно один установленный бит (например, 1, 2, 4, 8, 16).
    pub fn new(shard_count: usize) -> Self {
        debug!("[UNISWAP_GRAPH_new] 🧿 Создание нового универсального графа с {} шардами", shard_count);
        assert!(shard_count > 0, "Количество шардов должно быть > 0");
        assert!(shard_count & (shard_count - 1) == 0, "Количество шардов должно быть степенью двойки");

        let hasher = RandomState::new();

        UniversalGraph {
            nodes: DashMap::with_hasher_and_shard_amount(hasher.clone(), shard_count),
            edges: DashMap::with_hasher_and_shard_amount(hasher, shard_count),
        }
    }

    /// Обновляет или вставляет пул в граф, включая ликвидность token0 и token1.
    ///
    /// Метод асинхронный, чтобы позволить использование тайм-аута при доступе к DashMap.
    /// Возвращает Ok(()) при успехе или Err с описанием ошибки.
    pub async fn upsert_pool(&self, new_pool: UniswapPool) -> Result<(), String> {
        let start = Instant::now();
        debug!(
            "[UNISWAP_GRAPH_upsert_pool]🧿 Начало обновления/вставки пула {:?}, fee: {}, liquidity: {}, sqrt_price: {}, tick_current: {}, tick_map_size: {}, liquidity_token0: {}, liquidity_token1: {}",
            new_pool.graph_pool_address,
            new_pool.uniswap_fee_tier,
            new_pool.uniswap_liquidity,
            new_pool.uniswap_sqrt_price,
            new_pool.uniswap_tick_current,
            new_pool.tick_map.len(),
            new_pool.liquidity_token_a,
            new_pool.liquidity_token_b
        );

        // Проверка входных данных
        if new_pool.graph_pool_address.is_zero() {
            error!("[UNISWAP_GRAPH_upsert_pool] 🧿🧿 Ошибка: адрес пула нулевой");
            return Err("Адрес пула нулевой".to_string());
        }

        debug!(
            "[UNISWAP_GRAPH_upsert_pool] 🧿 Подробные данные пула: token_a: {:?}, token_b: {:?}, tick_lower: {}, tick_upper: {}, tick_spacing: {}, max_liquidity_per_tick: {}, is_active: {}, liquidity_token0: {}, liquidity_token1: {}",
            new_pool.uniswap_token_a,
            new_pool.uniswap_token_b,
            new_pool.uniswap_tick_lower,
            new_pool.uniswap_tick_upper,
            new_pool.uniswap_tick_spacing,
            new_pool.uniswap_max_liquidity_per_tick,
            new_pool.is_active,
            new_pool.liquidity_token_a,
            new_pool.liquidity_token_b
        );

        // Проверка ликвидности пула
        if new_pool.uniswap_liquidity > U256::from(u128::MAX) {
            return Err(format!(
                "Ликвидность пула 🧿🧿 превышает uint128: {}",
                new_pool.uniswap_liquidity
            ));
        }

        // Проверка максимальной ликвидности на тик
        if new_pool.uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            return Err(format!(
                "🧿🧿Максимальная ликвидность на тик превышает uint128: {}",
                new_pool.uniswap_max_liquidity_per_tick
            ));
        }

        // Проверка ликвидности токенов
        if new_pool.liquidity_token_a > U256::from(u128::MAX)
            || new_pool.liquidity_token_b > U256::from(u128::MAX)
        {
            return Err(format!(
                "Ликвидность 🧿🧿 токенов превышает uint128: token0: {}, token1: {}",
                new_pool.liquidity_token_a, new_pool.liquidity_token_b
            ));
        }

        let pool_address = *new_pool.graph_pool_address;
        let token_a = *new_pool.uniswap_token_a;
        let token_b = *new_pool.uniswap_token_b;

        debug!("[UNISWAP_GRAPH_upsert_pool] 🧿 Извлечены pool_address: {:?}, token_a: {:?}, token_b: {:?}",
            pool_address,
            token_a,
            token_b
        );

        let get_mut_start = Instant::now();

        match tokio::time::timeout(std::time::Duration::from_secs(10), async {
            self.edges.get_mut(&pool_address)
        })
        .await
        {
            Ok(Some(mut existing_pool)) => {
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Доступ к edges получен за {:?}, найден существующий пул {:?}",
                    get_mut_start.elapsed(),
                    pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Текущие данные пула: liquidity: {}, sqrt_price: {}, tick_current: {}, tick_map_size: {}, liquidity_token0: {}, liquidity_token1: {}, is_active: {}",
                    existing_pool.uniswap_liquidity,
                    existing_pool.uniswap_sqrt_price,
                    existing_pool.uniswap_tick_current,
                    existing_pool.tick_map.len(),
                    existing_pool.liquidity_token_a,
                    existing_pool.liquidity_token_b,
                    existing_pool.is_active
                );

                // Обновление данных пула
                existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена ликвидность: {}",
                    existing_pool.uniswap_liquidity
                );

                existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена sqrt_price: {}",
                    existing_pool.uniswap_sqrt_price
                );

                existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлен текущий тик: {}",
                    existing_pool.uniswap_tick_current
                );

                existing_pool.is_active = new_pool.is_active;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлен статус is_active: {}",
                    existing_pool.is_active
                );

                existing_pool.liquidity_token_a = new_pool.liquidity_token_a;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool]🧿 Обновлена ликвидность token0: {}",
                    existing_pool.liquidity_token_a
                );

                existing_pool.liquidity_token_b = new_pool.liquidity_token_b;
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена ликвидность token1: {}",
                    existing_pool.liquidity_token_b
                );

                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Начало объединения tick_map, старый размер: {}, новый размер: {}",
                    existing_pool.tick_map.len(),
                    new_pool.tick_map.len()
                );

                let union_start = Instant::now();
                existing_pool.tick_map = existing_pool
                    .tick_map
                    .clone()
                    .union(new_pool.tick_map.clone());
                warn!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Завершено объединение tick_map за {} мкс, итоговый размер: {}",
                    union_start.elapsed().as_micros(),
                    existing_pool.tick_map.len()
                );

                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлен пул: {:?}",
                    pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Попытка вставки в nodes: pool_address: {:?}, tokens: ({:?}, {:?})",
                    pool_address,
                    token_a,
                    token_b
                );
                match self.nodes.insert(pool_address, (token_a, token_b)) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена запись в nodes для пула {:?}",
                        pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool]🧿 Новая запись в nodes для пула {:?}",
                        pool_address
                    ),
                }
                debug!("[UNISWAP_GRAPH_upsert_pool] 🧿 Завершена вставка в nodes");
            }
            Ok(None) => {
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool]🧿 Пул {:?} не найден, создаем новый",
                    pool_address
                );
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool]🧿🧿🧿 Вставка нового пула в edges: {:?}",
                    pool_address
                );
                match self.edges.insert(pool_address, new_pool.clone()) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена запись в edges для пула {:?}",
                        pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool]🧿 Новая запись в edges для пула {:?}",
                        pool_address
                    ),
                }
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Вставка в nodes: pool_address: {:?}, tokens: ({:?}, {:?})",
                    pool_address,
                    token_a,
                    token_b
                );
                match self.nodes.insert(pool_address, (token_a, token_b)) {
                    Some(_) => debug!(
                        "[UNISWAP_GRAPH_upsert_pool] 🧿 Обновлена запись в nodes для пула {:?}",
                        pool_address
                    ),
                    None => debug!(
                        "[UNISWAP_GRAPH_upsert_pool]🧿 Новая запись в nodes для пула {:?}",
                        pool_address
                    ),
                }
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿🧿🧿 Новый пул {:?} успешно создан",
                    pool_address
                );
                // Проверяем, что пул действительно добавлен
                if self.edges.contains_key(&pool_address) {
                    debug!("[UNISWAP_GRAPH_upsert_pool] 🧿 Подтверждено: пул {:?} присутствует в edges", pool_address);
                } else {
                    error!(
                        "[UNISWAP_GRAPH_upsert_pool]🧿🧿 Ошибка: пул {:?} не был добавлен в edges",
                        pool_address
                    );
                    return Err(format!("Пул {:?} не был добавлен в edges", pool_address));
                }
            }
            Err(e) => {
                error!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿🧿 Тайм-аут при доступе к edges для пула {:?}: {}",
                    pool_address, e
                );
                return Err(format!(" 🧿🧿 Тайм-аут при доступе к edges: {}", e));
            }
        }

        let duration = start.elapsed();
        warn!(
            "[UNISWAP_GRAPH_upsert_pool] 🧿 Завершено выполнение upsert_pool для {:?}: время выполнения: {:?}",
            pool_address, duration
        );
        Ok(())
    }

    /// Сохраняет граф в JSON-файл.
    ///
    /// Метод создает временный файл для атомарной записи, чтобы избежать повреждения данных.
    /// Возвращает Ok(()) при успехе или Err с ошибкой ввода/вывода.
    pub fn save_graph_to_json(&self, path: &str) -> std::io::Result<()> {
        debug!(
            "[UNISWAP_GRAPH_save_graph_to_json] 🧿 Начало сохранения графа в JSON файл: {}",
            path
        );
        let snapshot = self.snapshot();
        debug!(
            "[UNISWAP_GRAPH_save_graph_to_json] 🧿 Создан снимок графа с {} узлами и {} ребрами",
            snapshot.nodes.len(),
            snapshot.edges.len()
        );

        let json = serde_json::to_string_pretty(&snapshot).map_err(|e| {
            debug!(
                "[UNISWAP_GRAPH_save_graph_to_json]🧿🧿 Ошибка сериализации JSON: {}",
                e
            );
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;

        let temp_path = format!("{}.tmp", path);
        debug!(
            "[UNISWAP_GRAPH_save_graph_to_json]🧿 Запись во временный файл: {}",
            temp_path
        );
        let mut temp_file = File::create(&temp_path)?;
        temp_file.write_all(json.as_bytes())?;
        temp_file.flush()?;

        debug!(
            "[UNISWAP_GRAPH_save_graph_to_json] 🧿 Атомарная замена файла {} -> {}",
            temp_path, path
        );
        rename(temp_path, path)?;

        debug!(
            "[UNISWAP_GRAPH_save_graph_to_json] 🧿 Граф успешно сохранен в файл: {}",
            path
        );
        Ok(())
    }

    /// Возвращает адреса всех пулов.
    ///
    /// Собирает адреса из nodes в HashSet для уникальности.
    pub fn get_pool_addresses(&self) -> HashSet<Address> {
        debug!("[UNISWAP_GRAPH_get_pool_addresses] 🧿 Получение адресов всех пулов");
        let addresses: HashSet<Address> = self.nodes.iter().map(|r| *r.key()).collect();
        debug!(
            "[UNISWAP_GRAPH_get_pool_addresses] 🧿 Найдено {} адресов пулов",
            addresses.len()
        );
        addresses
    }

    /// Создает снимок графа для сериализации.
    ///
    /// Копирует данные из DashMap в HashMap для удобства сериализации.
    fn snapshot(&self) -> UniversalGraphSnapshot {
        debug!("[UNISWAP_GRAPH_snapshot] 🧿 Создание снимка текущего состояния графа");
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
            "[UNISWAP_GRAPH_snapshot] 🧿 Снимок создан с {} узлами и {} ребрами",
            nodes_count, edges_count
        );
        snapshot
    }
}