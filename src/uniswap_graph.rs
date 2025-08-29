
use dashmap::DashMap;
use ethers::types::{Address, U256};
use im::OrdMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs, DisplayFromStr};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{rename, File};
use std::io::{Write};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, warn};
use std::collections::hash_map::RandomState;

// Тип для представления чисел в формате Q96.64
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Q96_64 {
    pub(crate) value: U256, // Хранит число в формате Q96.64 (96 бит целая часть, 64 бита дробная)
}

impl Q96_64 {
    // Константа для масштабирования дробной части (2^64)
    const FRACTION_DENOMINATOR: U256 = U256([0, 1, 0, 0]); // 2^64

    // Создание из U256 (предполагается, что значение уже в формате Q96.64)
    pub fn from_u256(value: U256) -> Result<Self, String> {
        if value >> 160 != U256::zero() {
            return Err("Переполнение Q96.64".to_string());
        }
        Ok(Q96_64 { value })
    }

    // Конвертация в U256
    pub fn to_u256(&self) -> U256 {
        self.value
    }

    // Создание из целой и дробной части
    pub fn from_parts(integer: u128, fraction: u64) -> Result<Self, String> {
        if integer >> 96 != 0 {
            return Err("Переполнение целой части Q96.64".to_string());
        }
        let value = (U256::from(integer) << 64) | U256::from(fraction);
        Ok(Q96_64 { value })
    }

    // Получение целой части (96 бит)
    pub fn integer_part(&self) -> U256 {
        self.value >> 64
    }

    // Получение дробной части (64 бита)
    pub fn fractional_part(&self) -> U256 {
        self.value & (Self::FRACTION_DENOMINATOR - U256::one())
    }

    // Сложение с проверкой переполнения
    pub fn add(self, other: Q96_64) -> Result<Self, String> {
        let result = self.value.checked_add(other.value).ok_or("Переполнение при сложении Q96.64")?;
        if result >> 160 != U256::zero() {
            return Err("Переполнение Q96.64".to_string());
        }
        Ok(Q96_64 { value: result })
    }

    // Вычитание с проверкой переполнения
    pub fn sub(self, other: Q96_64) -> Result<Self, String> {
        let result = self.value.checked_sub(other.value).ok_or("Переполнение при вычитании Q96.64")?;
        Ok(Q96_64 { value: result })
    }

    // Умножение с учётом масштабирования
    pub fn mul(self, other: Q96_64) -> Result<Self, String> {
        let (high, low) = full_multiply(self.value, other.value);
        let scaled = (high << 64) | (low >> 64);
        if scaled >> 160 != U256::zero() {
            return Err("Переполнение при умножении Q96.64".to_string());
        }
        Ok(Q96_64 { value: scaled })
    }

    // Деление с учётом масштабирования
    pub fn div(self, other: Q96_64) -> Result<Self, String> {
        if other.value.is_zero() {
            return Err("Деление на ноль в Q96.64".to_string());
        }
        let scaled = (self.value << 64) / other.value;
        if scaled >> 160 != U256::zero() {
            return Err("Переполнение при делении Q96.64".to_string());
        }
        Ok(Q96_64 { value: scaled })
    }
}

// Вспомогательная функция для умножения U256
fn full_multiply(a: U256, b: U256) -> (U256, U256) {
    let a_high = a >> 128;
    let a_low = a & ((U256::from(1) << 128) - U256::from(1));
    let b_high = b >> 128;
    let b_low = b & ((U256::from(1) << 128) - U256::from(1));

    let low_low = a_low * b_low;
    let high_low = a_high * b_low;
    let low_high = a_low * b_high;
    let high_high = a_high * b_high;

    let intermediate = low_low
        .checked_add((high_low << 128).checked_add(low_high << 128).unwrap())
        .unwrap();
    let high = high_high
        .checked_add(high_low >> 128)
        .unwrap()
        .checked_add(low_high >> 128)
        .unwrap();
    (high, intermediate)
}

// Сериализация Q96_64 как строки
impl Serialize for Q96_64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!(
            "{}.{}",
            self.integer_part(),
            self.fractional_part()
        );
        serializer.serialize_str(&s)
    }
}

// Десериализация Q96_64 из строки
impl<'de> Deserialize<'de> for Q96_64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            return Err(serde::de::Error::custom("Неверный формат Q96.64"));
        }
        let integer = U256::from_dec_str(parts[0]).map_err(serde::de::Error::custom)?;
        let fraction = U256::from_dec_str(parts[1]).map_err(serde::de::Error::custom)?;
        if integer >> 96 != U256::zero() || fraction >> 64 != U256::zero() {
            return Err(serde::de::Error::custom("Переполнение при десериализации Q96.64"));
        }
        Q96_64::from_parts(integer.as_u128(), fraction.as_u64())
            .map_err(serde::de::Error::custom)
    }
}

// Реализация SerializeAs для serde_with
struct Q96_64AsString;

impl SerializeAs<Q96_64> for Q96_64AsString {
    fn serialize_as<S>(source: &Q96_64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        source.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, Q96_64> for Q96_64AsString {
    fn deserialize_as<D>(deserializer: D) -> Result<Q96_64, D::Error>
    where
        D: Deserializer<'de>,
    {
        Q96_64::deserialize(deserializer)
    }
}

// Структура для сериализации снимка графа в JSON
#[derive(Serialize, Deserialize)]
struct UniversalGraphSnapshot {
    nodes: HashMap<Address, (Address, Address)>,
    edges: HashMap<Address, UniswapPool>,
}

// Основная структура графа Uniswap V3
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UniversalGraph {
    pub nodes: DashMap<Address, (Address, Address), RandomState>,
    pub edges: DashMap<Address, UniswapPool, RandomState>,
}

// Структура пула Uniswap V3
#[serde_as]
#[derive(Serialize, Default, Clone, Debug, Deserialize)]
pub struct UniswapPool {
    #[serde_as(as = "ArcAsInner")]
    pub graph_pool_address: Arc<Address>,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_dex: Arc<String>,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a: Arc<Address>,
    pub uniswap_token_a_decimals: u8,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_a_symbol: Arc<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub liquidity_token_a: U256,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b: Arc<Address>,
    pub uniswap_token_b_decimals: u8,
    #[serde_as(as = "ArcAsInner")]
    pub uniswap_token_b_symbol: Arc<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub uniswap_liquidity: U256,
    #[serde_as(as = "Q96_64AsString")]
    pub uniswap_sqrt_price: Q96_64, // Изменено на Q96.64
    #[serde_as(as = "DisplayFromStr")]
    pub liquidity_token_b: U256,
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

// Сериализация и десериализация Arc<T>
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
        D: Deserializer<'de>,
    {
        Ok(Arc::new(T::deserialize(deserializer)?))
    }
}

// Сериализация и десериализация OrdMap<i32, (i128, U256)>
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

    pub async fn upsert_pool(&self, new_pool: UniswapPool) -> Result<(), String> {
        let start = Instant::now();
        debug!(
            "[UNISWAP_GRAPH_upsert_pool]🧿 Начало обновления пула {:?}, fee: {}, liquidity: {}, sqrt_price: {}.{}, tick_current: {}, tick_map_size: {}, liquidity_token0: {}, liquidity_token1: {}",
            new_pool.graph_pool_address,
            new_pool.uniswap_fee_tier,
            new_pool.uniswap_liquidity,
            new_pool.uniswap_sqrt_price.integer_part(),
            new_pool.uniswap_sqrt_price.fractional_part(),
            new_pool.uniswap_tick_current,
            new_pool.tick_map.len(),
            new_pool.liquidity_token_a,
            new_pool.liquidity_token_b
        );

        if new_pool.graph_pool_address.is_zero() {
            error!("[UNISWAP_GRAPH_upsert_pool] 🧿🧿 Ошибка: адрес пула нулевой");
            return Err("Адрес пула нулевой".to_string());
        }

        if new_pool.uniswap_liquidity > U256::from(u128::MAX) {
            return Err(format!(
                "Ликвидность пула 🧿🧿 превышает uint128: {}",
                new_pool.uniswap_liquidity
            ));
        }

        if new_pool.uniswap_max_liquidity_per_tick > U256::from(u128::MAX) {
            return Err(format!(
                "🧿🧿Максимальная ликвидность на тик превышает uint128: {}",
                new_pool.uniswap_max_liquidity_per_tick
            ));
        }

        if new_pool.liquidity_token_a > U256::from(u128::MAX)
            || new_pool.liquidity_token_b > U256::from(u128::MAX)
        {
            return Err(format!(
                "Ликвидность 🧿🧿 токенов превышает uint128: token0: {}, token1: {}",
                new_pool.liquidity_token_a, new_pool.liquidity_token_b
            ));
        }

        if new_pool.uniswap_sqrt_price.to_u256() >> 160 != U256::zero() {
            return Err(format!(
                "🧿🧿 Значение uniswap_sqrt_price превышает Q96.64: {}",
                new_pool.uniswap_sqrt_price.to_u256()
            ));
        }

        let pool_address = *new_pool.graph_pool_address;
        let token_a = *new_pool.uniswap_token_a;
        let token_b = *new_pool.uniswap_token_b;

        debug!(
            "[UNISWAP_GRAPH_upsert_pool] 🧿 Извлечены pool_address: {:?}, token_a: {:?}, token_b: {:?}", 
            pool_address, token_a, token_b
        );

        let get_mut_start = Instant::now();
        match tokio::time::timeout(std::time::Duration::from_secs(10), async {
            self.edges.get_mut(&pool_address)
        })
        .await
        {
            Ok(Some(mut existing_pool)) => {
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Доступ к edges за {:?}, найден пул {:?}", 
                    get_mut_start.elapsed(), pool_address
                );
                existing_pool.uniswap_liquidity = new_pool.uniswap_liquidity;
                existing_pool.uniswap_sqrt_price = new_pool.uniswap_sqrt_price;
                existing_pool.uniswap_tick_current = new_pool.uniswap_tick_current;
                existing_pool.is_active = new_pool.is_active;
                existing_pool.liquidity_token_a = new_pool.liquidity_token_a;
                existing_pool.liquidity_token_b = new_pool.liquidity_token_b;
                existing_pool.tick_map = existing_pool.tick_map.clone().union(new_pool.tick_map.clone());
                debug!(
                    "[UNISWAP_GRAPH_upsert_pool] 🧿 Вставка в nodes: pool_address: {:?}, tokens: ({:?}, {:?})",
                    pool_address, token_a, token_b
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
                    pool_address, token_a, token_b
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

    pub fn get_pool_addresses(&self) -> HashSet<Address> {
        debug!("[UNISWAP_GRAPH_get_pool_addresses] 🧿 Получение адресов всех пулов");
        let addresses: HashSet<Address> = self.nodes.iter().map(|r| *r.key()).collect();
        debug!(
            "[UNISWAP_GRAPH_get_pool_addresses] 🧿 Найдено {} адресов пулов",
            addresses.len()
        );
        addresses
    }

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
