use ethers::types::{Address, U512};
use std::{collections::HashMap, io::Write};
use std::fs::File;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};
use serde_json::{json, Value};
use log::{info, warn, error};

pub struct UniversalGraph {
    pub nodes: HashMap<Address, u8>, // Хранит адреса токенов и их decimal
    pub edges: HashMap<Address, UniswapPool>, // Хранит пулы по их адресу
}

#[derive(Serialize,Clone,Debug,Deserialize )]
pub struct UniswapPool {
    // Основные параметры пула
    pub uniswap_pool_address: Address,      // Адрес пула
    pub uniswap_dex: String,                // Название DEX (например, "uniswap_v3")
    //   ТОКЕН А
    pub uniswap_token_a: Address,           // Адрес токена A
    pub uniswap_token_a_decimals: u8,       // Количество десятичных знаков токена A
    pub uniswap_token_a_symbol: String,     // Символ токена A (например, "ETH")
    //ТОКЕН B
    pub uniswap_token_b: Address,           // Адрес токена B
    pub uniswap_token_b_decimals: u8,       // Количество десятичных знаков токена B
    pub uniswap_token_b_symbol: String,     // Символ токена B (например, "USDC")
    // Ликвидность
    pub uniswap_liquidity: U512,     // Общая ликвидность пула
    // Цена, тики, комиссии
    pub uniswap_sqrt_price: U512,           // Квадратный корень цены (используется в Uniswap V3)
    pub uniswap_current_price: U512,         // Текущая цена токена A относительно токена B (вычисляется из sqrt_price)
    pub uniswap_tick_current: i32,          // Текущий тик (соответствует текущей цене)
    pub uniswap_tick_lower: i32,            // Нижняя граница диапазона ликвидности
    pub uniswap_tick_upper: i32,            // Верхняя граница диапазона ликвидности
    pub uniswap_tick_spacing: i32,          // Расстояние между тиками (определяется уровнем комиссии)
    pub uniswap_max_liquidity_per_tick: U512, // Максимальная ликвидность на одном тике
    pub uniswap_fee_tier: u32,              // Уровень комиссии (например, 3000 для 0.3%)
    #[serde(skip_serializing_if = "HashMap::is_empty", serialize_with = "serialize_tick_map")]
    pub tick_map: HashMap<i32, (i128, U512)>, // Новое поле для тиковой карты
}

impl UniversalGraph {
    pub fn new() -> Self {
        UniversalGraph {
            nodes: HashMap::new(),
            edges: HashMap::new(),
        }
    }     
    
    
    ///добавляет данные в граф
    pub fn add_pool(
        &mut self,
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
        uniswap_current_price:U512,
        uniswap_tick_current: i32,
        uniswap_tick_lower: i32,
        uniswap_tick_upper: i32,
        uniswap_tick_spacing: i32,
        uniswap_max_liquidity_per_tick: U512,
        uniswap_fee_tier: u32,
        tick_map: HashMap<i32, (i128, U512)>,      
    ) {          
               // Добавляем токены в nodes
        self.nodes.insert(uniswap_token_a, uniswap_token_a_decimals);
        self.nodes.insert(uniswap_token_b, uniswap_token_b_decimals);

        // Добавляем пул в edges
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
                uniswap_fee_tier,
                uniswap_max_liquidity_per_tick,
                tick_map
            },
        );        
    }

    pub fn save_pool_to_file(&self) -> std::io::Result<()> {

        let file_path = "uniswap_pools_data.json";        
        // 1. Проверка наличия данных
        if self.edges.is_empty() {
            warn!("Попытка сохранения пустого графа. edges содержит 0 пулов.");
            return Err(Error::new(
                ErrorKind::InvalidData, 
                "Нет данных для сохранения (edges пуст)"
            ));
        }
        info!("Начинаем сохранение {} пулов в файл {}", self.edges.len(), file_path);

        // 2. Подготовка данных с безопасной обработкой чисел
        let pools_dec: Vec<Value> = self.edges.values()
            .map(|pool| {
                json!({
                    "uniswap_pool_address": format!("{:?}", pool.uniswap_pool_address),
                    "uniswap_dex": pool.uniswap_dex,
                    "uniswap_token_a": format!("{:?}", pool.uniswap_token_a),
                    "uniswap_token_a_decimals": pool.uniswap_token_a_decimals,
                    "uniswap_token_a_symbol": pool.uniswap_token_a_symbol,
                    "uniswap_token_b": format!("{:?}", pool.uniswap_token_b),
                    "uniswap_token_b_decimals": pool.uniswap_token_b_decimals,
                    "uniswap_token_b_symbol": pool.uniswap_token_b_symbol,
                    "uniswap_liquidity": pool.uniswap_liquidity.to_string(),
                    "uniswap_sqrt_price": pool.uniswap_sqrt_price.to_string(),
                    "uniswap_current_price": pool.uniswap_current_price.to_string(),
                    "uniswap_tick_current": pool.uniswap_tick_current,
                    "uniswap_tick_lower": pool.uniswap_tick_lower,
                    "uniswap_tick_upper": pool.uniswap_tick_upper,
                    "uniswap_tick_spacing": pool.uniswap_tick_spacing,
                    "uniswap_max_liquidity_per_tick": pool.uniswap_max_liquidity_per_tick.to_string(),
                    "uniswap_fee_tier": pool.uniswap_fee_tier,
                    "tick_map": pool.tick_map.iter().map(|(k, (liquidity_net, sqrt_price_x96))| {
                    (
                        k.to_string(),
                        (
                            liquidity_net.to_string(),
                            sqrt_price_x96.to_string()
                        )
                    )
                }).collect::<HashMap<String, (String, String)>>()
            })
            })
            .collect();
                    

        // 3. Создание файла
        let mut file = match File::create(file_path) {
            Ok(f) => {
                info!("Файл {} успешно создан", file_path);
                f
            },
            Err(e) => {
                error!("Не удалось создать файл {}: {}", file_path, e);
                return Err(e);
            }
        };

        // 4. Сериализация и запись с обработкой ошибок
        info!("Начало сериализации данных...");
        let json_string = match serde_json::to_string_pretty(&pools_dec) {
            Ok(s) => s,
            Err(e) => {
                error!("Ошибка сериализации: {}", e);
                
                // Попробуем сохранить хотя бы часть данных для диагностики
                let fallback_path = "uniswap_pools_fallback.json";
                warn!("Попытка сохранить упрощенные данные в {}", fallback_path);
                
                let simplified: Vec<Value> = pools_dec.iter()
                    .map(|pool| json!({
                        "pool": pool["uniswap_pool_address"],
                        "token_a": pool["uniswap_token_a"],
                        "token_b": pool["uniswap_token_b"]
                    }))
                    .collect();
                
                if let Ok(simple_str) = serde_json::to_string(&simplified) {
                    let _ = std::fs::write(fallback_path, simple_str);
                }
                
                return Err(Error::new(ErrorKind::InvalidData, format!("Ошибка сериализации: {}", e)));
            }
        }; 

        // 5. Запись в файл
        match file.write_all(json_string.as_bytes()) {
            Ok(_) => {
                info!("Данные успешно записаны в файл");
                match file.sync_all() {
                    Ok(_) => {
                        info!("Синхронизация файла завершена. Всего сохранено {} пулов", pools_dec.len());
                        Ok(())
                    },
                    Err(e) => {
                        error!("Ошибка синхронизации файла: {}", e);
                        Err(e)
                    }
                }
            },
            Err(e) => {
                error!("Ошибка записи в файл: {}", e);
                Err(e)
            }
        }
    }
}



// Кастомный сериализатор для tick_map

fn serialize_tick_map<S>(tick_map: &HashMap<i32, (i128, U512)>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(tick_map.len()))?;
    for (tick, (liquidity_net, sqrt_price_x96)) in tick_map {
        map.serialize_entry(
            &tick.to_string(),
            &(
                liquidity_net.to_string(),
                sqrt_price_x96.to_string()
            )
        )?;
    }
    map.end()
}
