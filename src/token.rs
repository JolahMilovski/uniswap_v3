use dashmap::DashMap;
use ethers::contract::abigen;
use ethers::prelude::*;
use ethers::types::Address;
use log::error;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, fs};
use tokio::time::sleep;

// Генерация биндингов для ERC20 контракта
// Создает структуру ERC20 с методами decimals() и symbol()
// Используется для взаимодействия с токенами на блокчейне
abigen!(
    ERC20,
    r#"[{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
    {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]"#
);

/// Структура для хранения основной информации о токене
/// Содержит минимально необходимые данные для работы с токеном в DeFi операциях
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    /// Символ токена (например, "USDC", "WETH", "DAI")
    pub symbol: String,
    /// Количество десятичных знаков токена (обычно 18 для большинства ERC20)
    pub decimals: u8,
}

/// Тип для потокобезопасного кэша токенов
/// Использует DashMap для concurrent доступа из разных потоков
/// Ключ - адрес токена, значение - информация о токене
pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;

/// Путь к JSON файлу для сохранения кэша токенов
/// Позволяет сохранять данные между перезапусками приложения
const TOKEN_CACHE_JSON_PATH: &str = "token_cache.json";

/// Получает информацию о токене по его адресу
/// Сначала проверяет кэш, если данных нет - делает запрос к блокчейну
/// 
/// # Аргументы
/// * `address` - Адрес токена в блокчейне
/// * `provider` - Провайдер для подключения к блокчейну (Infura, Alchemy и т.д.)
/// * `token_cache` - Кэш для хранения уже полученной информации о токенах
/// 
/// # Возвращает
/// Result с TokenInfo при успехе или ошибкой при неудаче
/// 
/// # Логика работы
/// 1. Проверяет наличие токена в кэше
/// 2. Если нет - создает контракт и вызывает методы symbol() и decimals()
/// 3. Валидирует полученный символ токена
/// 4. Сохраняет результат в кэш и JSON файл
/// 5. Добавляет задержку для избежания rate limiting
pub async fn get_single_token_data<M: Middleware + 'static>(
    address: Address,
    provider: Arc<M>,
    token_cache: &TokenCache,
) -> Result<TokenInfo,anyhow::Error > {

    // Проверяем кэш - если токен уже есть, возвращаем сохраненные данные
    if let Some(cached) = token_cache.get(&address) {
        return Ok(cached.clone());
    }

    // Создаем экземпляр ERC20 контракта для взаимодействия с токеном
    let contract = ERC20::new(address, provider.clone());

    // Добавляем задержку для избежания превышения лимитов RPC провайдера
    // 300ms между запросами - компромисс между скоростью и надежностью
    sleep(Duration::from_millis(300)).await;

    // Получаем символ токена с валидацией
    let symbol = if let Ok(sym) = contract.symbol().call().await {
        let sym_trimmed = sym.trim();
        
        // Проверяем, что символ не пустой и не содержит нежелательные слова
        if sym_trimmed.is_empty()
            || sym_trimmed.to_lowercase().contains("test")
            || sym_trimmed.to_lowercase().contains("null")
        {
            return Err(anyhow::anyhow!("Невалидный символ токена"));
        }
        
        // Проверяем символ регулярным выражением
        // Разрешены только буквы, цифры и подчеркивания, длина 1-20 символов
        if let Ok(re) = regex::Regex::new(r"^[a-zA-Z0-9_]{1,20}$") {
            if !re.is_match(sym_trimmed) {
                return Err(anyhow::anyhow!("Невалидный символ токена"));
            }
        }
        sym_trimmed.to_string()
    } else {
        // Если не удалось получить символ, возвращаем ошибку
        return Err(anyhow::anyhow!("Ошибка вызова symbol()"));
    };

    // Получаем количество десятичных знаков токена
    // Это критически важно для правильных расчетов сумм
    let decimals = contract.decimals().call().await?;

    // Создаем структуру с информацией о токене
    let token_info = TokenInfo {
        symbol: symbol.clone(),
        decimals,
    };

    // Сохраняем в кэш для быстрого доступа в будущем
    token_cache.insert(address, token_info.clone());

    // Асинхронно сохраняем кэш в JSON файл
    // Если сохранение не удалось, логируем ошибку, но не прерываем выполнение
    if let Err(e) = save_token_cache_to_json(token_cache).await {
        error!("[TOKEN][КЭШ] Ошибка записи JSON: {:?}", e);
    }

    Ok(token_info)
}

/// Сохраняет текущий кэш токенов в JSON файл
/// Позволяет сохранить данные между перезапусками приложения
/// 
/// # Аргументы
/// * `token_cache` - Кэш токенов для сохранения
/// 
/// # Возвращает
/// Result с ошибкой в случае проблем с записью
/// 
/// # Логика
/// 1. Конвертирует DashMap в обычный HashMap для сериализации
/// 2. Сериализует в JSON с красивым форматированием
/// 3. Записывает в файл
pub async fn save_token_cache_to_json(
    token_cache: &TokenCache,
) -> Result<(), Box<dyn std::error::Error>> {
    // Конвертируем DashMap в HashMap для сериализации
    // iter() возвращает итератор по парам ключ-значение
    let map: HashMap<_, _> = token_cache
        .iter()
        .map(|kv| (*kv.key(), kv.value().clone()))
        .collect();
    
    // Сериализуем в JSON с красивым форматированием (отступы, переносы строк)
    let json = serde_json::to_string_pretty(&map)?;
    
    // Записываем JSON в файл
    std::fs::write(TOKEN_CACHE_JSON_PATH, json)?;
    Ok(())
}

/// Загружает список токенов из JSON файла
/// Используется при запуске приложения для восстановления кэша
/// 
/// # Возвращает
/// HashMap с адресами токенов как ключами и TokenInfo как значениями
/// 
/// # Паника
/// Функция паникует если файл не найден или содержит невалидный JSON
/// Это сделано намеренно, так как отсутствие кэша критично для работы
/// 
/// # Логика
/// 1. Читает JSON файл как строку
/// 2. Парсит JSON в HashMap<String, TokenInfo>
/// 3. Конвертирует строковые адреса в Address типы
/// 4. Фильтрует невалидные адреса
pub fn load_token_list_from_json() -> HashMap<Address, TokenInfo> {
    // Читаем содержимое JSON файла
    let json = fs::read_to_string(TOKEN_CACHE_JSON_PATH)
        .expect("Не удалось прочитать token_cache.json");
    
    // Парсим JSON в промежуточную структуру со строковыми ключами
    let raw_map: HashMap<String, TokenInfo> =
        serde_json::from_str(&json).expect("Ошибка парсинга token_cache.json");

    // Конвертируем строковые адреса в Address типы
    raw_map
        .into_iter()
        .filter_map(|(addr_str, info)| {
            // Пытаемся распарсить строку как Ethereum адрес
            // Если не удается - пропускаем эту запись
            addr_str.parse::<Address>().ok().map(|addr| (addr, info))
        })
        .collect()
}
