pub mod uniswap_v3;
pub mod uniswap_graph;
pub mod token;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod provider;

use dashmap::{DashMap, DashSet};
use provider::ProviderManager;

use uniswap_cache::UniswapPoolCache;
use uniswap_events::UniswapEventSubscriber;
use uniswap_graph::UniversalGraph;

use crate::token::TokenInfo;
use token::load_token_cache;

use ethers::types::Address;
use dotenv::dotenv;
use env_logger::Env;
use env_logger::Builder;
use log::{error, info};
use std::{env, sync::Arc};
use std::io::Write;

use tokio::sync::Mutex;

use lazy_static::lazy_static;

use std::sync::atomic::AtomicU64;

lazy_static! {
    // Глобальные переменные для отслеживания диапазона блоков
    pub static ref SYNC_START_BLOCK: AtomicU64 = AtomicU64::new(0);
    pub static ref SYNC_END_BLOCK: AtomicU64 = AtomicU64::new(0);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {


    //подключаем .ENV
    dotenv().ok();
    
    //подключаем логирование
   Builder::from_env(Env::default().default_filter_or("info"))
        .format(|buf, record| {
            // Определяем цвета и стили
            let level_color = match record.level() {
                log::Level::Error => "\x1b[31;1m",  // Красный жирный
                log::Level::Warn => "\x1b[33;1m",   // Желтый жирный
                log::Level::Info => "\x1b[32;1m",    // Зеленый жирный
                log::Level::Debug => "\x1b[36;1m",   // Голубой жирный
                log::Level::Trace => "\x1b[35;1m",   // Пурпурный жирный
            };
            
            // Эмодзи для уровней
            let level_emoji = match record.level() {
                log::Level::Error => "🔥",  // Огонь для ошибок
                log::Level::Warn => "⚠️",   // Предупреждение
                log::Level::Info => "ℹ️",   // Информация
                log::Level::Debug => "🐞",  // Жук для дебага
                log::Level::Trace => "🔬",  // Лупа для трассировки
            };
            
            // Цвет модуля
            let module_color = "\x1b[90;1m";  // Серый жирный
            
            // Сброс стилей
            let reset = "\x1b[0m";
            
            // Форматируем сообщение
            writeln!(
                buf,
                "{}{} {}{} {}{} {}[{}]{} {}{}{}",
                level_color,
                level_emoji,
                chrono::Local::now().format("%H:%M:%S%.3f"),
                reset,
                level_color,
                record.level(),
                reset,
                module_color,
                record.module_path().unwrap_or("unknown"),
                reset,
                level_color,
                record.args(),
            )
        })
        .filter_module("tokio", log::LevelFilter::Warn)  // Уменьшаем шум от tokio
        .filter_module("hyper", log::LevelFilter::Warn)  // Уменьшаем шум от hyper
        .init();

  
    
    info!(" [MAIN] Подключаемся к блокчейну");

    //создали менеджера провайдеров
    let provider_manager = ProviderManager::new().await;


    // Получение WS провайдера
    let provider_ws = match provider_manager.get_ws_provider().await {
        Some(p) => p,
        None => {
            error!(" [MAIN] Не удалось получить рабочий WebSocket провайдер");
            return Err("WebSocket провайдер не доступен".into());
        }
    };

    // Получение HTTP провайдера
    let provider_http = match provider_manager.get_http_provider().await {
        Some(p) => p,
        None => {
            error!(" [MAIN] Не удалось получить рабочий HTTP провайдер");
            return Err("HTTP провайдер не доступен".into());
        }
    };


    let start_block = get_env_var("START_BLOCK").parse::<u64>()?;  
  
    
    
    // ⛓ Инициализация токен-кэша
    pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;
    
    let token_cache: TokenCache = Arc::new(match load_token_cache().await {
    Some(cache) => {
        info!("[КЭШ] [MAIN] Token кэш успешно загружен");
        DashMap::from_iter(cache.into_iter())
    }
    None => {
        info!("[КЭШ] [MAIN] Token кэш не найден или поврежден, создаём новый");
        DashMap::new()
    }
});



    
    
    //  ✅ Загрузка token_list.json для фильтрации топовых токенов
    let token_whitelist_set: Arc<DashSet<Address>> = Arc::new(
    token::load_token_list_from_json("token_list.json")
        .keys()
        .cloned()
        .collect()
);


    
    info!("[MAIN] Загружено {} токенов из token_list.json", token_whitelist_set.len());    

    let pool_cache: Arc<Mutex<UniswapPoolCache>> = Arc::new(Mutex::new(
        match UniswapPoolCache::load_from_bin("uniswap_pool_addresses_cache.bin") {
            Ok(cache) => {
                info!("[КЭШ][MAIN] Кэш пулов успешно загружен с диска");
                cache
            }
            Err(_) => {
                info!("[КЭШ][MAIN] Кэш пулов не найден, создаём новый");
                UniswapPoolCache::new()
            }
        }
    ));
              
    //  Создаем UniversalGraph
    let graph: Arc<UniversalGraph> = Arc::new(UniversalGraph::new());
    
    info!("⏳[MAIN]  Синхронизация пулов начата...");
    let start = std::time::Instant::now();

    // Клонируем Arc перед передачей в sync_pools
    let graph_for_sync: Arc<UniversalGraph> = Arc::clone(&graph);
    
    
    
    info!("⏳[MAIN]  Создание подписчика на блоки...");


    // Инициализация подписчика
    let subscriber = Arc::new(UniswapEventSubscriber::new(provider_http.clone()));

    //запускаем синхронизацию UNISWAP
    uniswap_v3::sync_pools(graph_for_sync, Arc::clone(&provider_ws), &Arc::clone(&token_cache), Arc::clone(&pool_cache), &token_whitelist_set, start_block, subscriber ).await?;

    
    
    let pool_cache_guard = pool_cache.lock().await;

    if let Err(e) = pool_cache_guard.save_to_bin("uniswap_pool_addresses_cache.bin") {
        error!("[MAIN_КЭШ] Ошибка при сохранении кэша пулов: {:?}", e);
    } else {
        info!("[КЭШ] Кэш пулов успешно сохранён");
    }

    if let Err(e) = pool_cache_guard.save_to_json("debug_uniswap_cache.json") {
        error!("Ошибка при сохранении кеша Uniswap в JSON: {:?}", e);
    } else {
        info!("Кеш Uniswap успешно сохранён в debug_uniswap_cache.json");
    }  
      
    let duration = start.elapsed();
    let secs = duration.as_secs();
    let minutes = secs / 60;
    let seconds = secs % 60;

    info!("[MAIN]✅ Синхронизация пулов завершена за {} минут {} секунд", minutes, seconds);
    info!("[MAIN] Бот завершил сканирование пулов");

    Ok(())

}

pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name).unwrap_or_else(|_| panic!("[MAIN]Environment variable {} not found", var_name))
}
