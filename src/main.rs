pub mod uniswap_v3;
pub mod uniswap_graph;
pub mod token;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod provider;

use provider::get_working_provider;
use uniswap_cache::UniswapPoolCache;
use uniswap_events::UniswapEventSubscriber;
use uniswap_graph::UniversalGraph;

use crate::token::TokenInfo;
use token::load_token_cache;

use dotenv::dotenv;
use env_logger::Env;
use log::{error, info};
use ethers::{providers::{Provider, Ws}, types::Address};
use std::{collections::{HashMap, HashSet}, env, sync::Arc};
use tokio::{signal, sync::Mutex};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    info!("Подключаемся к блокчену");

    // Вызов асинхронной функции для создания провайдеров
    let provider: Arc<Provider<Ws>> = get_working_provider().await;

    info!("Подключились через алхимиеские врата");

    // ⛓ Инициализация токен-кэша
    type TokenCache = Arc<Mutex<HashMap<Address, TokenInfo>>>;

    let token_cache: TokenCache = Arc::new(Mutex::new(
        match load_token_cache().await {
            Some(cache) => {
                info!("[КЭШ] Token кэш успешно загружен");
                cache
            }
            None => {
                info!("[КЭШ] Token кэш не найден или поврежден, создаём новый");
                HashMap::new()
            }
        }
    ));

    // ✅ Загрузка token_list.json для фильтрации топовых токенов
    let token_whitelist_set: HashSet<Address> = token::load_token_list_from_json("token_list.json").keys().cloned().collect();
    info!("[ТОП] Загружено {} токенов из token_list.json", token_whitelist_set.len());

    // Создаем UniversalGraph
    let graph = Arc::new(Mutex::new(UniversalGraph::new()));

    let pool_cache: Arc<Mutex<UniswapPoolCache>> = Arc::new(Mutex::new(
        match UniswapPoolCache::load_from_file() {
            Ok(cache) => {
                info!("[КЭШ] Кэш пулов успешно загружен с диска");
                cache
            }
            Err(_) => {
                info!("[КЭШ] Кэш пулов не найден, создаём новый");
                UniswapPoolCache::new()
            }
        }
    ));

    let start = std::time::Instant::now();
    info!("⏳ Синхронизация пулов начата...");

    // Клонируем Arc перед передачей в sync_pools
    let graph_for_sync = Arc::clone(&graph);

    let start_block = get_env_var("START_BLOCK").parse::<u64>()?;

    uniswap_v3::sync_pools(graph_for_sync, Arc::clone(&provider), &Arc::clone(&token_cache), Arc::clone(&pool_cache), &token_whitelist_set, start_block).await?;

    let pool_cache = pool_cache.lock().await;
     
    if let Err(e) = pool_cache.save_to_file() {
        error!("[КЭШ] Ошибка при сохранении кэша пулов: {:?}", e);
    } else {
        info!("[КЭШ] Кэш пулов успешно сохранён");
    }

    if let Err(e) = pool_cache.save_to_json_debug("debug_uniswap_cache.json") {
        error!("Ошибка при сохранении кеша Uniswap в JSON: {:?}", e);
    } else {
        info!("Кеш Uniswap успешно сохранён в debug_uniswap_cache.json");
    }

    // 1. Создаем подписчика
    let subscriber = Arc::new(UniswapEventSubscriber::new(
        Arc::clone(&provider),
        Arc::clone(&graph),
    ));

    // 1. Спавним подписку
    let subscriber_clone = Arc::clone(&subscriber);

    let handle = tokio::spawn(async move {
        if let Err(e) = subscriber_clone.subscribe_to_events_for_all_pools().await {
            error!("Ошибка при подписке на события: {:?}", e);
        }
    });

    // 2. Ожидаем сигнал на завершение (Ctrl+C)
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("🚪 Получен сигнал завершения...");
        },
        _ = handle => {
            info!("Подписка завершена");
        },
    }
    
    let duration = start.elapsed();
    let secs = duration.as_secs();
    let minutes = secs / 60;
    let seconds = secs % 60;

    info!("✅ Синхронизация пулов завершена за {} минут {} секунд", minutes, seconds);
    info!("Бот завершил сканирование пулов");

    Ok(())
}


pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name).unwrap_or_else(|_| panic!("Environment variable {} not found", var_name))
}
