pub mod uniswap_v3;
pub mod uniswap_graph;
pub mod token;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod provider;
/*
use console_subscriber::ConsoleLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
 */
use dashmap::{DashMap, DashSet};
use log::warn;
use provider::ProviderManager;

use tokio::sync::watch;
use tokio::time::sleep;
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
use std::time::Duration;
use std::{env, sync::Arc};
use std::io::Write;

use tokio::sync::Mutex;



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {


    
    

    //==========================================  подключаем .ENV  и ЛОГ ============================================
    dotenv().ok();
    let start_block = get_env_var("START_BLOCK").parse::<u64>()?;  
    
   
    //подключаем логирование
    Builder::from_env(Env::default().default_filter_or("info"))
    .format(|buf, record| {
        // Определяем цвета и стили
        let level_color = match record.level() {
            log::Level::Error => "\x1b[31;1m",  // Красный жирный
            log::Level::Warn => "\x1b[95;1m",   // Пурпурный жирный
            log::Level::Info => "\x1b[36;1m",    // Зеленый жирный
            log::Level::Debug => "\x1b[36;1m",   // Голубой жирный
            log::Level::Trace => "\x1b[35;1m",   // Пурпурный жирный
        };
        
        // Эмодзи для уровней
        let level_emoji = match record.level() {
            log::Level::Error => "",  // Огонь для ошибок
            log::Level::Warn => "⚠️",   // Предупреждение
            log::Level::Info => "🔥",   // Информация
            log::Level::Debug => "🐞",  // Жук для дебага
            log::Level::Trace => "🔬",  // Лупа для трассировки
        };
        
        // Цвет модуля
        let module_color = "\x1b[31;1m";  // Серый жирный
        
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
 
 
//==========================================  ПОДКЛЮЧАЕМ ПРОВАЙДЕРОВ  ============================================

info!(" [MAIN] Подключаемся к блокчейну");


    //создали менеджера провайдеров
    let provider_manager = ProviderManager::new(499).await;  //лимит по запросам в new

    // Получение WS провайдера
    let provider_ws =  provider_manager.get_ws().await;

    // Получение HTTP провайдера
    let provider_http =  provider_manager.get_http().await;


    let  provider_ws_clone = provider_ws.clone() ;


    let provider_ws_for_sync = provider_ws.clone() ;
  //==========================================  КЭШ ТОКЕНОВ И БЕЛЫЙ СПИСОК  ============================================
           
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
        
//==========================================  ПОЖКЛЮЧАЕМ ГРАФ  ==============================================================

    //  Создаем UniversalGraph
    let graph: Arc<UniversalGraph> = Arc::new(UniversalGraph::new());

    info!("⏳[MAIN]  Синхронизация пулов начата...");
    //let start = std::time::Instant::now();

    // Клонируем Arc перед передачей в sync_pools
    let graph_for_sync: Arc<UniversalGraph> = Arc::clone(&graph);




//==========================================  ПОДКЛЮЧАЕМ МОДУЛЬ ПОДПИСКИ  ==============================================================

/*
*/
 
    // Создаем канал для передачи новых блоков
    let (block_sender, block_receiver) = watch::channel(0);

    // Запускаем подписку на новые блоки в отдельной задаче
    tokio::spawn(async move {
        if let Err(e) = UniswapEventSubscriber::subscribe_to_new_blocks( &provider_ws, block_sender).await {
            error!("Ошибка в подписке на блоки: {:?}", e);
        }
    });

    sleep(std::time::Duration::from_secs(1)).await;

    info!("⏳[MAIN]  Создание подписчика на блоки...");

    let subscriber: Arc<UniswapEventSubscriber> = Arc::new(UniswapEventSubscriber::new(provider_http.clone()));

    let subscriber_clone = Arc::clone(&subscriber);

    // Запускаем polling_event как вечную фоновую задачу
    tokio::spawn(async move {
        if let Err(e) = subscriber_clone
            .polling_event(graph.clone(), provider_ws_clone, block_receiver)
            .await
        {
            error!("💥 [MAIN] Задача polling_event завершилась с ошибкой: {:?}", e);
        } else {
            warn!("⚠️ [MAIN] Задача polling_event завершилась. Это не штатное поведение.");
        }
    });


//==========================================  ЗАПУСТИЛИ СКАНИРОВАНИЕ  ==============================================================

 // Основной цикл для периодической синхронизации пулов
     // Настройки синхронизации
    let mut sync_counter: u64 = 0;
    let sync_interval = Duration::from_secs(1800); // 30 минут

    // Основной цикл
    loop {
        sync_counter += 1;
        let cycle_start = std::time::Instant::now();
        
        info!("🔄 [ЦИКЛ {}] Начало синхронизации пулов", sync_counter);

        // Синхронизация пулов
        match uniswap_v3::sync_pools(graph_for_sync.clone(),provider_ws_for_sync.clone(),&token_cache,pool_cache.clone(),&token_whitelist_set,start_block,subscriber.clone()).await {
            Ok(_) => {
                let duration = cycle_start.elapsed();
                info!("✅ [ЦИКЛ {}] Синхронизация завершена за {:?}", sync_counter, duration);
            }
            Err(e) => {
                error!("❌ [ЦИКЛ {}] Ошибка синхронизации: {:?}", sync_counter, e);
            }
        }
//==========================================  ОБНОВИМ КЕШ  ==============================================================
        {
            let cache = pool_cache.lock().await;
            if let Err(e) = cache.save_to_bin("uniswap_pool_addresses_cache.bin") {
                error!("[ЦИКЛ {}] Ошибка сохранения кэша: {:?}", sync_counter, e);
            }
            if let Err(e) = cache.save_to_json("debug_uniswap_cache.json") {
                error!("[ЦИКЛ {}] Ошибка сохранения JSON: {:?}", sync_counter, e);
            }
        }

        // Ожидание следующего цикла
        info!("⏳ [ЦИКЛ {}] Ожидание следующей синхронизации через {} минут...", 
            sync_counter, 
            sync_interval.as_secs() / 60);
        
        sleep(sync_interval).await;
        info!("[MAIN] Бот завершил сканирование пулов");
    }

}


pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name).unwrap_or_else(|_| panic!("[MAIN]Environment variable {} not found", var_name))
}
/*



//========================================= ЗАВЕРШЕНИЯ ===============================================================================
      
*/