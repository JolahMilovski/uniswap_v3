pub mod aave_v3_flash_monitor;
pub mod path_builder;
pub mod provider;
pub mod take_gas_price;
pub mod token;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod uniswap_graph;
pub mod uniswap_v3;
//pub mod arb_scanner;


use aave_v3_flash_monitor::get_aave_data;
use aave_v3_flash_monitor::AaveTokenLiquidity;
use log::warn;
use log::Record;
use path_builder::PathBuilder;
use provider::ProviderManager;
use token::load_token_list_from_json;
use tokio::signal;
use tokio::spawn;

use crate::token::TokenInfo;
use dashmap::{DashMap, DashSet};
use dotenv::dotenv;
use env_logger::Builder;
use env_logger::Env;
use ethers::types::Address;
use log::error;
use log::info;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::watch;
use tokio::time::sleep;

use uniswap_cache::UniswapPoolCache;
use uniswap_events::UniswapEventSubscriber;
use uniswap_graph::UniversalGraph;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //==========================================  подключаем .ENV  и ЛОГ ==========================================================================================================
    dotenv().ok();

    let start_block = get_env_var("START_BLOCK").parse::<u64>()?;

    // Инициализация логгера с кастомным форматом и записью в файл
    let log_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open("log.md")?
    ));

    // Клонируем Arc для использования в замыкании
    let log_file_for_format = Arc::clone(&log_file);

    Builder::from_env(Env::default().default_filter_or("info"))
        .format(move |buf, record: &Record| {
            // Определяем цвета и стили
            let level_color = match record.level() {
                log::Level::Error => "\x1b[31;1m", // Красный жирный
                log::Level::Warn => "\x1b[95;1m",  // Пурпурный жирный
                log::Level::Info => "\x1b[36;1m",  // Зеленый жирный
                log::Level::Debug => "\x1b[36;1m", // Голубой жирный
                log::Level::Trace => "\x1b[35;1m", // Пурпурный жирный
            };

            // Эмодзи для уровней
            let level_emoji = match record.level() {
                log::Level::Error => "",   // Огонь для ошибок
                log::Level::Warn => "⚠️",  // Предупреждение
                log::Level::Info => "🔥",  // Информация
                log::Level::Debug => "🐞", // Жук для дебага
                log::Level::Trace => "🔬", // Лупа для трассировки
            };

            // Цвет модуля
            let module_color = "\x1b[31;1m"; // Серый жирный



            // Форматируем сообщение для консоли
            let message = format!(
                "{} {} {} {}[{}]{} {}{}{}",
                level_color,
                level_emoji,
                chrono::Local::now().format("%H:%M:%S%.3f"),
                level_color,
                record.level(),
                module_color,
                record.module_path().unwrap_or("unknown"),
                level_color,
                record.args(),
            );

       
            // Записываем в консоль
            writeln!(buf, "{}", message)?;

            // Асинхронная запись в файл
            let log_file = Arc::clone(&log_file_for_format);
            tokio::spawn(async move {
                let mut file = log_file.lock().await;
                if let Err(e) = file.write_all((message + "\n").as_bytes()) {
                    eprintln!("Ошибка записи в log.md: {:?}", e);
                }
                if let Err(e) = file.flush() {
                    eprintln!("Ошибка сброса буфера в log.md: {:?}", e);
                }
            });

            Ok(())
        })
        .filter_module("tokio", log::LevelFilter::Warn) // Уменьшаем шум от tokio
        .filter_module("hyper", log::LevelFilter::Warn) // Уменьшаем шум от hyper
        .init();

    //==========================================  ПОДКЛЮЧАЕМ ПРОВАЙДЕРОВ  ================================================================================================================

    info!(" [MAIN] Подключаемся к блокчейну");
    //создали менеджера провайдеров
    let provider_manager = ProviderManager::new(499).await; //лимит по запросам в new

    // Получение WS провайдера
    let provider_ws = provider_manager.get_ws().await;

    // Получение HTTP провайдера
    let provider_http = provider_manager.get_http().await;

    // Клонируем HTTP  провайдеров
    let provider_for_aave = provider_http.clone();

    // Клонируем WS  провайдеров
    let provider_ws_clone = provider_ws.clone();
    let provider_ws_for_sync = provider_ws.clone();

    //let provider_gas = provider_http.clone();

   

    //==========================================  КЭШ ТОКЕНОВ  ==========================================================================================================


    // ⛓ Инициализация токен-кэша
    pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;

    let token_cache: TokenCache = {
        let raw_map = load_token_list_from_json();
        if raw_map.is_empty() {
            info!("[КЭШ] [MAIN] Token кэш не найден или пуст, создаём новый");
            Arc::new(DashMap::new())
        } else {
            info!("[КЭШ] [MAIN] Token кэш успешно загружен");
            Arc::new(DashMap::from_iter(raw_map.into_iter()))
        }
    };



//======================================= ЗАГРУЖАЕМ БЕЛЫЙ СПИСОК ТОКЕНОВ ================================================================================================


    //  ✅ Загрузка token_list.json для фильтрации топовых токенов
    let token_whitelist_set: Arc<DashSet<Address>> = Arc::new(
    token::load_token_whitelist("token_white_list.json")
        .into_iter()
        .collect(),
);
    info!(
        "[MAIN] Загружено {} токенов из token_list.json",
        token_whitelist_set.len()
    );


//===========================================  ЗАГРУЖАЕМ КЭШ АДРЕСОВ ПУЛОВ ===============================================================================================



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
        },
    ));

    //====================================  ПОДКЛЮЧАЕМСЯ К ГАЗОВОЙ ТРУБЕ ===============================================================================================================================
/*
// Создаем канал для газа
let (_gas_feed, gas_sender) = take_gas_price::GasPriceFeed::new();

// Запускаем таск по обновлению цены газа
tokio::spawn({
    async move {
        take_gas_price::start_gas_price_loop(provider_gas, gas_sender).await;
    }
});

*/
    //==========================================  ПОЖКЛЮЧАЕМ ГРАФ  И ЕГО КЛОНЫ  =========================================================================================================================

    //  Создаем UniversalGraph
    let graph: Arc<UniversalGraph> = Arc::new(UniversalGraph::new());

    // Клонируем для потоков и вызовов
    let graph_for_event = Arc::clone(&graph);
    let graph_for_sync = Arc::clone(&graph);
    let graph_for_paths = Arc::clone(&graph);
    let graph_for_shutdown = Arc::clone(&graph);
    //let graph_for_aave = Arc::clone(&graph);

    //=======================================  Обработки Ctrl+C =============================================================================================

    spawn
    (
        
        async move {

        signal::ctrl_c().await.expect("[MAIN] Ошибка в обработке сигнала Ctrl+C");
        if let Err(e) = graph_for_shutdown.save_graph_to_json("uniswap_graph_snapshot.json") {
            error!("[MAIN] Ошибка при сохранении графа в JSON: {:?}", e);
        } else {
            info!("[MAIN] Граф успешно сохранен в uniswap_graph_snapshot.json");
        }
          sleep(Duration::from_secs(5)).await;
        std::process::exit(0);
    }); 


 //==========================================  ИНИЦИАЛИЗАЦИЯ AAVE FLASH MONITOR  ====================================================================================================
 

    // Создаём канал с пустой структурой
    let (aave_tx, aave_rx) = watch::channel(AaveTokenLiquidity::default());

    // Запускаем мониторинг, передавая Sender
    tokio::spawn
    (
        { 
            async move {
            if let Err(e) = get_aave_data(provider_for_aave, aave_tx).await {
                
                eprintln!("[MAIN] Error in Aave liquidity monitor: {:?}", e);
            }
        }
    });
    
    //==========================================  ПОДКЛЮЧАЕМ МОДУЛЬ ПУЛИНГА ===============================================================================================================
    
    // Создаем канал для передачи новых блоков
    let (block_sender, block_receiver) = watch::channel(0);
    
    // Запускаем подписку на новые блоки в отдельной задаче
    spawn
        (   
            async move {
                if let Err(e) =
                UniswapEventSubscriber::subscribe_to_new_blocks(&provider_ws, block_sender).await
                {
                    error!("[MAIN] Ошибка в подписке на блоки: {:?}", e);
                }
        });
    
    
    info!("⏳[MAIN] Создание модуля пулинга");
    
    let subscriber: Arc<UniswapEventSubscriber> = Arc::new(UniswapEventSubscriber::new(provider_http.clone()));
    
    let subscriber_clone = Arc::clone(&subscriber);
    
    let block_receiver_clone_to_subscriber = block_receiver.clone();
    // Запускаем polling_event как вечную фоновую задачу
    spawn
    (
        async move {

        if let Err(e) = subscriber_clone

        .polling_event( graph_for_event, provider_ws_clone, &block_receiver_clone_to_subscriber)
        .await
        {
            error!(
                "💥 [MAIN] Задача polling_event завершилась с ошибкой: {:?}",
                e
            );
        } else {
            warn!("⚠️ [MAIN] Задача polling_event завершилась. Это не штатное поведение.");
        }
    });
    
    //==========================================  ЗАПУСТИЛИ СКАНИРОВАНИЕ  ============================================================================================================================
    
    info!("⏳[MAIN]  Синхронизация пулов начата...");

    // Основной цикл для периодической синхронизации пулов
    // Настройки синхронизации
    let mut sync_counter: u64 = 0;
    //let sync_interval = Duration::from_secs(1800); // 30 минут
    // Основной цикл

        sync_counter += 1;
        let cycle_start = std::time::Instant::now();
        
        info!("🔄 [MAIN_ЦИКЛ {}] Начало синхронизации пулов", sync_counter);
        
        // Синхронизация пулов
        match uniswap_v3::sync_pools(
            graph_for_sync.clone(),
            provider_ws_for_sync.clone(),
            &token_cache,
            pool_cache.clone(),
            &token_whitelist_set,
            start_block,
            subscriber.clone(),
        )
        .await
        {
            Ok(_) => {
                let duration = cycle_start.elapsed();
                info!(
                    "✅ [MAIN_ЦИКЛ {}] Синхронизация завершена за {:?}",
                    sync_counter, duration
                );
            }
            Err(e) => {
                error!("❌ [MAIN_ЦИКЛ {}] Ошибка синхронизации: {:?}", sync_counter, e);
            }
        }
        
        info!("[MAIN] Бот завершил сканирование пулов");
               
               
        //==========================================  ОБНОВИМ КЕШ  ============================================================================================================================


        {
            let block_receiver_clone_to_cache = block_receiver.clone();

            let last_block = *block_receiver_clone_to_cache.borrow(); // Берём последний известный номер блока из канала
            
            let mut cache = pool_cache.lock().await;
            cache.last_verified_block = last_block;  // Обновляем номер блока в кеше
            
            if let Err(e) = cache.save_to_bin("uniswap_pool_addresses_cache.bin") {
                error!("[MAIN_ЦИКЛ {}] Ошибка сохранения кэша: {:?}", sync_counter, e);
            }
            if let Err(e) = cache.save_to_json("debug_uniswap_cache.json") {
                error!("[ЦИКЛ {}] Ошибка сохранения JSON: {:?}", sync_counter, e);
            }
        }

        
        //==============================  ВКЛЮЧАЕМ СВЕТ - НАХОДИМ ПУТЬ =======================================================================================================================
        
        
      // Построение арбитражных путей
    info!("[MAIN_PATH_BUILDER] Начинаем построение арбитражных путей");
    
    let path_build_start = std::time::Instant::now();
    let mut path_builder = PathBuilder::new(aave_rx.clone());
    path_builder.build_all_paths(&graph_for_paths);
    let path_build_duration = path_build_start.elapsed();
    
    info!(
        "✅ [MAIN_PATH_BUILDER] Построение путей завершено за {:?} секунд, найдено {:?} путей",
        path_build_duration.as_secs_f64(),
        path_builder.paths.len()
    );
    
    Ok(())       
    
    
        /* 
        // Ожидание следующего цикла
        info!(
            "⏳ [MAIN_ЦИКЛ {}] Ожидание следующей синхронизации через {} минут...",
            sync_counter,
            sync_interval.as_secs() / 60
        );
        
        sleep(sync_interval).await;
        
    }
   
    loop {
        sleep(Duration::from_secs(60)).await;
    }
    */
}

pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name)
        .unwrap_or_else(|_| panic!("[MAIN]Environment variable {} not found", var_name))
}

//========================================= ЗАВЕРШЕНИЯ =============================================================================================================================================
