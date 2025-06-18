pub mod aave_v3_flash_monitor;
pub mod path_builder;
pub mod provider;
pub mod take_gas_price;
pub mod token;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod uniswap_graph;
pub mod uniswap_v3;
pub mod arb_scanner;
pub mod token_white_list;
pub mod arb_simulator; 
pub mod tick_fetcher;

use aave_v3_flash_monitor::AaveTokenLiquidity;
use aave_v3_flash_monitor::get_aave_data;
use log::Record;
use log::warn;
use path_builder::PathBuilder;
use provider::ProviderManager;
use token::load_token_list_from_json;
use tokio::signal;
use tokio::spawn;
use tokio::sync::mpsc;


use crate::token::TokenInfo;
use crate::uniswap_events::PoolEventInfo;
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

use arb_simulator::ArbitrageSimulator;
use uniswap_cache::UniswapPoolCache;
use uniswap_events::UniswapEventSubscriber;
use uniswap_graph::UniversalGraph;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    //==========================================  подключаем .ENV  и ЛОГ ==========================================================================================================
    dotenv().ok();

   // let start_block = get_env_var("START_BLOCK").parse::<u64>()?;

   //============================================== ПОДКЛЮЧАЕМ ЛОГГЕР =========================================================================================================
   
   // Инициализация логгера с кастомным форматом и записью в файл
   let log_file = Arc::new(Mutex::new(
       OpenOptions::new()
       .create(true)
       .append(true)
       .open("log.md")?,
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
            log::Level::Debug => "🐶", // Жук для дебага
            log::Level::Trace => "🔬", // Лупа для ответа
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
            record.args()
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

//============================================== ПОДКЛЮЧАЕМ СЕРДЦА =========================================================================================================
   
    let _heartbeat = tokio::spawn(async move {
        loop {
            info!("Heartbeat - still running");
            tokio::time::sleep(Duration::from_secs(200)).await;
        }
    });

    
//==========================================  ПОДКЛЮЧАЕМ ПРОВАЙДЕРОВ  ================================================================================================================

    info!(" [ MAIN ] Подключаемся к блокчейну");
    //создали менеджера провайдеров
    let provider_manager = ProviderManager::new(200).await; //лимит по запросам в new

    // Получение WS провайдера
    let provider_ws = provider_manager.get_ws().await;

    // Получение HTTP провайдера
    let provider_http = provider_manager.get_http().await;

    // Клонируем HTTP  провайдеров
    let provider_for_aave = provider_http.clone();

    // Клонируем WS  провайдеров
    let provider_ws_for_sync = provider_ws.clone();

    //Провайдер для деспетчера
    let dispatcher_provider = provider_ws.clone();
    //let provider_gas = provider_http.clone();

    //==========================================  КЭШ ТОКЕНОВ  ==========================================================================================================

    // ⛓ Инициализация токен-кэша
    pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;

    let token_cache: TokenCache = {
        let raw_map = load_token_list_from_json();
        if raw_map.is_empty() {
            info!("[ MAIN ] Token кэш не найден или пуст, создаём новый");
            Arc::new(DashMap::new())
        } else {
            info!("[ MAIN ] Token кэш успешно загружен");
            Arc::new(DashMap::from_iter(raw_map.into_iter()))
        }
    };

    //======================================= ЗАГРУЖАЕМ БЕЛЫЙ СПИСОК ТОКЕНОВ ================================================================================================

    //  ✅ Загрузка белого списка токенов
    let token_whitelist_set: Arc<DashSet<Address>> = Arc::new(token_white_list::load_token_whitelist());
    info!(
        "[ MAIN ] Загружено {} токенов из белого списка",
        token_whitelist_set.len()
    );

//===========================================  ЗАГРУЖАЕМ КЭШ АДРЕСОВ ПУЛОВ ===============================================================================================

    let pool_cache: Arc<UniswapPoolCache> = Arc::new(
        
        match UniswapPoolCache::load_from_bin("uniswap_pool_addresses_cache.bin") {
            Ok(cache) => {
                info!("[ MAIN ] Кэш пулов успешно загружен с диска");
                cache
            }
            Err(_) => {
                info!("[ MAIN ] Кэш пулов не найден, создаём новый");
                UniswapPoolCache::new()
            }
        },
    );

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

//==========================================  ПОДКЛЮЧАЕМ ГРАФ  И ЕГО КЛОНЫ  =========================================================================================================================

    //  Создаем UniversalGraph
    let graph: Arc<UniversalGraph> = Arc::new(UniversalGraph::new());

    // Клонируем для потоков и вызовов
    let graph_for_sync = Arc::clone(&graph);
    let graph_for_paths = Arc::clone(&graph);
    let graph_for_shutdown = Arc::clone(&graph);
    //let dispatcher_graph = Arc::clone(&graph);
    let pulling_graph = Arc::clone(&graph);

//=======================================  Обработки Ctrl+C =============================================================================================

    spawn(async move {
        signal::ctrl_c()
            .await
            .expect("[ MAIN ] Ошибка в обработке сигнала Ctrl+C");
        if let Err(e) = graph_for_shutdown.save_graph_to_json("uniswap_graph_snapshot.json") {
            error!("[ MAIN ] Ошибка при сохранении графа в JSON: {:?}", e);
        } else {
            info!("[ MAIN ] Граф успешно сохранен в uniswap_graph_snapshot.json");
        }
        std::process::exit(0);
    });

//==========================================  ИНИЦИАЛИЗАЦИЯ AAVE FLASH MONITOR  ====================================================================================================

// Создаём канал с пустой структурой
let (aave_tx, aave_rx) = watch::channel(AaveTokenLiquidity::default());

// Запускаем мониторинг, передавая Sender
spawn({
    async move {
        if let Err(e) = get_aave_data(provider_for_aave, aave_tx).await {
            eprintln!("[ MAIN ] Error in Aave liquidity monitor: {:?}", e);
        }
    }
});


// Не ждём здесь первого обновления, просто продолжаем работу
info!("[ MAIN ] Мониторинг ликвидности Aave запущен в фоне");

//==========================================  ПОДКЛЮЧАЕМ МОДУЛЬ ПУЛИНГА ===============================================================================================================

    // Создаем канал для передачи новых блоков
    let (block_sender, block_receiver) = watch::channel(0);

   let (event_tx, event_rx) = mpsc::channel::<PoolEventInfo>(2048);

    // Запускаем подписку на новые блоки в отдельной задаче
    spawn(async move {
        if let Err(e) =
            UniswapEventSubscriber::subscribe_to_new_blocks(&provider_ws, block_sender).await
        {
            error!("[ MAIN ] Ошибка в подписке на блоки: {:?}", e);
        }
    });

    info!("⏳[ MAIN ] Создание модуля пулинга");

    let subscriber: Arc<UniswapEventSubscriber> = Arc::new(UniswapEventSubscriber::new(provider_http.clone()));
    let subscriber_clone = Arc::clone(&subscriber);
    let block_receiver_clone_to_subscriber = block_receiver.clone();

    // Запускаем polling_event
    spawn({
        let subscriber = Arc::clone(&subscriber_clone);
        let graph = Arc::clone(&pulling_graph);
        async move {
            if let Err(e) = subscriber
                .polling_event(
                    &block_receiver_clone_to_subscriber,
                     graph,
                      event_tx)
                .await
            {
                error!(
                    "💥 [ MAIN ] Задача polling_event завершилась с ошибкой: {:?}",
                    e
                );
            } else {
                warn!("⚠️ [ MAIN ] Задача polling_event завершилась. Это не штатное поведение.");
            }
        }
    });

// =============================================================🔧  Запуск ДИСПЕТЧЕРА воркеров  ===========================================================================
    
    let (simulator_tx, simulator_rx) = mpsc::channel::<PoolEventInfo>(2048);

    const NUM_WORKERS: usize = 20;
    info!("⏳[ MAIN ] Запуск координатора с {} воркерами", NUM_WORKERS);

    spawn({
        let subscriber = Arc::clone(&subscriber);
        let graph = Arc::clone(&pulling_graph);
        let provider = Arc::clone(&dispatcher_provider);
        async move {
            subscriber
                .start_coordinator_and_workers(graph, provider, NUM_WORKERS, event_rx, simulator_tx)
                .await;
        }
    });

//==============================================  ЗАПУСТИЛИ СКАНИРОВАНИЕ  =====================================================================================================

    info!("⏳[ MAIN ]  Синхронизация пулов начата...");
    
     // Синхронизация пулов
    match uniswap_v3::sync_pools(
        graph_for_sync.clone(),
        provider_ws_for_sync.clone(),
        &token_cache,
        pool_cache.clone(),
        &token_whitelist_set,
        subscriber.clone(),
    )
    .await
    {
        Ok(_) => {
            info!("✅ [MAIN_ЦИКЛ ] Синхронизация завершена ");
        }
        Err(_e) => {
            error!("❌ [ MAIN_ЦИКЛ ] Ошибка синхронизации");
        }
    }

    info!("[ MAIN ] Бот завершил сканирование пулов");

//==========================================  ОБНОВИМ КЕШ АДРЕСОВ ПУЛОВ ============================================================================================================================

 {
    // Клонируем приемник блоков для использования в кеше
    let block_receiver_clone_to_cache = block_receiver.clone();

    // Получаем текущий номер последнего блока
    let last_block = *block_receiver_clone_to_cache.borrow();

    // Получаем все адреса пулов из графа в виде Vec с дедупликацией
    let pool_addresses_from_graph: Vec<Address> = {
        let mut addresses: Vec<Address> = graph_for_sync.get_pool_addresses().into_iter().collect();
        addresses.sort_unstable();
        addresses.dedup();
        addresses
    };

    // Создаем новый экземпляр кэша с обновленными данными
    let updated_cache = UniswapPoolCache {
        pool_addresses: pool_addresses_from_graph,
        last_verified_block: last_block,
    };

    // Сохраняем обновленный кэш в бинарный файл с новым именем
    if let Err(e) = updated_cache.save_to_bin("uniswap_pool_addresses_cache_update.bin") {
        error!("[ MAIN_ЦИКЛ ] Ошибка сохранения обновленного кэша в uniswap_pool_addresses_cache_update.bin: {:?}", e);
    } else {
        info!("[ MAIN_ЦИКЛ ] Обновленный кэш успешно сохранен в uniswap_pool_addresses_cache_update.bin");
    }

    // Сохраняем кэш в основной бинарный файл
    if let Err(e) = updated_cache.save_to_bin("uniswap_pool_addresses_cache.bin") {
        error!("[ MAIN_ЦИКЛ ] Ошибка сохранения кэша: {:?}", e);
    }

    // Сохраняем кэш в JSON формате для отладки
    if let Err(e) = updated_cache.save_to_json("debug_uniswap_cache.json") {
        error!("[ MAIN_ЦИКЛ ] Ошибка сохранения JSON: {:?}", e);
    }
}

    //==============================  ВКЛЮЧАЕМ СВЕТ - НАХОДИМ ПУТЬ =======================================================================================================================

    // Построение арбитражных путей
    info!("[MAIN_PATH_BUILDER] Начинаем построение арбитражных путей");

    let path_build_start = std::time::Instant::now();
    let mut path_builder = PathBuilder::new(aave_rx.clone());
    path_builder.build_all_paths(graph_for_paths);
    let path_builder = Arc::new(path_builder);
    let path_builder_clone = Arc::clone(&path_builder);
    let path_build_duration = path_build_start.elapsed();

    info!(
        "✅ [MAIN_PATH_BUILDER] Построение путей завершено за {:?} секунд, найдено {:?} путей",
        path_build_duration.as_secs_f64(),
        path_builder.paths.len()
    );

    //==========================================  АРБИТРАЖИРУЕМ САБАТАЖ  ===============================================================================================================

    // Запускаем симулятор после построения путей
    let mut arb_simulator = ArbitrageSimulator::new(
        path_builder_clone,
        aave_rx.clone(),
        graph.clone(),
        simulator_rx,
    );
    tokio::spawn(async move {
        info!("[ARB_SIMULATOR] Симулятор арбитража запущен после построения путей");
        arb_simulator.run().await;
    });

    Ok(())
}

pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name)
        .unwrap_or_else(|_| panic!("[ MAIN ]Environment variable {} not found", var_name))
}