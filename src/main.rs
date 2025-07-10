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
use path_builder::PathBuilder;
use provider::ProviderManager;
use token::load_token_list_from_json;
use tokio::signal;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::{error, info, warn, Level};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_appender::rolling;
use crate::token::TokenInfo;
use crate::uniswap_events::PoolEventInfo;
use dashmap::{DashMap, DashSet};
use dotenv::dotenv;
use ethers::types::Address;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use chrono;
use std::io::IsTerminal;

use arb_simulator::ArbitrageSimulator;
use uniswap_cache::UniswapPoolCache;
use uniswap_events::UniswapEventSubscriber;
use uniswap_graph::UniversalGraph;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Подключаем .env
    dotenv().ok();

    // Настройка логирования с tracing
    let file_appender = rolling::minutely("./logs", "uniswap_arb_scanner.log");
    let file_layer = fmt::layer()
        .json()
        .with_writer(file_appender)
        .with_ansi(false)
        .with_target(true)
        .with_line_number(true)
        .with_thread_names(true);

    let stdout_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_level(true)
        .event_format(CustomEventFormat::new(true));

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(
            "info,
            uniswap::arb_scanner=debug,
            uniswap::arb_simulator=debug,
            uniswap::aave_v3_flash_monitor=warn,            
            uniswap::path_builder=info,
            uniswap::provider=warn,
            uniswap::take_gas_price=warn,
            uniswap::token=warn,
            uniswap::token_white_list=warn,
            uniswap::uniswap_cache=warn,
            uniswap::tick_fetcher=debug,
            uniswap::uniswap_events=debug,
            uniswap::uniswap_graph=warn,
            uniswap::uniswap_v3=info,
            tokio=off,hyper=off,
            rustls=off,
            ethers_providers=off"
        ));

    tracing_subscriber::registry()
        .with(file_layer)
        .with(stdout_layer)
        .with(filter)
        .init();

    info!("[MAIN] Логгер инициализирован с tracing, время: {:?}", chrono::Utc::now());

    // Heartbeat
    let _heartbeat = tokio::spawn(async move {
        loop {
            info!("[MAIN] Heartbeat - still running");
            tokio::time::sleep(Duration::from_secs(200)).await;
        }
    });

    //============================================================================================== Подключаемся к блокчейну
    info!("[MAIN] Подключаемся к блокчейну");
    let provider_manager = ProviderManager::new(200).await;
    let provider_ws = provider_manager.get_ws().await;
    let provider_http = provider_manager.get_http().await;
    let provider_for_aave = provider_http.clone();
    let provider_ws_for_sync = provider_ws.clone();
    let dispatcher_provider = provider_ws.clone();
    let polling_provider = provider_ws.clone();

    //=============================================================================================== Инициализация TokenCache
    pub type TokenCache = Arc<DashMap<Address, TokenInfo>>;
    let token_cache: TokenCache = {
        let raw_map = load_token_list_from_json();
        if raw_map.is_empty() {
            info!("[MAIN] Token кэш не найден или пуст, создаём новый");
            Arc::new(DashMap::new())
        } else {
            info!("[MAIN] Token кэш успешно загружен");
            Arc::new(DashMap::from_iter(raw_map.into_iter()))
        }
    };

    //================================================================================================ Загрузка белого списка токенов
    let token_whitelist_set: Arc<DashSet<Address>> = Arc::new(token_white_list::load_token_whitelist());
    info!("[MAIN] Загружено {} токенов из белого списка", token_whitelist_set.len());

    // Загрузка кэша адресов пулов
    let pool_cache: Arc<UniswapPoolCache> = Arc::new(
        match UniswapPoolCache::load_from_bin("uniswap_pool_addresses_cache.bin") {
            Ok(cache) => {
                info!("[MAIN] Кэш пулов успешно загружен с диска");
                cache
            }
            Err(_) => {
                info!("[MAIN] Кэш пулов не найден, создаём новый");
                UniswapPoolCache::new()
            }
        },
    );

    //================================================================================================== Создаем UniversalGraph
    let graph: Arc<UniversalGraph> = Arc::new(UniversalGraph::new());
    let graph_for_sync = Arc::clone(&graph);
    let graph_for_paths = Arc::clone(&graph);
    let graph_for_shutdown = Arc::clone(&graph);
    let pulling_graph = Arc::clone(&graph);

    // Обработка Ctrl+C
    spawn(async move {
        signal::ctrl_c()
            .await
            .expect("[MAIN] Ошибка в обработке сигнала Ctrl+C");
        if let Err(e) = graph_for_shutdown.save_graph_to_json("uniswap_graph_snapshot.json") {
            error!("[MAIN] Ошибка при сохранении графа в JSON: {:?}", e);
        } else {
            info!("[MAIN] Граф успешно сохранен в uniswap_graph_snapshot.json");
        }
        std::process::exit(0);
    });

    //================================================================================================ Инициализация Aave Flash Monitor
    let (aave_tx, aave_rx) = watch::channel(AaveTokenLiquidity::default());
    info!("[MAIN] Мониторинг ликвидности Aave запущен в фоне");
    spawn({
        async move {
            if let Err(e) = get_aave_data(provider_for_aave, aave_tx).await {
                error!("[MAIN] Error in Aave liquidity monitor: {:?}", e);
            }
        }
    });

    //=============================================================================== Модуль пулинга =============================================================================================================
    let (block_sender, block_receiver) = watch::channel(0);
    let (event_tx, event_rx) = mpsc::channel::<PoolEventInfo>(2048);
    spawn(async move {
        if let Err(e) = UniswapEventSubscriber::subscribe_to_new_blocks(&provider_ws, block_sender).await {
            error!("[MAIN] Ошибка в подписке на блоки: {:?}", e);
        }
    });

    info!("[MAIN] Создание модуля пулинга");
    let subscriber: Arc<UniswapEventSubscriber> = Arc::new(UniswapEventSubscriber::new(provider_http.clone()));
    let subscriber_clone = Arc::clone(&subscriber);
    let block_receiver_clone_to_subscriber = block_receiver.clone();
    
    spawn({
        let subscriber = Arc::clone(&subscriber_clone);
        let graph = Arc::clone(&pulling_graph);
        async move {
            if let Err(e) = subscriber
                .polling_event(&block_receiver_clone_to_subscriber, graph, event_tx, polling_provider)
                .await
            {
                error!("[MAIN] Задача polling_event завершилась с ошибкой: {:?}", e);
            } else {
                warn!("[MAIN] Задача polling_event завершилась. Это не штатное поведение.");
            }
        }
    });

    //==================================================================================== Запуск диспетчера воркеров =======================================================
    let (simulator_tx, simulator_rx) = mpsc::channel::<PoolEventInfo>(2048);
    const NUM_WORKERS: usize = 20;
    info!("[MAIN] Запуск координатора с {} воркерами", NUM_WORKERS);
    spawn({
        let graph = Arc::clone(&pulling_graph);
        let provider = Arc::clone(&dispatcher_provider);
        let simulator_tx = simulator_tx.clone();
        let subscriber = Arc::clone(&subscriber);
        async move {
            UniswapEventSubscriber::start_coordinator_and_workers(
                graph,
                provider,
                NUM_WORKERS,
                event_rx,
                simulator_tx,
                subscriber,
            )
            .await;
        }
    });

//==================================================================================== Синхронизация пулов ====================================================================================  
    info!("[MAIN] Синхронизация пулов начата...");
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
            info!("[MAIN] Синхронизация завершена");
        }
        Err(_e) => {
            error!("[MAIN] Ошибка синхронизации");
        }
    }

    info!("[MAIN] Бот завершил сканирование пулов");

    //==================================================================================== Обновление кэша адресов пулов ====================================================================================
    {
        let block_receiver_clone_to_cache = block_receiver.clone();
        let last_block = *block_receiver_clone_to_cache.borrow();
        let pool_addresses_from_graph: Vec<Address> = {
            let mut addresses: Vec<Address> = graph_for_sync.get_pool_addresses().into_iter().collect();
            addresses.sort_unstable();
            addresses.dedup();
            addresses
        };
        let updated_cache = UniswapPoolCache {
            pool_addresses: pool_addresses_from_graph,
            last_verified_block: last_block,
        };
        if let Err(e) = updated_cache.save_to_bin("uniswap_pool_addresses_cache_update.bin") {
            error!("[MAIN] Ошибка сохранения обновленного кэша: {:?}", e);
        } else {
            info!("[MAIN] Обновленный кэш успешно сохранен");
        }
        if let Err(e) = updated_cache.save_to_json("debug_uniswap_cache.json") {
            error!("[MAIN] Ошибка сохранения JSON: {:?}", e);
        }
    }

    //================================================================================================ Построение арбитражных путей
    info!("[MAIN_PATH_BUILDER] Начинаем построение арбитражных путей");
    let path_build_start = std::time::Instant::now();
    let mut path_builder = PathBuilder::new(aave_rx.clone());
    path_builder.build_all_paths(graph_for_paths);
    let path_builder = Arc::new(path_builder);
    let path_builder_clone = Arc::clone(&path_builder);
    let path_build_duration = path_build_start.elapsed();
    info!(
        "[MAIN_PATH_BUILDER] Построение путей завершено за {:?} секунд, найдено {} путей",
        path_build_duration.as_secs_f64(),
        path_builder.paths.len()
    );

    //==================================================================================== Запуск симулятора арбитража ====================================================================================
    let mut arb_simulator = ArbitrageSimulator::new(
        path_builder_clone,
        aave_rx.clone(),
        graph.clone(),
        simulator_rx,
    );
    tokio::spawn(async move {
        info!("[UNISWAP_MAIN_ARB_SIMULATOR] Симулятор арбитража запущен");
        arb_simulator.run().await;
    });

    Ok(())
}

// Кастомный формат для вывода в терминал и log.md
#[derive(Clone)]
struct CustomEventFormat {
    use_ansi: bool,
}

impl CustomEventFormat {
    fn new(use_ansi: bool) -> Self {
        CustomEventFormat { use_ansi }
    }
}

impl<S, N> tracing_subscriber::fmt::format::FormatEvent<S, N> for CustomEventFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::format::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let metadata = event.metadata();
        let level = metadata.level();
        let module = metadata.module_path().unwrap_or("unknown");
        let time = chrono::Local::now().format("%H:%M:%S%.3f");

        // Эмодзи для уровней
        let level_emoji = match *level {
            Level::ERROR => "🔥",
            Level::WARN => "⚠️",
            Level::INFO => "ℹ️",
            Level::DEBUG => "🐶",
            Level::TRACE => "🔬",
        };

        // Цвета для уровней (только для терминала, если use_ansi = true)
        let level_color = if self.use_ansi {
            match *level {
                Level::ERROR => "\x1b[31;1m", // Красный жирный
                Level::WARN => "\x1b[95;1m",  // Пурпурный жирный
                Level::INFO => "\x1b[32;1m",  // Зеленый жирный
                Level::DEBUG => "\x1b[36;1m", // Голубой жирный
                Level::TRACE => "\x1b[35;1m", // Пурпурный жирный
            }
        } else {
            ""
        };

        // Форматируем префикс
        let prefix = if self.use_ansi && std::io::stdout().is_terminal() {
            format!(
                "{} {} {} {}[{}]{} {}:{} ",
                level_color,
                level_emoji,
                time,
                level_color,
                level,
                "\x1b[90;1m", // Серый жирный для модуля
                module,
                metadata.line().unwrap_or(0)
            )
        } else {
            format!(
                "{} {} [{}] {}:{} ",
                level_emoji,
                time,
                level,
                module,
                metadata.line().unwrap_or(0)
            )
        };

        // Записываем префикс
        write!(writer, "{}", prefix)?;
        // Записываем поля события
        ctx.format_fields(writer.by_ref(), event)?;
        // Добавляем перенос строки
        writeln!(writer)?;
        Ok(())
    }
}

pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name)
        .unwrap_or_else(|_| panic!("[MAIN] Environment variable {} not found", var_name))
}
