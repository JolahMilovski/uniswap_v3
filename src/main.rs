pub mod aave_v3_flash_monitor;
pub mod path_builder;
pub mod provider;
pub mod take_gas_price;
pub mod tick_fetcher;
pub mod token;
pub mod token_white_list;
pub mod trade_simulator;
pub mod uniswap_cache;
pub mod uniswap_events;
pub mod uniswap_graph;
pub mod uniswap_v3;

use aave_v3_flash_monitor::{get_aave_data, AaveTokenLiquidity};
use path_builder::PathBuilder;
use provider::ProviderManager;
use token::load_token_list_from_json;
use tokio::{
    signal, spawn,
    sync::{broadcast, mpsc, watch, Notify},
    time::interval,
};

use tracing::{error, info, warn, Level};
use tracing_appender::{non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard}, rolling};
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::trade_simulator::TradeSimulator;

use crate::token::TokenInfo;
use crate::uniswap_events::{PoolEventInfo, UniswapEventSubscriber};

use arc_swap::ArcSwap;
use chrono;
use dashmap::{DashMap, DashSet};
use dotenv::dotenv;
use ethers::types::Address;

use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, env};
use std::{io::IsTerminal, sync::atomic::AtomicBool};

use uniswap_cache::UniswapPoolCache;
use uniswap_graph::UniversalGraph;

// Основная асинхронная функция программы
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Загрузка переменных окружения из .env файла
    dotenv().ok();

    // Настройка ротации логов (ежеминутно) и запись в файл
    let file_appender = rolling::hourly("./logs", "uniswap_bot_log.txt");

    let (non_blocking, _guard): (NonBlocking, WorkerGuard) = NonBlockingBuilder::default()
        .buffered_lines_limit(128_000) // Буфер на 128_000 событий
        .lossy(false) // Не терять логи, применять обратное давление
        .thread_name("unilog")
        .finish(file_appender);



    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(true)
        .with_target(false)
        .with_line_number(true)
        .with_thread_names(false);

    // Настройка вывода логов в консоль
    let stdout_layer = fmt::layer()
        .with_ansi(std::io::stdout().is_terminal())
        .with_target(false)
        .with_line_number(true)
        .with_thread_names(false)
        .with_level(true)
        .event_format(CustomEventFormat::new(true));

    // Настройка фильтра логирования
    let filter = EnvFilter::builder()
        .with_default_directive(Level::DEBUG.into())
        .parse(
            "debug,\
            uniswap::aave_v3_flash_monitor=warn,\
            uniswap::path_builder=warn,\
            uniswap::provider=warn,\
            uniswap::take_gas_price=warn,\
            uniswap::token=warn,\
            uniswap::token_white_list=warn,\
            uniswap::trade_simulator=warn,\
            uniswap::uniswap_cache=warn,\
            uniswap::tick_fetcher=warn,\
            uniswap::uniswap_events=warn,\
            uniswap::uniswap_graph=warn,\
            uniswap::uniswap_v3=warn,\
            h2=off,\
            hyper=off,\
            ",
        )
        .expect("Неверная конфигурация EnvFilter");

    // Инициализация логгера
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(file_layer)
            .with(stdout_layer)
            .with(filter)
    ).expect("Ошибка настройки логгера");

    // Логирование инициализации логгера
    info!("[MAIN] Логгер инициализирован в {:?}", chrono::Utc::now());

    //============================================================================  Запуск фоновой задачи для периодического логирования (heartbeat)
    let _heartbeat = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(200));
        loop {
            ticker.tick().await;
            info!("❤️  ❤️  ❤️  [ MAIN ] Heartbeat - still running   ❤️  ❤️  ❤️");
        }
    });

    //===============================================================================================           Подключение к блокчейну через ProviderManager

    info!("[MAIN] Подключаемся к блокчейну");

    let provider_manager = ProviderManager::new(500).await;
    let provider_http = provider_manager.get_http().await;
    let provider_for_aave = provider_http.clone();
    let provider_for_sub_block = provider_http.clone();
    let provider_http_for_sync = provider_http.clone();
    let provider_http_for_polling = provider_http.clone();

    //===========================================================================================        Инициализация кэша токенов

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

    //==============================================================================================         Загрузка белого списка токенов

    let token_whitelist_set: Arc<DashSet<Address>> =
        Arc::new(token_white_list::load_token_whitelist());
    info!(
        "[MAIN] Загружено {} токенов из белого списка",
        token_whitelist_set.len()
    );

    //=============================================================================================          Инициализация кэша пулов Uniswap

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

    //==========================================================================================     Инициализация графа Uniswap

    let graph: Arc<ArcSwap<UniversalGraph>> =
        Arc::new(ArcSwap::from_pointee(UniversalGraph::new(32)));
    let graph_for_sync = Arc::clone(&graph);
    let graph_for_paths = Arc::clone(&graph);
    let graph_for_shutdown = Arc::clone(&graph);
    let pulling_graph = Arc::clone(&graph);

    //=======================================================================================        Обработка сигнала завершения (Ctrl+C) и сохранение графа

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = Arc::clone(&shutdown_notify);
    spawn(async move {
        signal::ctrl_c()
            .await
            .expect("[MAIN] Ошибка в обработке сигнала Ctrl+C");
        if let Err(e) = graph_for_shutdown
            .load()
            .save_graph_to_json("uniswap_graph_snapshot.json")
        {
            error!("[MAIN] Ошибка при сохранении графа в JSON: {:?}", e);
        } else {
            info!("[MAIN] Граф успешно сохранен в uniswap_graph_snapshot.json");
        }
        shutdown_notify_clone.notify_one();
        info!("[MAIN] Уведомление о завершении отправлено, выход из программы");
        std::process::exit(0);
    });

    //============================================================================ Запуск мониторинга ликвидности Aave

    let (aave_tx, aave_rx) = watch::channel(AaveTokenLiquidity::default());
    info!("[MAIN] Мониторинг ликвидности Aave запущен в фоне");
    let aave_handle = spawn({
        async move {
            if let Err(e) = get_aave_data(provider_for_aave, aave_tx).await {
                error!("[MAIN] Ошибка в мониторинге ликвидности Aave: {:?}", e);
            } else {
                info!("[MAIN] Мониторинг ликвидности Aave завершен");
            }
        }
    });

    //============================================================================ Подписка на новые блоки

    let (block_sender, block_receiver) = watch::channel(0);
    let block_handle = spawn({
        let provider_ws = Arc::clone(&provider_for_sub_block);
        async move {
            match UniswapEventSubscriber::subscribe_to_new_blocks(
                &provider_ws,
                block_sender.clone(),
            )
            .await
            {
                Ok(_) => {
                    info!("[MAIN_SUBSCRIBE_BLOCKS] Подписка на новые блоки успешно запущена");
                }
                Err(e) => {
                    error!("[MAIN_SUBSCRIBE_BLOCKS] Ошибка подписки на блоки: {}. Завершение программы", e);
                }
            }
        }
    });

    //========================================================================================================           Создание модуля пулинга событий

    info!("[MAIN] Инициализация модуля пулинга событий");

    let (event_tx, _event_rx) = broadcast::channel::<HashMap<Address, PoolEventInfo>>(2048);

    let subscriber: Arc<UniswapEventSubscriber> =
        Arc::new(UniswapEventSubscriber::new(provider_http.clone()));
    let subscriber_clone = Arc::clone(&subscriber);
    let block_receiver_clone_to_subscriber = block_receiver.clone();
    let is_paths_built = Arc::new(AtomicBool::new(false));

    // Создание задачи для обработки событий
    let is_paths_built_for_polling = Arc::clone(&is_paths_built);
    let event_tx_for_polling = event_tx.clone();
    let (simulator_tx, simulator_rx) = mpsc::channel::<PoolEventInfo>(2000);
    //let token_cache_clone = Arc::clone(&token_cache);
    let polling_handle = spawn({
        let graph = Arc::clone(&pulling_graph);
        let provider_http = Arc::clone(&provider_http_for_polling);
        async move {
            if let Err(e) = &subscriber_clone
                .polling_event(
                    &block_receiver_clone_to_subscriber,
                    graph,
                    event_tx_for_polling,
                    is_paths_built_for_polling,
                    provider_http,
                    simulator_tx,
                )
                .await
            {
                error!(
                    "[MAIN_POLLING] Ошибка выполнения задачи пулинга событий: {:?}",
                    e
                );
            } else {
                warn!("[MAIN_POLLING] Задача пулинга событий завершена штатно");
            }
        }
    });

    //============================================================================ Синхронизация пулов Uniswap

    info!("[MAIN] Синхронизация пулов начата...");

    match uniswap_v3::sync_pools(
        graph_for_sync.clone(),
        provider_http_for_sync.clone(),
        &token_cache,
        pool_cache.clone(),
        &token_whitelist_set,
        subscriber.clone(),
    )
    .await
    {
        Ok(_) => {
            info!("[MAIN] Синхронизация пулов завершена");
        }
        Err(_e) => {
            error!("[MAIN] Ошибка синхронизации пулов");
        }
    }

    info!("[MAIN] Бот завершил сканирование пулов");

    //============================================================================ Обновление и сохранение кэша пулов

    let block_receiver_clone_to_cache = block_receiver.clone();
    let last_block = *block_receiver_clone_to_cache.borrow();
    let pool_addresses_from_graph: Vec<Address> = {
        let mut addresses: Vec<Address> = graph_for_sync
            .load()
            .get_pool_addresses()
            .into_iter()
            .collect();
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
        error!("[MAIN] Ошибка сохранения JSON кэша: {:?}", e);
    }

    //============================================================================ Построение арбитражных путей

    let paths_built_notify = Arc::new(Notify::new());
    info!(
        "[MAIN_PATH_BUILDER] Начинаем построение арбитражных путей. Состояние графа перед PathBuilder: nodes={}, edges={}", 
        graph_for_paths.load().nodes.len(),
        graph_for_paths.load().edges.len()
    );

    let path_build_start = std::time::Instant::now();
    let mut path_builder = PathBuilder::new(aave_rx.clone(), Arc::clone(&is_paths_built));

    path_builder.build_all_paths(graph_for_paths.clone());

    let path_builder = Arc::new(path_builder);
    let path_build_duration = path_build_start.elapsed();
    is_paths_built.store(true, std::sync::atomic::Ordering::Release);
    paths_built_notify.notify_one();
    info!(
        "[MAIN_PATH_BUILDER] Построение путей завершено за {} мс. Количество путей: {}",
        path_build_duration.as_millis(),
        path_builder.paths.len()
    );

    //============================================================================ Запуск симулятора арбитража

    let mut trade_simulator =
        TradeSimulator::new(Arc::clone(&path_builder), aave_rx, Arc::clone(&graph));

    info!("[MAIN] Запуск TradeSimulator");
    let simulation_handle = spawn(async move {
        trade_simulator.run(simulator_rx).await;
        info!("[MAIN_TRADE_SIMULATOR] TradeSimulator завершил работу");
    });

    // ============================================================================ Ожидание завершения задач или сигнала завершения

    info!("[MAIN] Ожидание завершения задач или сигнала завершения");
    tokio::select! {
        _ = shutdown_notify.notified() => {
            info!("[MAIN] Получен сигнал завершения");
        }
        _ = aave_handle => {
            warn!("[MAIN] Задача Aave завершилась неожиданно");
        }
        _ = block_handle => {
            warn!("[MAIN] Задача подписки на блоки завершилась неожиданно");
        }
        _ = polling_handle => {
            warn!("[MAIN] Задача polling_event завершилась неожиданно");
        }
        _ = simulation_handle => {
            warn!("[MAIN] Задача SimulationRunner завершилась неожиданно");
        }

    }

    // Завершение программы
    info!("[MAIN] Программа завершает работу");
    Ok(())
}

//================================================================================= ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =================================================================================


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

        let level_emoji = match *level {
            Level::ERROR => "🔥",
            Level::WARN => "⚠️",
            Level::INFO => "ℹ️",
            Level::DEBUG => "🐶",
            Level::TRACE => "🔬",
        };

        let level_color = if self.use_ansi {
            match *level {
                Level::ERROR => "\x1b[31;1m",
                Level::WARN => "\x1b[95;1m",
                Level::INFO => "\x1b[32;1m",
                Level::DEBUG => "\x1b[36;1m",
                Level::TRACE => "\x1b[35;1m",
            }
        } else {
            ""
        };

        let prefix = if self.use_ansi && std::io::stdout().is_terminal() {
            format!(
                "{} {} {} {}[{}]{} {}:{} ",
                level_color,
                level_emoji,
                time,
                level_color,
                level,
                "\x1b[90;1m",
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

        write!(writer, "{}", prefix)?;
        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)?;
        Ok(())
    }
}

pub fn get_env_var(var_name: &str) -> String {
    env::var(var_name)
        .unwrap_or_else(|_| panic!("[MAIN] Environment variable {} not found", var_name))
}
