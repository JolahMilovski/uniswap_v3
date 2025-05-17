
use ethers_providers::{Provider, Ws, Http};
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct ProviderManager {
    ws_providers: Vec<Arc<Provider<Ws>>>,      // Список WS-провайдеров
    http_providers: Vec<Arc<Provider<Http>>>,  // Список HTTP-провайдеров
    ws_index: AtomicUsize,                     // Роунд-робин для WS
    http_index: AtomicUsize,                   // Роунд-робин для HTTP
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,                 // Общий RPS-лимит (WS + HTTP)
}

impl ProviderManager {
    pub async fn new(rps_limit: u32) -> Arc<Self> {
        // Загружаем URL из переменных окружения
        let ws_urls = vec![
            //get_env_var("WS_PROVIDER_URL_ALCHEMY_FIRST"),
            get_env_var("WS_PROVIDER_URL_ALCHEMY_SECOND"),
        ];

        let http_urls = vec![
           // get_env_var("HTTP_PROVIDER_URL_ALCHEMY_FIRST"),
            get_env_var("HTTP_PROVIDER_URL_ALCHEMY_SECOND"),
        ];

        // Настраиваем общий лимит запросов
        let rps_limit = NonZeroU32::new(rps_limit.max(1)).unwrap();
        let quota = Quota::per_second(rps_limit);
        
        let limiter = Arc::new(RateLimiter::direct(quota));

        // Инициализируем WS-провайдеры
        let mut ws_providers = Vec::new();
        for url in ws_urls {
            let ws = Ws::connect(&url).await.expect("Failed to connect WS");
            ws_providers.push(Arc::new(Provider::new(ws)));
        }

        // Инициализируем HTTP-провайдеры
        let mut http_providers = Vec::new();
        for url in http_urls {
            let http = Provider::<Http>::try_from(&url).expect("Failed to create HTTP provider");
            http_providers.push(Arc::new(http));
        }

        Arc::new(Self {
            ws_providers,
            http_providers,
            ws_index: AtomicUsize::new(0),
            http_index: AtomicUsize::new(0),
            limiter,
        })
    }

    /// Получить WS-провайдер (для подписок на события)
    pub async fn get_ws(&self) -> Arc<Provider<Ws>> {
        self.limiter.until_ready().await; // Соблюдаем RPS-лимит
        let idx = self.ws_index.fetch_add(1, Ordering::Relaxed);
        self.ws_providers[idx % self.ws_providers.len()].clone()
    }

    /// Получить HTTP-провайдер (для обычных запросов)
    pub async fn get_http(&self) -> Arc<Provider<Http>> {
        self.limiter.until_ready().await; // Тот же лимит!
        let idx = self.http_index.fetch_add(1, Ordering::Relaxed);
        self.http_providers[idx % self.http_providers.len()].clone()
    }
}

/// Вспомогательная функция для загрузки переменных окружения
fn get_env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("Env var {} not set", name))
}



/*
use ethers_providers::{Middleware, Provider, Ws, Http};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering}, 
        Arc
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::get_env_var;

#[derive(Clone)]
enum ProviderType {
    Ws(Arc<Provider<Ws>>),
    Http(Arc<Provider<Http>>),
}

struct ProviderState {
    provider: ProviderType,
    failed_attempts: AtomicU32,
    last_failed_ts: AtomicU64,
    blacklisted: AtomicBool,
}

pub struct ProviderManager {
    ws_providers: Vec<Arc<ProviderState>>,
    http_providers: Vec<Arc<ProviderState>>,
    blacklist_duration_secs: u64,
    ws_index: AtomicUsize,
    http_index: AtomicUsize,
}

impl ProviderManager {
    pub async fn new() -> Arc<Self> {
        let ws_urls = vec![

            get_env_var("WS_PROVIDER_URL_ALCHEMY_FIRST"),
            get_env_var("WS_PROVIDER_URL_ALCHEMY_SECOND"),
        ];

        let http_urls = vec![
            get_env_var("HTTP_PROVIDER_URL_ALCHEMY_FIRST"),
            get_env_var("HTTP_PROVIDER_URL_ALCHEMY_SECOND"),
        ];

        let mut ws_providers = Vec::new();
        let mut http_providers = Vec::new();

        for url in ws_urls {
            if let Ok(ws) = Ws::connect(&url).await {
                let provider = Arc::new(Provider::new(ws));
                ws_providers.push(Arc::new(ProviderState {
                    provider: ProviderType::Ws(provider),
                    failed_attempts: AtomicU32::new(0),
                    last_failed_ts: AtomicU64::new(0),
                    blacklisted: AtomicBool::new(false),
                }));
            }
        }

        for url in http_urls {
            if let Ok(provider) = Provider::<Http>::try_from(&url) {
                http_providers.push(Arc::new(ProviderState {
                    provider: ProviderType::Http(Arc::new(provider)),
                    failed_attempts: AtomicU32::new(0),
                    last_failed_ts: AtomicU64::new(0),
                    blacklisted: AtomicBool::new(false),
                }));
            }
        }

        Arc::new(Self {
            ws_providers,
            http_providers,
            blacklist_duration_secs: 600,
            ws_index: AtomicUsize::new(0),
            http_index: AtomicUsize::new(0),
        })
    }

    pub async fn get_ws_provider(&self) -> Option<Arc<Provider<Ws>>> {
        self.get_provider_internal(&self.ws_providers, &self.ws_index).await.map(|p| {
            if let ProviderType::Ws(provider) = p {
                provider
            } else {
                unreachable!()
            }
        })
    }

    pub async fn get_http_provider(&self) -> Option<Arc<Provider<Http>>> {
        self.get_provider_internal(&self.http_providers, &self.http_index).await.map(|p| {
            if let ProviderType::Http(provider) = p {
                provider
            } else {
                unreachable!()
            }
        })
    }

    async fn get_provider_internal(
        &self,
        providers: &[Arc<ProviderState>],
        index: &AtomicUsize,
    ) -> Option<ProviderType> {
        if providers.is_empty() {
            return None;
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let start_idx = index.fetch_add(1, Ordering::Relaxed);
        let mut current_idx = start_idx % providers.len();

        for _ in 0..providers.len() {
            let state = &providers[current_idx];
            current_idx = (current_idx + 1) % providers.len();

            let blacklisted = state.blacklisted.load(Ordering::Relaxed);
            if blacklisted {
                let last_failed = state.last_failed_ts.load(Ordering::Relaxed);
                if last_failed != 0 && now_secs - last_failed < self.blacklist_duration_secs {
                    continue;
                }
            }

            let result = match &state.provider {
                ProviderType::Ws(provider) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    provider.get_block_number().await
                },
                ProviderType::Http(provider) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    provider.get_block_number().await
                },
            };

            match result {
                Ok(_) => {
                    state.failed_attempts.store(0, Ordering::Relaxed);
                    state.blacklisted.store(false, Ordering::Relaxed);
                    return Some(state.provider.clone());
                }
                Err(_) => {
                    state.last_failed_ts.store(now_secs, Ordering::Relaxed);
                    let fails = state.failed_attempts.fetch_add(1, Ordering::Relaxed) + 1;

                    if fails >= 3 {
                        state.blacklisted.store(true, Ordering::Relaxed);
                    }
                    
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        None
    }
}

pub async fn get_working_ws_provider() -> Arc<Provider<Ws>> {
    let manager = ProviderManager::new().await;

    loop {
        if let Some(provider) = manager.get_ws_provider().await {
            return provider;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn get_working_http_provider() -> Arc<Provider<Http>> {
    let manager = ProviderManager::new().await;

    loop {
        if let Some(provider) = manager.get_http_provider().await {
            return provider;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
} */