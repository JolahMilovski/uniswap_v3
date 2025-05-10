use ethers_providers::{Middleware, Provider, Ws, Http};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
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
    url: String,
    failed_attempts: AtomicU32,
    last_failed_ts: AtomicU64, // UNIX timestamp в секундах
    blacklisted: AtomicBool,
}

pub struct ProviderManager {
    ws_providers: Vec<Arc<ProviderState>>,
    http_providers: Vec<Arc<ProviderState>>,
    blacklist_duration_secs: u64,
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

        // Инициализация WS провайдеров
        for url in ws_urls {
            match Ws::connect(&url).await {
                Ok(ws) => {
                    let provider = Arc::new(Provider::new(ws));
                    ws_providers.push(Arc::new(ProviderState {
                        provider: ProviderType::Ws(provider),
                        url: url.clone(),
                        failed_attempts: AtomicU32::new(0),
                        last_failed_ts: AtomicU64::new(0),
                        blacklisted: AtomicBool::new(false),
                    }));
                }
                Err(e) => eprintln!("[init] Ошибка подключения к WS провайдеру {url}: {e}"),
            }
        }

        // Инициализация HTTP провайдеров
        for url in http_urls {
            match Provider::<Http>::try_from(&url) {
                Ok(provider) => {
                    http_providers.push(Arc::new(ProviderState {
                        provider: ProviderType::Http(Arc::new(provider)),
                        url: url.clone(),
                        failed_attempts: AtomicU32::new(0),
                        last_failed_ts: AtomicU64::new(0),
                        blacklisted: AtomicBool::new(false),
                    }));
                }
                Err(e) => eprintln!("[init] Ошибка подключения к HTTP провайдеру {url}: {e}"),
            }
        }

        Arc::new(Self {
            ws_providers,
            http_providers,
            blacklist_duration_secs: 600,
        })
    }

    pub async fn get_ws_provider(&self) -> Option<Arc<Provider<Ws>>> {
        self.get_provider_internal(&self.ws_providers).await.map(|p| {
            if let ProviderType::Ws(provider) = p {
                provider
            } else {
                unreachable!()
            }
        })
    }

    pub async fn get_http_provider(&self) -> Option<Arc<Provider<Http>>> {
        self.get_provider_internal(&self.http_providers).await.map(|p| {
            if let ProviderType::Http(provider) = p {
                provider
            } else {
                unreachable!()
            }
        })
    }

    async fn get_provider_internal(&self, providers: &[Arc<ProviderState>]) -> Option<ProviderType> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for state in providers {
            let blacklisted = state.blacklisted.load(Ordering::Relaxed);
            if blacklisted {
                let last_failed = state.last_failed_ts.load(Ordering::Relaxed);
                if last_failed != 0 && now_secs - last_failed < self.blacklist_duration_secs {
                    continue;
                }
            }

            let result = match &state.provider {
                ProviderType::Ws(provider) => provider.get_block_number().await,
                ProviderType::Http(provider) => provider.get_block_number().await,
            };

            match result {
                Ok(block) => {
                    if blacklisted {
                        println!("[check] Провайдер {} восстановлен, блок {}", state.url, block);
                        state.blacklisted.store(false, Ordering::Relaxed);
                    } else {
                        println!("[ok] Провайдер {}: блок {}", state.url, block);
                    }

                    state.failed_attempts.store(0, Ordering::Relaxed);
                    return Some(state.provider.clone());
                }
                Err(e) => {
                    eprintln!("[fail] {}: {e}", state.url);
                    let fails = state.failed_attempts.fetch_add(1, Ordering::Relaxed) + 1;

                    if fails >= 3 && !state.blacklisted.load(Ordering::Relaxed) {
                        state.blacklisted.store(true, Ordering::Relaxed);
                        state.last_failed_ts.store(now_secs, Ordering::Relaxed);
                        println!("[blacklist] Провайдер {} временно отключен", state.url);
                    }
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
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn get_working_http_provider() -> Arc<Provider<Http>> {
    let manager = ProviderManager::new().await;

    loop {
        if let Some(provider) = manager.get_http_provider().await {
            return provider;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}