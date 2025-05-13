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
}