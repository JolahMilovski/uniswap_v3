use ethers_providers::{Middleware, Provider, Ws};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crate::get_env_var;

struct ProviderState {
    provider: Arc<Provider<Ws>>,
    url: String,
    failed_attempts: AtomicU32,
    last_failed_ts: AtomicU64, // UNIX timestamp в секундах
    blacklisted: AtomicBool,
}

pub struct ProviderManager {
    providers: Vec<Arc<ProviderState>>,
    blacklist_duration_secs: u64,
}

impl ProviderManager {
    pub async fn new() -> Arc<Self> {
        let urls = vec![
            get_env_var("WS_PROVIDER_URL_FIRST"),
            get_env_var("WS_PROVIDER_URL_SECOND"),
        ];

        let mut providers = Vec::new();
        for url in urls {
            match Ws::connect(&url).await {
                Ok(ws) => {
                    let provider = Arc::new(Provider::new(ws));
                    providers.push(Arc::new(ProviderState {
                        provider,
                        url: url.clone(),
                        failed_attempts: AtomicU32::new(0),
                        last_failed_ts: AtomicU64::new(0),
                        blacklisted: AtomicBool::new(false),
                    }));
                }
                Err(e) => eprintln!("[init] Ошибка подключения к {url}: {e}"),
            }
        }

        Arc::new(Self {
            providers,
            blacklist_duration_secs: 600,
        })
    }

    pub async fn get_provider(&self) -> Option<Arc<Provider<Ws>>> {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for state in &self.providers {
            let blacklisted = state.blacklisted.load(Ordering::Relaxed);
            if blacklisted {
                let last_failed = state.last_failed_ts.load(Ordering::Relaxed);
                if last_failed != 0 && now_secs - last_failed < self.blacklist_duration_secs {
                    continue;
                }
            }

            match state.provider.get_block_number().await {
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

pub async fn get_working_provider() -> Arc<Provider<Ws>> {
    let manager = ProviderManager::new().await;

    loop {
        if let Some(provider) = manager.get_provider().await {
            return provider;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
