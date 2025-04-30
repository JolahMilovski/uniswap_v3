use ethers_providers::{Middleware, Provider, Ws};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use crate::get_env_var;

struct ProviderState {
    provider: Arc<Provider<Ws>>,
    url: String,
    failed_attempts: u32,
    last_failed: Option<Instant>,
}

pub struct ProviderManager {
    providers: Vec<ProviderState>,
    blacklist: HashSet<String>,
    blacklist_duration: Duration,
}

impl ProviderManager {
    pub async fn new() -> Arc<Mutex<Self>> {
        let urls = vec![
            get_env_var("WS_PROVIDER_URL_FIRST"),
            get_env_var("WS_PROVIDER_URL_SECOND"),
        ];

        let mut providers = Vec::new();
        for url in urls {
            match Ws::connect(&url).await {
                Ok(ws) => {
                    let provider = Arc::new(Provider::new(ws));
                    providers.push(ProviderState {
                        provider,
                        url: url.clone(),
                        failed_attempts: 0,
                        last_failed: None,
                    });
                }
                Err(e) => eprintln!("[init] Ошибка подключения к {url}: {e}"),
            }
        }

        Arc::new(Mutex::new(Self {
            providers,
            blacklist: HashSet::new(),
            blacklist_duration: Duration::from_secs(600),
        }))
    }

    pub async fn get_provider(&mut self) -> Option<Arc<Provider<Ws>>> {
        for state in &mut self.providers {
            let now = Instant::now();

            let blacklisted = self.blacklist.contains(&state.url);
            if blacklisted {
                if let Some(last_failed) = state.last_failed {
                    if now.duration_since(last_failed) < self.blacklist_duration {
                        continue;
                    }
                }
            }

            match state.provider.get_block_number().await {
                Ok(block) => {
                    if blacklisted {
                        println!("[check] Провайдер {} восстановлен, блок {}", state.url, block);
                        self.blacklist.remove(&state.url);
                    } else {
                        println!("[ok] Провайдер {}: блок {}", state.url, block);
                    }

                    state.failed_attempts = 0;
                    return Some(state.provider.clone());
                }
                Err(e) => {
                    eprintln!("[fail] {}: {e}", state.url);
                    state.failed_attempts += 1;

                    if state.failed_attempts >= 3 {
                        self.blacklist.insert(state.url.clone());
                        state.last_failed = Some(now);
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
        let mut lock = manager.lock().await;
        if let Some(provider) = lock.get_provider().await {
            return provider;
        }
        drop(lock); // отпускаем мьютекс перед ожиданием
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
