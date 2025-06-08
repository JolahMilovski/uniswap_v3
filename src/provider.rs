use ethers_providers::{Http, Provider, Ws};
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ProviderManager {
    ws_providers: Vec<Arc<Provider<Ws>>>, // Список WS-провайдеров
    http_providers: Vec<Arc<Provider<Http>>>, // Список HTTP-провайдеров
    ws_index: AtomicUsize,                // Роунд-робин для WS
    http_index: AtomicUsize,              // Роунд-робин для HTTP
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>, // Общий RPS-лимит (WS + HTTP)
}

impl ProviderManager {
    pub async fn new(rps_limit: u32) -> Arc<Self> {
        // Загружаем URL из переменных окружения
        let ws_urls = vec![
            get_env_var("WS_PROVIDER_URL_ALCHEMY_FIRST"),
            //get_env_var("WS_PROVIDER_URL_ALCHEMY_SECOND"),
           // get_env_var("WS_PROVIDER_URL_INFURA_FIRST"),
        ];

        let http_urls = vec![
            get_env_var("HTTP_PROVIDER_URL_ALCHEMY_FIRST"),
            //get_env_var("HTTP_PROVIDER_URL_ALCHEMY_SECOND"),
            //get_env_var("HTTP_PROVIDER_URL_INFURA_FIRST"),
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
