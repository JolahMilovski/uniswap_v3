use ethers::prelude::Provider;
use ethers::types::U256;
use ethers_providers::{Http, Middleware};
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;

pub struct GasPriceFeed {
    pub receiver: watch::Receiver<U256>,
}

impl GasPriceFeed {
    pub fn new() -> (Self, watch::Sender<U256>) {
        let (tx, rx) = watch::channel(U256::zero());
        (Self { receiver: rx }, tx)
    }
}

// Асинхронная функция, которая в бесконечном цикле запрашивает цену газа и отправляет в канал
pub async fn start_gas_price_loop(
    provider: Arc<Provider<Http>>,
    sender: watch::Sender<U256>,
) {
    loop {
       
        match provider.get_gas_price().await {
            Ok(price) => {
                // Логируем цену газа в терминал сразу при получении
                info!(" [GET_GAS_PRICE] ЦЕНА ГАЗА: {} wei", price);

                // Отправляем в канал
                let _ = sender.send(price);
            }
            Err(e) => {
                eprintln!(" [GET_GAS_PRICE] Failed to fetch gas price: {:?}", e);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}
