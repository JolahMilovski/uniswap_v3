use std::{collections::HashSet, fs::File};
use ethers::types::Address;
use ethers_providers::{Middleware, Provider, Ws};
use log::info;
use serde::Serialize;
use std::io::{self, Write, Read};
use bincode::{serialize, deserialize};

#[derive(Debug,Serialize,Clone)]
pub struct UniswapPoolCache {
    pub pool_addresses: HashSet<Address>,
    pub last_verified_block: u64, 
}

impl UniswapPoolCache {
    
    pub fn new() -> Self {
        UniswapPoolCache {
            pool_addresses: HashSet::new(),
            last_verified_block: 0,
        }
    }
    
    // Добавление адреса пула в кэш
    pub fn add_pool_address(&mut self, address: Address) {
        self.pool_addresses.insert(address);
    }

    // Обновление последнего проверенного блока
    pub async fn update_last_verified_block(&mut self, provider: &Provider<Ws>) -> Result<(), String> {
        match provider.get_block_number().await {
            Ok(block_number) => {
                self.last_verified_block = block_number.as_u64();  // Обновляем значение в структуре
                Ok(())  
            }
            Err(e) => Err(format!("[КЭШ]Ошибка получения последнего блока: {:?}", e)),  
        }
    }
  

      // Сохранение кэша в бинарный файл
      pub fn save_to_bin(&self, path: &str) -> io::Result<()> {
        let cache_data = (&self.pool_addresses, self.last_verified_block);
        let serialized = serialize(&cache_data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = File::create(path)?;
        file.write_all(&serialized)?;
        Ok(())
    }

    // Загрузка кэша из бинарного файла
    pub fn load_from_bin(path: &str) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let (pool_addresses, last_verified_block): (HashSet<Address>, u64) = deserialize(&buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(UniswapPoolCache { 
            pool_addresses,
            last_verified_block 
        })
    }

    // Сохранение в JSON (уже принимает путь)
    pub fn save_to_json(&self, path: &str) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(&self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    /// Обновляет кэш новыми пулами, сохраняя существующие
    pub fn update_with_new_pools(&mut self, new_pools: &HashSet<Address>, current_block: u64) -> io::Result<()> {
        let old_count = self.pool_addresses.len();        
        for addr in new_pools {
            self.pool_addresses.insert(*addr);
        }        
        self.last_verified_block = current_block;        
        info!("[КЭШ]Обновлен кэш пулов: было {} -> стало {}", old_count, self.pool_addresses.len());        
        self.save_to_bin("uniswap_pool_addresses_cache.bin")
    }

}
