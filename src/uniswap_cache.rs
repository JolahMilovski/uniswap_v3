use bincode::{deserialize, serialize};
use ethers::types::Address;
use serde::Serialize;
use std::fs::File;
use std::io::{self, Read, Write};

#[derive(Debug, Serialize, Clone)]
pub struct UniswapPoolCache {
    pub pool_addresses: Vec<Address>,
    pub last_verified_block: u64,
}

impl UniswapPoolCache {
    pub fn new() -> Self {
        UniswapPoolCache {
            pool_addresses: Vec::new(),
            last_verified_block: 0,
        }
    }

    // Добавление адреса пула в кэш с дедупликацией
    pub fn add_pool_address(&self, address: Address) -> Self {
        let mut new_addresses = self.pool_addresses.clone();
        if !new_addresses.contains(&address) {
            new_addresses.push(address);
        }
        UniswapPoolCache {
            pool_addresses: new_addresses,
            last_verified_block: self.last_verified_block,
        }
    }

    // Сохранение кэша в бинарный файл
    pub fn save_to_bin(&self, path: &str) -> io::Result<()> {
        let cache_data = (&self.pool_addresses, self.last_verified_block);
        let serialized =
            serialize(&cache_data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = File::create(path)?;
        file.write_all(&serialized)?;
        Ok(())
    }

    // Загрузка кэша из бинарного файла
    pub fn load_from_bin(path: &str) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let (pool_addresses, last_verified_block): (Vec<Address>, u64) =
            deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(UniswapPoolCache {
            pool_addresses,
            last_verified_block,
        })
    }

    // Сохранение в JSON
    pub fn save_to_json(&self, path: &str) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&self)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}
