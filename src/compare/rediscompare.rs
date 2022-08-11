use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::from_str;
use std::fs;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ScenarioType {
    single2single,
    single2cluster,
    cluster2cluster,
    multisingle2single,
    multisingle2cluster,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct RedisCompare {
    pub surls: Vec<String>,
    pub turls: Vec<String>,
    pub batch_size: u32,
    pub threads: u32,
    pub ttl_diff: u32,
    pub compare_times: u32,
    pub compare_interval: u32,
    pub report: bool,
    pub scenario: ScenarioType,
}

impl RedisCompare {
    pub fn default() -> Self {
        let mut source = vec![];
        let mut target = vec![];
        source.push("redis://127.0.0.1/".to_string());
        target.push("redis://127.0.0.1/".to_string());

        Self {
            surls: source,
            turls: target,
            batch_size: 10,
            threads: 2,
            ttl_diff: 0,
            compare_times: 1,
            compare_interval: 1,
            report: false,
            scenario: ScenarioType::single2single,
        }
    }

    pub fn batch_size(&mut self, size: u32) {
        self.batch_size = size;
    }
    pub fn threads(&mut self, threads: u32) {
        self.threads = threads;
    }
    pub fn ttl_diff(&mut self, diff: u32) {
        self.ttl_diff = diff;
    }
    pub fn compare_times(&mut self, times: u32) {
        self.compare_times = times;
    }
    pub fn compare_interval(&mut self, interval: u32) {
        self.compare_interval = interval;
    }
    pub fn report(&mut self, report: bool) {
        self.report = report;
    }

    pub fn flush_to_file(&self, path: String) -> Result<()> {
        let yml = serde_yaml::to_string(&self)?;
        fs::write(path, yml)?;
        Ok(())
    }
}
