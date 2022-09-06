use super::comparekey::{Comparer, RedisKey};
use crate::util::{key_type, scan};
use anyhow::anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ScenarioType {
    Single2single,
    Single2cluster,
    Cluster2cluster,
    Multisingle2single,
    Multisingle2cluster,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct RedisCompare {
    pub surls: Vec<String>,
    pub turls: Vec<String>,
    pub batch_size: usize,
    pub threads: usize,
    pub ttl_diff: usize,
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
            scenario: ScenarioType::Single2single,
        }
    }

    pub fn batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }
    pub fn threads(&mut self, threads: usize) {
        self.threads = threads;
    }
    pub fn ttl_diff(&mut self, diff: usize) {
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

    pub fn exec(&self) {
        match self.scenario {
            ScenarioType::Single2single => {
                self.exec_single2single();
            }
            ScenarioType::Single2cluster => self.exec_single2cluster(),
            ScenarioType::Cluster2cluster => self.exec_cluster2cluster(),
            ScenarioType::Multisingle2single => self.exec_multisingle2single(),
            ScenarioType::Multisingle2cluster => self.exec_multisingle2cluster(),
        }
    }
}

impl RedisCompare {
    pub fn exec_single2single(&self) -> Result<()> {
        println!("exec single2single");
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .map_err(|err| anyhow!("{}", err.to_string()))?;

        pool.scope(|s| {
            let sclient = redis::Client::open(self.surls[0].as_str());
            if let Err(e) = sclient {
                log::error!("{}", e);
                return;
            }

            let tclient = redis::Client::open(self.turls[0].as_str());
            if let Err(e) = tclient {
                log::error!("{}", e);
                return;
            }

            let scan_conn = sclient.as_ref().unwrap().get_connection();
            match scan_conn {
                Ok(mut conn) => {
                    let s_iter = scan::<String>(&mut conn);
                    match s_iter {
                        Ok(i) => {
                            let mut vec_key: Vec<RedisKey> = Vec::new();
                            let mut count = 0 as usize;

                            let mut keytype_conn =
                                sclient.as_ref().unwrap().get_connection().unwrap();
                            for item in i {
                                if count < self.batch_size {
                                    let key = key_type(item.clone(), &mut keytype_conn);
                                    if let Ok(keytype) = key {
                                        let rediskey = RedisKey {
                                            key: item.clone(),
                                            key_type: keytype,
                                        };
                                        vec_key.push(rediskey);
                                    }
                                    count += 1;
                                    continue;
                                }
                                let vk = vec_key.clone();
                                let mut sconn = sclient.as_ref().unwrap().get_connection();
                                if let Err(e) = sconn {
                                    log::error!("{}", e);
                                    return;
                                }
                                let mut tconn = tclient.as_ref().unwrap().get_connection();
                                if let Err(e) = tconn {
                                    log::error!("{}", e);
                                    return;
                                }
                                s.spawn(move |_| {
                                    let mut comparer = Comparer {
                                        sconn: &mut sconn.unwrap(),
                                        tconn: &mut tconn.unwrap(),
                                        ttl_diff: self.ttl_diff,
                                        batch: self.batch_size,
                                    };
                                    for k in vk {
                                        let r = comparer.compare_key(k);
                                        if let Err(e) = r {
                                            log::error!("{}", e);
                                        }
                                    }
                                });

                                count = 0;
                                vec_key.clear();
                                let key = key_type(item.clone(), &mut keytype_conn);
                                if let Ok(keytype) = key {
                                    let rediskey = RedisKey {
                                        key: item.clone(),
                                        key_type: keytype,
                                    };
                                    vec_key.push(rediskey);
                                }
                                count += 1;
                            }

                            if vec_key.len() > 0 {
                                let vk = vec_key.clone();
                                // let mut sconn = sclient.as_ref().unwrap().get_connection().unwrap();
                                // let mut tconn = tclient.as_ref().unwrap().get_connection().unwrap();
                                let mut sconn = sclient.as_ref().unwrap().get_connection();
                                if let Err(e) = sconn {
                                    log::error!("{}", e);
                                    return;
                                }
                                let mut tconn = tclient.as_ref().unwrap().get_connection();
                                if let Err(e) = tconn {
                                    log::error!("{}", e);
                                    return;
                                }
                                s.spawn(move |_| {
                                    let mut comparer = Comparer {
                                        sconn: &mut sconn.unwrap(),
                                        tconn: &mut tconn.unwrap(),
                                        ttl_diff: self.ttl_diff,
                                        batch: self.batch_size,
                                    };
                                    for k in vk {
                                        let r = comparer.compare_key(k);
                                        if let Err(e) = r {
                                            log::error!("{}", e);
                                        }
                                    }
                                });
                            }
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            }
        });

        Ok(())
    }
    pub fn exec_single2cluster(&self) {
        println!("exec single2cluster");
    }
    pub fn exec_cluster2cluster(&self) {
        println!("exec cluster2cluster");
    }
    pub fn exec_multisingle2single(&self) {
        println!("exec multisingle2single");
    }
    pub fn exec_multisingle2cluster(&self) {
        println!("exec multisingle2cluster");
    }
}

pub fn compare_task(comparer: &mut Comparer, keys_vec: Vec<RedisKey>) {
    for key in keys_vec {
        let r = comparer.compare_key(key);
        if let Err(e) = r {
            log::error!("{}", e);
        }
    }
}
