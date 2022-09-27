use std::collections::HashMap;
use std::time::Instant;

use super::comparekey::Comparer;
use crate::compare::compare_error::CompareError;
use crate::util::{key_type, scan};
use crate::util::{RedisClient, RedisKey};

use anyhow::Result;

use redis::cluster::ClusterClientBuilder;
use redis::ConnectionLike;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ScenarioType {
    Single2single,
    Single2cluster,
    Cluster2cluster,
    Multisingle2single,
    Multisingle2cluster,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum InstanceType {
    Single,
    Cluster,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct RedisInstance {
    #[serde(default = "RedisInstance::urls_default")]
    pub urls: Vec<String>,
    #[serde(default = "RedisInstance::password_default")]
    pub password: String,
    #[serde(default = "RedisInstance::instance_type_default")]
    pub instance_type: InstanceType,
}

impl Default for RedisInstance {
    fn default() -> Self {
        Self {
            urls: vec!["redis://127.0.0.1:6379".to_string()],
            password: "".to_string(),
            instance_type: InstanceType::Single,
        }
    }
}

impl RedisInstance {
    pub fn urls_default() -> Vec<String> {
        vec!["redis://127.0.0.1:6379".to_string()]
    }
    pub fn password_default() -> String {
        "".to_string()
    }
    pub fn instance_type_default() -> InstanceType {
        InstanceType::Single
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DBInstance {
    instance: RedisInstance,
    db: usize,
}

impl Default for DBInstance {
    fn default() -> Self {
        Self {
            instance: RedisInstance::default(),
            db: 0,
        }
    }
}

// #[derive(Debug, Clone)]
// pub struct DBClient {
//     client: redis::Client,
//     db: usize,
// }

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SourceInstance {
    #[serde(default = "SourceInstance::instance_default")]
    pub instance: RedisInstance,
    #[serde(default = "SourceInstance::dbmapper_default")]
    pub dbmapper: HashMap<usize, usize>,
}

impl Default for SourceInstance {
    fn default() -> Self {
        let mut mapper = HashMap::new();
        mapper.insert(0, 0);
        Self {
            instance: RedisInstance::default(),
            dbmapper: mapper,
        }
    }
}

impl SourceInstance {
    pub fn instance_default() -> RedisInstance {
        RedisInstance::default()
    }
    pub fn dbmapper_default() -> HashMap<usize, usize> {
        let mut mapper = HashMap::new();
        mapper.insert(0, 0);
        mapper
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Compare {
    #[serde(default = "Compare::source_default")]
    pub source: Vec<SourceInstance>,
    #[serde(default = "Compare::target_default")]
    pub target: RedisInstance,
    #[serde(default = "Compare::batch_size_default")]
    pub batch_size: usize,
    #[serde(default = "Compare::threads_default")]
    pub threads: usize,
    #[serde(default = "Compare::compare_threads_default")]
    pub compare_threads: usize,
    #[serde(default = "Compare::ttl_diff_default")]
    pub ttl_diff: usize,
    #[serde(default = "Compare::compare_times_default")]
    pub compare_times: u32,
    #[serde(default = "Compare::compare_interval_default")]
    pub compare_interval: u32,
    #[serde(default = "Compare::report_default")]
    pub report: bool,
    #[serde(default = "Compare::scenario_default")]
    pub scenario: ScenarioType,
    //双向校验
    #[serde(default = "Compare::bothway_default")]
    pub bothway: bool,
}

impl Default for Compare {
    fn default() -> Self {
        Self {
            source: vec![SourceInstance::default()],
            target: RedisInstance::default(),
            batch_size: 10,
            threads: 1,
            compare_threads: 1,
            ttl_diff: 1,
            compare_times: 1,
            compare_interval: 1,
            report: false,
            scenario: ScenarioType::Single2single,
            bothway: false,
        }
    }
}

impl Compare {
    fn source_default() -> Vec<SourceInstance> {
        vec![SourceInstance::default()]
    }
    fn target_default() -> RedisInstance {
        RedisInstance::default()
    }
    fn batch_size_default() -> usize {
        10
    }
    fn threads_default() -> usize {
        1
    }
    fn compare_threads_default() -> usize {
        1
    }
    fn ttl_diff_default() -> usize {
        2
    }
    fn compare_times_default() -> u32 {
        1
    }
    fn compare_interval_default() -> u32 {
        1
    }
    fn report_default() -> bool {
        false
    }
    fn scenario_default() -> ScenarioType {
        ScenarioType::Single2single
    }
    fn bothway_default() -> bool {
        false
    }

    pub fn exec(&self) {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .map_err(|e| {
                log::error!("{}", e);
                return;
            })
            .unwrap();

        pool.scope(move |p| {
            // 正向校验
            for si in &self.source {
                let dbmapper = si.dbmapper.clone();
                for (s_db, t_db) in dbmapper {
                    p.spawn(move |_| {
                        // 获取源端 clients 列表，
                        // 集群实例转换为单实例
                        let source_clients: Vec<redis::Client> = match si.instance.instance_type {
                            InstanceType::Single => {
                                let mut vec_client = vec![];
                                let c = redis::Client::open(si.instance.urls[0].as_str())
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        return;
                                    })
                                    .unwrap();
                                vec_client.push(c);
                                vec_client
                            }
                            InstanceType::Cluster => {
                                let mut vec_client = vec![];
                                for url in si.instance.urls.clone() {
                                    if !si.instance.password.is_empty() {
                                        let vec_url = url.split(r#"//"#).collect::<Vec<&str>>();
                                        let url_single = vec_url[0].to_string()
                                            + "//"
                                            + ":"
                                            + &si.instance.password
                                            + "@"
                                            + vec_url[1];

                                        let c = redis::Client::open(url_single);
                                        if let Err(e) = c {
                                            log::error!("{}", e);
                                            continue;
                                        }
                                        vec_client.push(c.unwrap());
                                    } else {
                                        let c = redis::Client::open(url)
                                            .map_err(|e| {
                                                log::error!("{}", e);
                                                return;
                                            })
                                            .unwrap();
                                        vec_client.push(c);
                                    }
                                }
                                vec_client
                            }
                        };

                        let target_client = match self.target.instance_type {
                            InstanceType::Single => {
                                let c = redis::Client::open(self.target.urls[0].as_str())
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        return;
                                    })
                                    .unwrap();
                                RedisClient::Single(c)
                            }
                            InstanceType::Cluster => {
                                let mut cb = ClusterClientBuilder::new(self.target.urls.clone());
                                if !self.target.password.is_empty() {
                                    cb = cb.password(self.target.password.clone());
                                }
                                let c = cb
                                    .open()
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        return;
                                    })
                                    .unwrap();
                                RedisClient::Cluster(c)
                            }
                        };
                        let pool_compare = rayon::ThreadPoolBuilder::new()
                            .num_threads(self.threads)
                            .build()
                            .map_err(|e| {
                                log::error!("{}", e);
                                return;
                            })
                            .unwrap();

                        pool_compare.scope(|pc| {
                            for s_client in source_clients {
                                let begin = Instant::now();
                                let s_scan_conn_r = s_client.get_connection();
                                if let Err(e) = s_scan_conn_r {
                                    log::error!("{}", e);
                                    continue;
                                }
                                let mut s_scan_conn = s_scan_conn_r.unwrap();

                                let redis_cmd_select = redis::cmd("select");
                                let _ = s_scan_conn.req_command(redis_cmd_select.clone().arg(s_db));

                                let scan_iter = scan::<String>(&mut s_scan_conn);

                                match scan_iter {
                                    Ok(iter) => {
                                        let mut vec_keys: Vec<String> = Vec::new();
                                        let mut count = 0 as usize;
                                        for key in iter {
                                            if count < self.batch_size {
                                                vec_keys.push(key.clone());
                                                count += 1;
                                                continue;
                                            }
                                            let vk = vec_keys.clone();

                                            let mut s_conn = s_client
                                                .get_connection()
                                                .map_err(|e| {
                                                    log::error!("{}", e);
                                                    return;
                                                })
                                                .unwrap();

                                            let t_redis_conn = target_client
                                                .get_redis_connection()
                                                .map_err(|e| {
                                                    log::error!("{}", e);
                                                    return;
                                                })
                                                .unwrap();

                                            pc.spawn(move |_| {
                                                let cmd_select = redis::cmd("select");
                                                let mut tconn: Box<dyn ConnectionLike> =
                                                    t_redis_conn.get_dyn_connection();

                                                let _ = s_conn
                                                    .req_command(cmd_select.clone().arg(s_db));

                                                if let InstanceType::Single =
                                                    self.target.instance_type
                                                {
                                                    let _ = tconn
                                                        .req_command(cmd_select.clone().arg(t_db));
                                                }

                                                let mut rediskeys: Vec<RedisKey> = Vec::new();

                                                for key in vk {
                                                    let key_type =
                                                        key_type(key.clone(), &mut s_conn);
                                                    if let Ok(kt) = key_type {
                                                        let rediskey = RedisKey {
                                                            key: key.clone(),
                                                            key_type: kt,
                                                        };
                                                        rediskeys.push(rediskey);
                                                    }
                                                }

                                                let mut comparer = Comparer {
                                                    sconn: &mut s_conn,
                                                    tconn: tconn.as_mut(),
                                                    ttl_diff: self.ttl_diff,
                                                    batch: self.batch_size,
                                                };

                                                compare_rediskeys(&mut comparer, rediskeys);
                                            });

                                            count = 0;
                                            vec_keys.clear();
                                            vec_keys.push(key.clone());
                                            count += 1;
                                        }

                                        if !vec_keys.is_empty() {
                                            let mut s_conn = s_client
                                                .get_connection()
                                                .map_err(|e| {
                                                    log::error!("{}", e);
                                                    return;
                                                })
                                                .unwrap();
                                            let t_redis_conn = target_client
                                                .get_redis_connection()
                                                .map_err(|e| {
                                                    log::error!("{}", e);
                                                    return;
                                                })
                                                .unwrap();
                                            pc.spawn(move |_| {
                                                let cmd_select = redis::cmd("select");

                                                let mut tconn: Box<dyn ConnectionLike> =
                                                    t_redis_conn.get_dyn_connection();

                                                let _ = s_conn
                                                    .req_command(cmd_select.clone().arg(s_db));

                                                if let InstanceType::Single =
                                                    self.target.instance_type
                                                {
                                                    let _ = tconn
                                                        .req_command(cmd_select.clone().arg(t_db));
                                                }

                                                let mut rediskeys: Vec<RedisKey> = Vec::new();

                                                for key in vec_keys {
                                                    let key_type =
                                                        key_type(key.clone(), &mut s_conn);
                                                    if let Ok(kt) = key_type {
                                                        let rediskey = RedisKey {
                                                            key: key.clone(),
                                                            key_type: kt,
                                                        };
                                                        rediskeys.push(rediskey);
                                                    }
                                                }

                                                let mut comparer = Comparer {
                                                    sconn: &mut s_conn,
                                                    tconn: tconn.as_mut(),
                                                    ttl_diff: self.ttl_diff,
                                                    batch: self.batch_size,
                                                };

                                                compare_rediskeys(&mut comparer, rediskeys);
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("scan iter error:{}", e);
                                        return;
                                    }
                                }
                                println!(
                                    "source:{:?};target:{:?};elapsed:{:?}",
                                    s_client.get_connection_info(),
                                    self.target.clone(),
                                    begin.elapsed()
                                );
                            }
                        });
                    });
                }
            }

            // 反向校验
            if self.bothway {
                println!("执行反向校验")
            }
        });
    }
}

pub fn compare_rediskeys(comparer: &mut Comparer, keys_vec: Vec<RedisKey>) {
    for key in keys_vec {
        let r = comparer.compare_key(key.clone());
        if let Err(e) = r {
            log::error!("error:{};key:{:?}", e, key);

            // let _compareerror: CompareError = e.into();
        }
    }
}

// 比较key在多个db中是否存在，在任意一个库中存在则返回true，key在所有key中都不存在返回false
pub fn keys_exists_any_connections(
    mut conns: Vec<&mut dyn redis::ConnectionLike>,
    key: String,
) -> Result<bool> {
    let mut key_existes = false;
    for i in 0..conns.len() {
        let exists: bool = redis::cmd("exists").arg(key.clone()).query(conns[i])?;

        if exists {
            key_existes = exists;
        }
    }

    Ok(key_existes)
}

#[cfg(test)]
mod test {
    use crate::compare::rediscompare::Compare;

    //cargo test compare::rediscompare::test::test_compare --  --nocapture
    #[test]
    fn test_compare() {
        let file = "./a.yml";
        let compare = crate::util::from_yaml_file_to_struct::<Compare>(file);
        println!("{:?}", compare);
    }
}
