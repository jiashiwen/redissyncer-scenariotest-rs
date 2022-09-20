use std::collections::HashMap;

use super::compare_error::{CompareError, CompareErrorReason};
use super::comparekey::Comparer;
// use crate::compare::comparekey::ComparerMulti;
use crate::util::RedisKey;
use crate::util::{key_type, scan};
use anyhow::anyhow;
use anyhow::Result;
use futures::{pin_mut, StreamExt};
use redis::cluster::ClusterClientBuilder;
use redis::{ConnectionLike, Value};
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
pub struct Instance {
    #[serde(default = "Instance::urls_default")]
    pub urls: Vec<String>,
    #[serde(default = "Instance::password_default")]
    pub password: String,
    #[serde(default = "Instance::instance_type_default")]
    pub instance_type: InstanceType,
}

impl Default for Instance {
    fn default() -> Self {
        Self {
            urls: vec!["redis://127.0.0.1:6379".to_string()],
            password: "".to_string(),
            instance_type: InstanceType::Single,
        }
    }
}

impl Instance {
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
    instance: Instance,
    db: usize,
}

impl Default for DBInstance {
    fn default() -> Self {
        Self {
            instance: Instance::default(),
            db: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DBClient {
    client: redis::Client,
    db: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SourceInstance {
    #[serde(default = "SourceInstance::instance_default")]
    pub instance: Instance,
    #[serde(default = "SourceInstance::dbmapper_default")]
    pub dbmapper: HashMap<usize, usize>,
}

impl Default for SourceInstance {
    fn default() -> Self {
        let mut mapper = HashMap::new();
        mapper.insert(0, 0);
        Self {
            instance: Instance::default(),
            dbmapper: mapper,
        }
    }
}

impl SourceInstance {
    pub fn instance_default() -> Instance {
        Instance::default()
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
    pub target: Instance,
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
            target: Instance::default(),
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
    fn target_default() -> Instance {
        Instance::default()
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
        // 正向校验
        for si in &self.source {
            match si.instance.instance_type {
                InstanceType::Single => {
                    let c = redis::Client::open(si.instance.urls[0].as_str())
                        .map_err(|e| {
                            log::error!("{}", e);
                            return;
                        })
                        .unwrap();
                    println!("执行sourcesingle 校验")
                }
                InstanceType::Cluster => {
                    let c = ClusterClientBuilder::new(self.target.urls.clone())
                        .password(self.target.password.clone())
                        .open()
                        .map_err(|e| {
                            log::error!("{}", e);
                            return;
                        })
                        .unwrap();
                    println!("执行sourcecluseter 校验")
                }
            };
        }

        // 反向校验
    }

    pub fn exec_scenario(&self) {
        match self.scenario {
            ScenarioType::Single2single => {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(self.threads)
                    .build();
                if let Err(e) = pool {
                    log::error!("{}", e);
                    return;
                }
                let dbmapper = self.source[0].dbmapper.clone();

                pool.unwrap().scope(|s| {
                    if self.source.len() == 0 {
                        return;
                    }

                    for (k, v) in dbmapper {
                        let sclient = redis::Client::open(self.source[0].instance.urls[0].as_str())
                            .map_err(|e| {
                                log::error!("{}", e);
                                return;
                            })
                            .unwrap();

                        let tclient = redis::Client::open(self.target.urls[0].as_str())
                            .map_err(|e| {
                                log::error!("{}", e);
                                return;
                            })
                            .unwrap();

                        let scan_conn = sclient.get_connection();
                        if let Err(e) = scan_conn {
                            log::error!("{}", e);
                            return;
                        }

                        s.spawn(move |_| {
                            let pool_compare = rayon::ThreadPoolBuilder::new()
                                .num_threads(self.compare_threads)
                                .build();
                            if let Err(e) = pool_compare {
                                log::error!("{}", e);
                                return;
                            }

                            pool_compare.unwrap().scope(|s| {
                                let cmd_select = redis::cmd("select");
                                match scan_conn {
                                    Ok(mut conn) => {
                                        let _ = conn.req_command(cmd_select.clone().arg(k));
                                        let s_iter = scan::<String>(&mut conn);
                                        match s_iter {
                                            Ok(i) => {
                                                let mut vec_keys: Vec<String> = Vec::new();
                                                let mut count = 0 as usize;

                                                for item in i {
                                                    if count < self.batch_size {
                                                        vec_keys.push(item.clone());
                                                        count += 1;
                                                        continue;
                                                    }
                                                    let sconn = sclient.get_connection();
                                                    if let Err(e) = sconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    let tconn = tclient.get_connection();
                                                    if let Err(e) = tconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }

                                                    let vk = vec_keys.clone();
                                                    s.spawn(move |_| {
                                                        let cmd_select = redis::cmd("select");
                                                        let mut s = sconn.unwrap();
                                                        let _ = s
                                                            .req_command(cmd_select.clone().arg(k));
                                                        let mut t = tconn.unwrap();
                                                        let _ = t
                                                            .req_command(cmd_select.clone().arg(v));

                                                        let mut rediskeys: Vec<RedisKey> =
                                                            Vec::new();
                                                        for key in vk {
                                                            let key_type =
                                                                key_type(key.clone(), &mut s);
                                                            if let Ok(kt) = key_type {
                                                                let rediskey = RedisKey {
                                                                    key: key.clone(),
                                                                    key_type: kt,
                                                                };
                                                                rediskeys.push(rediskey);
                                                            }
                                                        }

                                                        let mut comparer = Comparer {
                                                            sconn: &mut s,
                                                            tconn: &mut t,
                                                            ttl_diff: self.ttl_diff,
                                                            batch: self.batch_size,
                                                        };
                                                        compare_rediskeys(&mut comparer, rediskeys);
                                                    });

                                                    count = 0;
                                                    vec_keys.clear();
                                                    vec_keys.push(item.clone());
                                                    count += 1;
                                                }

                                                if vec_keys.len() > 0 {
                                                    let sconn = sclient.get_connection();
                                                    if let Err(e) = sconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    let tconn = tclient.get_connection();
                                                    if let Err(e) = tconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    s.spawn(move |_| {
                                                        let cmd_select = redis::cmd("select");
                                                        let mut s = sconn.unwrap();
                                                        let _ = s
                                                            .req_command(cmd_select.clone().arg(k));
                                                        let mut t = tconn.unwrap();
                                                        let _ = t
                                                            .req_command(cmd_select.clone().arg(v));

                                                        let mut rediskeys: Vec<RedisKey> =
                                                            Vec::new();
                                                        for key in vec_keys.clone() {
                                                            let key_type =
                                                                key_type(key.clone(), &mut s);
                                                            if let Ok(kt) = key_type {
                                                                let rediskey = RedisKey {
                                                                    key: key.clone(),
                                                                    key_type: kt,
                                                                };
                                                                rediskeys.push(rediskey);
                                                            }
                                                        }

                                                        let mut comparer = Comparer {
                                                            sconn: &mut s,
                                                            tconn: &mut t,
                                                            ttl_diff: self.ttl_diff,
                                                            batch: self.batch_size,
                                                        };
                                                        compare_rediskeys(&mut comparer, rediskeys);
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
                            println!("compare {}: {} finished", k, v);
                        });
                    }
                });
            }

            ScenarioType::Multisingle2single => {
                println!("exec multisigle2single");

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(self.threads)
                    .build();
                if let Err(e) = pool {
                    log::error!("{}", e);
                    return;
                }

                pool.unwrap().scope(|ps| {
                    if self.source.len() == 0 {
                        return;
                    }

                    for si in self.source.iter() {
                        for (k, v) in si.dbmapper.clone() {
                            let sclient = redis::Client::open(si.instance.urls[0].as_str())
                                .map_err(|e| {
                                    log::error!("{}", e);
                                    return;
                                })
                                .unwrap();

                            let tclient = redis::Client::open(self.target.urls[0].as_str())
                                .map_err(|e| {
                                    log::error!("{}", e);
                                    return;
                                })
                                .unwrap();

                            let scan_conn = sclient.get_connection();
                            if let Err(e) = scan_conn {
                                log::error!("{}", e);
                                return;
                            }
                            ps.spawn(move |_| {
                                let pool_compare = rayon::ThreadPoolBuilder::new()
                                    .num_threads(self.compare_threads)
                                    .build();
                                if let Err(e) = pool_compare {
                                    log::error!("{}", e);
                                    return;
                                }

                                pool_compare.unwrap().scope(|s| {
                                    let cmd_select = redis::cmd("select");
                                    match scan_conn {
                                        Ok(mut conn) => {
                                            let _ = conn.req_command(cmd_select.clone().arg(k));
                                            let s_iter = scan::<String>(&mut conn);
                                            match s_iter {
                                                Ok(i) => {
                                                    let mut vec_keys: Vec<String> = Vec::new();
                                                    let mut count = 0 as usize;

                                                    for item in i {
                                                        if count < self.batch_size {
                                                            vec_keys.push(item.clone());
                                                            count += 1;
                                                            continue;
                                                        }
                                                        let sconn = sclient.get_connection();
                                                        if let Err(e) = sconn {
                                                            log::error!("{}", e);
                                                            return;
                                                        }
                                                        let tconn = tclient.get_connection();
                                                        if let Err(e) = tconn {
                                                            log::error!("{}", e);
                                                            return;
                                                        }

                                                        let vk = vec_keys.clone();
                                                        s.spawn(move |_| {
                                                            let cmd_select = redis::cmd("select");
                                                            let mut s = sconn.unwrap();
                                                            let _ = s.req_command(
                                                                cmd_select.clone().arg(k),
                                                            );
                                                            let mut t = tconn.unwrap();
                                                            let _ = t.req_command(
                                                                cmd_select.clone().arg(v),
                                                            );

                                                            let mut rediskeys: Vec<RedisKey> =
                                                                Vec::new();
                                                            for key in vk {
                                                                let key_type =
                                                                    key_type(key.clone(), &mut s);
                                                                if let Ok(kt) = key_type {
                                                                    let rediskey = RedisKey {
                                                                        key: key.clone(),
                                                                        key_type: kt,
                                                                    };
                                                                    rediskeys.push(rediskey);
                                                                }
                                                            }

                                                            let mut comparer = Comparer {
                                                                sconn: &mut s,
                                                                tconn: &mut t,
                                                                ttl_diff: self.ttl_diff,
                                                                batch: self.batch_size,
                                                            };
                                                            compare_rediskeys(
                                                                &mut comparer,
                                                                rediskeys,
                                                            );
                                                        });

                                                        count = 0;
                                                        vec_keys.clear();
                                                        vec_keys.push(item.clone());
                                                        count += 1;
                                                    }

                                                    if vec_keys.len() > 0 {
                                                        let sconn = sclient.get_connection();
                                                        if let Err(e) = sconn {
                                                            log::error!("{}", e);
                                                            return;
                                                        }
                                                        let tconn = tclient.get_connection();
                                                        if let Err(e) = tconn {
                                                            log::error!("{}", e);
                                                            return;
                                                        }
                                                        s.spawn(move |_| {
                                                            let cmd_select = redis::cmd("select");
                                                            let mut s = sconn.unwrap();
                                                            let _ = s.req_command(
                                                                cmd_select.clone().arg(k),
                                                            );
                                                            let mut t = tconn.unwrap();
                                                            let _ = t.req_command(
                                                                cmd_select.clone().arg(v),
                                                            );

                                                            let mut rediskeys: Vec<RedisKey> =
                                                                Vec::new();
                                                            for key in vec_keys.clone() {
                                                                let key_type =
                                                                    key_type(key.clone(), &mut s);
                                                                if let Ok(kt) = key_type {
                                                                    let rediskey = RedisKey {
                                                                        key: key.clone(),
                                                                        key_type: kt,
                                                                    };
                                                                    rediskeys.push(rediskey);
                                                                }
                                                            }

                                                            let mut comparer = Comparer {
                                                                sconn: &mut s,
                                                                tconn: &mut t,
                                                                ttl_diff: self.ttl_diff,
                                                                batch: self.batch_size,
                                                            };
                                                            compare_rediskeys(
                                                                &mut comparer,
                                                                rediskeys,
                                                            );
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
                            });
                        }
                    }

                    // 反向校验只校验target 中的 key 是否在某个source 中存在，无一source存在则报错，若在其中一个source中存在后续校验会在souce中进行
                    if self.bothway {
                        ps.spawn(move |_| {
                            // 生成target 到 source 的反向索引
                            let mut dbmapper_reverse: HashMap<usize, Vec<DBInstance>> =
                                HashMap::new();
                            for s in self.source.iter() {
                                for (k, v) in s.dbmapper.iter() {
                                    let vec_dbinstance = dbmapper_reverse.get(v);
                                    if let Some(vec) = vec_dbinstance {
                                        let dbinstacne = DBInstance {
                                            instance: s.instance.clone(),
                                            db: k.clone(),
                                        };
                                        let mut db_vec = vec.clone();
                                        db_vec.push(dbinstacne);
                                        dbmapper_reverse.insert(v.clone(), db_vec);
                                    } else {
                                        let dbinstacne = DBInstance {
                                            instance: s.instance.clone(),
                                            db: k.clone(),
                                        };
                                        let db_vec = vec![dbinstacne];
                                        dbmapper_reverse.insert(v.clone(), db_vec);
                                    }
                                }
                            }

                            let tclient = redis::Client::open(self.target.urls[0].as_str())
                                .map_err(|e| {
                                    log::error!("{}", e);
                                    return;
                                })
                                .unwrap();

                            for (k, v) in dbmapper_reverse {
                                let pool_compare = rayon::ThreadPoolBuilder::new()
                                    .num_threads(self.compare_threads)
                                    .build();
                                if let Err(e) = pool_compare {
                                    log::error!("{}", e);
                                    return;
                                }
                                let scan_conn = tclient.get_connection();
                                if let Err(e) = scan_conn {
                                    log::error!("{}", e);
                                    return;
                                }

                                pool_compare.unwrap().scope(|s| match scan_conn {
                                    Ok(mut conn) => {
                                        let _ = conn.req_command(redis::cmd("select").arg(k));
                                        let s_iter = scan::<String>(&mut conn);

                                        match s_iter {
                                            Ok(i) => {
                                                let mut vec_keys: Vec<String> = Vec::new();
                                                let mut count = 0 as usize;
                                                for item in i {
                                                    if count < self.batch_size {
                                                        vec_keys.push(item.clone());
                                                        count += 1;
                                                        continue;
                                                    }
                                                    let sconn = tclient.get_connection();
                                                    if let Err(e) = sconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    let tconn = tclient.get_connection();
                                                    if let Err(e) = tconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }

                                                    let vk = vec_keys.clone();
                                                    let mut vec_client: Vec<DBClient> = vec![];

                                                    for dbinstance in v.clone() {
                                                        let client = redis::Client::open(
                                                            dbinstance.instance.urls[0].as_str(),
                                                        )
                                                        .map_err(|e| {
                                                            log::error!("{}", e);
                                                            return;
                                                        })
                                                        .unwrap();
                                                        let dbclient = DBClient {
                                                            client,
                                                            db: dbinstance.db,
                                                        };
                                                        vec_client.push(dbclient);
                                                    }

                                                    s.spawn(move |_| {
                                                        // let mut tconns: Vec<&mut dyn redis::ConnectionLike> = vec![];
                                                        let mut tconns = vec_client
                                                            .iter()
                                                            .map(|element| {
                                                                let mut conn = element
                                                                    .to_owned()
                                                                    .client
                                                                    .get_connection()
                                                                    .map_err(|e| {
                                                                        log::error!("{}", e);
                                                                        return;
                                                                    })
                                                                    .unwrap();
                                                                let _ = conn.req_command(
                                                                    redis::cmd("select")
                                                                        .arg(element.db),
                                                                );
                                                                conn
                                                            })
                                                            .collect::<Vec<redis::Connection>>();

                                                        for key in vk {
                                                            for i in 0..tconns.len() {
                                                                let exists: bool =
                                                                    redis::cmd("exists")
                                                                        .arg(key.clone())
                                                                        .query(&mut tconns[i])
                                                                        .map_err(|e| {
                                                                            log::error!("{}", e);
                                                                            return;
                                                                        })
                                                                        .unwrap();
                                                                println!("{}", exists);
                                                            }
                                                        }
                                                    });

                                                    count = 0;
                                                    vec_keys.clear();
                                                    vec_keys.push(item.clone());
                                                    count += 1;
                                                }

                                                if vec_keys.len() > 0 {
                                                    let mut vec_client: Vec<DBClient> = vec![];
                                                    for dbinstance in v.clone() {
                                                        let client = redis::Client::open(
                                                            dbinstance.instance.urls[0].as_str(),
                                                        )
                                                        .map_err(|e| {
                                                            log::error!("{}", e);
                                                            return;
                                                        })
                                                        .unwrap();
                                                        let dbclient = DBClient {
                                                            client,
                                                            db: dbinstance.db,
                                                        };
                                                        vec_client.push(dbclient);
                                                    }
                                                    s.spawn(move |_| {
                                                        let mut tconns = vec_client
                                                            .iter()
                                                            .map(|element| {
                                                                let mut conn = element
                                                                    .to_owned()
                                                                    .client
                                                                    .get_connection()
                                                                    .map_err(|e| {
                                                                        log::error!("{}", e);
                                                                        return;
                                                                    })
                                                                    .unwrap();
                                                                let _ = conn.req_command(
                                                                    redis::cmd("select")
                                                                        .arg(element.db),
                                                                );
                                                                conn
                                                            })
                                                            .collect::<Vec<redis::Connection>>();
                                                        for key in vec_keys {
                                                            for i in 0..tconns.len() {
                                                                let exists: bool =
                                                                    redis::cmd("exists")
                                                                        .arg(key.clone())
                                                                        .query(&mut tconns[i])
                                                                        .map_err(|e| {
                                                                            log::error!("{}", e);
                                                                            return;
                                                                        })
                                                                        .unwrap();
                                                                println!("{}", exists);
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
                                });

                                println!("反向校验，{}", k);
                            }
                        });
                    }
                });
            }

            ScenarioType::Single2cluster => {
                println!("exec single2cluster");
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(self.threads)
                    .build();
                if let Err(e) = pool {
                    log::error!("{}", e);
                    return;
                }
                let dbmapper = self.source[0].dbmapper.clone();
                pool.unwrap().scope(|s| {
                    if self.source.len() == 0 {
                        return;
                    }

                    for (k, v) in dbmapper {
                        if v != 0 {
                            return;
                        }
                        let sclient = redis::Client::open(self.source[0].instance.urls[0].as_str())
                            .map_err(|e| {
                                log::error!("{}", e);
                                return;
                            })
                            .unwrap();

                        let tclient = ClusterClientBuilder::new(self.target.urls.clone())
                            .password(self.target.password.clone())
                            .open()
                            .map_err(|e| {
                                log::error!("{}", e);
                                return;
                            })
                            .unwrap();

                        let scan_conn = sclient.get_connection();
                        if let Err(e) = scan_conn {
                            log::error!("{}", e);
                            return;
                        }

                        s.spawn(move |_| {
                            let pool_compare = rayon::ThreadPoolBuilder::new()
                                .num_threads(self.compare_threads)
                                .build();
                            if let Err(e) = pool_compare {
                                log::error!("{}", e);
                                return;
                            }

                            pool_compare.unwrap().scope(|s| {
                                let cmd_select = redis::cmd("select");
                                match scan_conn {
                                    Ok(mut conn) => {
                                        let _ = conn.req_command(cmd_select.clone().arg(k));
                                        let s_iter = scan::<String>(&mut conn);
                                        match s_iter {
                                            Ok(i) => {
                                                let mut vec_keys: Vec<String> = Vec::new();
                                                let mut count = 0 as usize;

                                                for item in i {
                                                    if count < self.batch_size {
                                                        vec_keys.push(item.clone());
                                                        count += 1;
                                                        continue;
                                                    }
                                                    let sconn = sclient.get_connection();
                                                    if let Err(e) = sconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    let tconn = tclient.get_connection();
                                                    if let Err(e) = tconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }

                                                    let vk = vec_keys.clone();
                                                    s.spawn(move |_| {
                                                        let cmd_select = redis::cmd("select");
                                                        let mut s = sconn.unwrap();
                                                        let _ = s
                                                            .req_command(cmd_select.clone().arg(k));
                                                        let mut t = tconn.unwrap();
                                                        let _ = t
                                                            .req_command(cmd_select.clone().arg(v));

                                                        let mut rediskeys: Vec<RedisKey> =
                                                            Vec::new();
                                                        for key in vk {
                                                            let key_type =
                                                                key_type(key.clone(), &mut s);
                                                            if let Ok(kt) = key_type {
                                                                let rediskey = RedisKey {
                                                                    key: key.clone(),
                                                                    key_type: kt,
                                                                };
                                                                rediskeys.push(rediskey);
                                                            }
                                                        }

                                                        let mut comparer = Comparer {
                                                            sconn: &mut s,
                                                            tconn: &mut t,
                                                            ttl_diff: self.ttl_diff,
                                                            batch: self.batch_size,
                                                        };
                                                        compare_rediskeys(&mut comparer, rediskeys);
                                                    });

                                                    count = 0;
                                                    vec_keys.clear();
                                                    vec_keys.push(item.clone());
                                                    count += 1;
                                                }

                                                if vec_keys.len() > 0 {
                                                    let sconn = sclient.get_connection();
                                                    if let Err(e) = sconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    let tconn = tclient.get_connection();
                                                    if let Err(e) = tconn {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                    s.spawn(move |_| {
                                                        let cmd_select = redis::cmd("select");
                                                        let mut s = sconn.unwrap();
                                                        let _ = s
                                                            .req_command(cmd_select.clone().arg(k));
                                                        let mut t = tconn.unwrap();
                                                        let _ = t
                                                            .req_command(cmd_select.clone().arg(v));

                                                        let mut rediskeys: Vec<RedisKey> =
                                                            Vec::new();
                                                        for key in vec_keys.clone() {
                                                            let key_type =
                                                                key_type(key.clone(), &mut s);
                                                            if let Ok(kt) = key_type {
                                                                let rediskey = RedisKey {
                                                                    key: key.clone(),
                                                                    key_type: kt,
                                                                };
                                                                rediskeys.push(rediskey);
                                                            }
                                                        }

                                                        let mut comparer = Comparer {
                                                            sconn: &mut s,
                                                            tconn: &mut t,
                                                            ttl_diff: self.ttl_diff,
                                                            batch: self.batch_size,
                                                        };
                                                        compare_rediskeys(&mut comparer, rediskeys);
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
                            println!("compare {}: {} finished", k, v);
                        });
                    }
                });
            }
            ScenarioType::Cluster2cluster => {
                println!("exec cluster2cluster");
            }

            ScenarioType::Multisingle2cluster => {
                println!("exec multisingle2cluster")
            }
        }
    }
}

// #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
// pub struct RedisCompare {
//     pub surls: Vec<String>,
//     pub turls: Vec<String>,
//     pub batch_size: usize,
//     pub threads: usize,
//     pub ttl_diff: usize,
//     pub compare_times: u32,
//     pub compare_interval: u32,
//     pub report: bool,
//     pub scenario: ScenarioType,
// }

// impl RedisCompare {
//     pub fn default() -> Self {
//         let mut source = vec![];
//         let mut target = vec![];
//         source.push("redis://127.0.0.1/".to_string());
//         target.push("redis://127.0.0.1/".to_string());

//         Self {
//             surls: source,
//             turls: target,
//             batch_size: 10,
//             threads: 2,
//             ttl_diff: 0,
//             compare_times: 1,
//             compare_interval: 1,
//             report: false,
//             scenario: ScenarioType::Single2single,
//         }
//     }

//     pub fn batch_size(&mut self, size: usize) {
//         self.batch_size = size;
//     }
//     pub fn threads(&mut self, threads: usize) {
//         self.threads = threads;
//     }
//     pub fn ttl_diff(&mut self, diff: usize) {
//         self.ttl_diff = diff;
//     }
//     pub fn compare_times(&mut self, times: u32) {
//         self.compare_times = times;
//     }
//     pub fn compare_interval(&mut self, interval: u32) {
//         self.compare_interval = interval;
//     }
//     pub fn report(&mut self, report: bool) {
//         self.report = report;
//     }

//     pub fn exec(&self) {
//         match self.scenario {
//             ScenarioType::Single2single => {
//                 self.exec_single2single();
//             }
//             ScenarioType::Single2cluster => self.exec_single2cluster(),
//             ScenarioType::Cluster2cluster => self.exec_cluster2cluster(),
//             ScenarioType::Multisingle2single => self.exec_multisingle2single(),
//             ScenarioType::Multisingle2cluster => self.exec_multisingle2cluster(),
//         }
//     }
// }

// impl RedisCompare {
//     pub fn exec_single2single(&self) -> Result<()> {
//         println!("exec single2single");
//         let pool = rayon::ThreadPoolBuilder::new()
//             .num_threads(self.threads)
//             .build()
//             .map_err(|err| anyhow!("{}", err.to_string()))?;

//         pool.scope(|s| {
//             let sclient = redis::Client::open(self.surls[0].as_str());
//             if let Err(e) = sclient {
//                 log::error!("{}", e);
//                 return;
//             }

//             let tclient = redis::Client::open(self.turls[0].as_str());
//             if let Err(e) = tclient {
//                 log::error!("{}", e);
//                 return;
//             }

//             let scan_conn = sclient.as_ref().unwrap().get_connection();
//             match scan_conn {
//                 Ok(mut conn) => {
//                     let s_iter = scan::<String>(&mut conn);
//                     match s_iter {
//                         Ok(i) => {
//                             let mut vec_key: Vec<RedisKey> = Vec::new();
//                             let mut count = 0 as usize;

//                             let mut keytype_conn =
//                                 sclient.as_ref().unwrap().get_connection().unwrap();

//                             for item in i {
//                                 if count < self.batch_size {
//                                     let key = key_type(item.clone(), &mut keytype_conn);
//                                     if let Ok(keytype) = key {
//                                         let rediskey = RedisKey {
//                                             key: item.clone(),
//                                             key_type: keytype,
//                                         };
//                                         vec_key.push(rediskey);
//                                     }
//                                     count += 1;
//                                     continue;
//                                 }
//                                 let sconn = sclient.as_ref().unwrap().get_connection();
//                                 if let Err(e) = sconn {
//                                     log::error!("{}", e);
//                                     return;
//                                 }
//                                 let tconn = tclient.as_ref().unwrap().get_connection();
//                                 if let Err(e) = tconn {
//                                     log::error!("{}", e);
//                                     return;
//                                 }
//                                 let vk = vec_key.clone();
//                                 s.spawn(move |_| {
//                                     let mut comparer = Comparer {
//                                         sconn: &mut sconn.unwrap(),
//                                         tconn: &mut tconn.unwrap(),
//                                         ttl_diff: self.ttl_diff,
//                                         batch: self.batch_size,
//                                     };
//                                     compare_rediskeys(&mut comparer, vk);
//                                 });

//                                 count = 0;
//                                 vec_key.clear();
//                                 let key = key_type(item.clone(), &mut keytype_conn);
//                                 if let Ok(keytype) = key {
//                                     let rediskey = RedisKey {
//                                         key: item.clone(),
//                                         key_type: keytype,
//                                     };
//                                     vec_key.push(rediskey);
//                                 }
//                                 count += 1;
//                             }

//                             if vec_key.len() > 0 {
//                                 let sconn = sclient.as_ref().unwrap().get_connection();
//                                 if let Err(e) = sconn {
//                                     log::error!("{}", e);
//                                     return;
//                                 }
//                                 let tconn = tclient.as_ref().unwrap().get_connection();
//                                 if let Err(e) = tconn {
//                                     log::error!("{}", e);
//                                     return;
//                                 }
//                                 s.spawn(move |_| {
//                                     let mut comparer = Comparer {
//                                         sconn: &mut sconn.unwrap(),
//                                         tconn: &mut tconn.unwrap(),
//                                         ttl_diff: self.ttl_diff,
//                                         batch: self.batch_size,
//                                     };
//                                     compare_rediskeys(&mut comparer, vec_key.clone());
//                                 });
//                             }
//                         }
//                         Err(e) => {
//                             log::error!("{}", e);
//                             return;
//                         }
//                     }
//                 }
//                 Err(e) => {
//                     log::error!("{}", e);
//                     return;
//                 }
//             }
//         });

//         Ok(())
//     }
//     pub fn exec_single2cluster(&self) {
//         println!("exec single2cluster");
//     }
//     pub fn exec_cluster2cluster(&self) {
//         println!("exec cluster2cluster");
//     }
//     pub fn exec_multisingle2single(&self) {
//         println!("exec multisingle2single");
//     }
//     pub fn exec_multisingle2cluster(&self) {
//         println!("exec multisingle2cluster");
//     }
// }

pub fn compare_rediskeys(comparer: &mut Comparer, keys_vec: Vec<RedisKey>) {
    for key in keys_vec {
        let r = comparer.compare_key(key);
        if let Err(e) = r {
            log::error!("{}", e);
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

    // if !key_existes {
    //     let v = Value::Data(Vec::<u8>::from(key.clone()));
    //     let reason = CompareErrorReason {
    //         key_name: key.clone(),
    //         source: Some(v),
    //         target: None,
    //     };
    //     return Err(Error::from(CompareError::from_reason(
    //         reason,
    //         CompareErrorType::ExistsErr,
    //     )));
    // }
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
