use super::comparekey::Comparer;
use crate::util::{key_type, rand_string, scan};
use crate::util::{RedisClient, RedisKey};
use anyhow::Result;
use chrono::prelude::Local;
use redis::cluster::ClusterClientBuilder;
use redis::ConnectionLike;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, create_dir, remove_dir_all, File, OpenOptions};
use std::io::{LineWriter, Read, Write};

use std::time::Instant;
use std::vec;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ScenarioType {
    Single2single,
    Single2cluster,
    Cluster2cluster,
    Multisingle2single,
    Multisingle2cluster,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareFailKeys {
    pub source_instance: RedisInstance,
    pub target_instance: RedisInstance,
    pub source_db: usize,
    pub target_db: usize,
    pub keys: Vec<RedisKey>,
    // 是否为反向校验
    pub reverse: bool,
}

impl CompareFailKeys {
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    pub fn write_to_file(&self, path: &str) -> Result<()> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        write_result_file(path, &buf)?;
        Ok(())
    }

    // 正向校验
    pub fn compare(&self) {}

    // 反向校验
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum InstanceType {
    Single,
    Cluster,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Clone)]
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

    pub fn to_single_redis_clients(&self) -> Result<Vec<redis::Client>> {
        return match self.instance_type {
            InstanceType::Single => {
                let mut vec_client = vec![];
                let cl = redis::Client::open(self.urls[0].as_str())?;
                vec_client.push(cl);
                Ok(vec_client)
            }
            InstanceType::Cluster => {
                let mut vec_client = vec![];
                for url in self.urls.clone() {
                    if !self.password.is_empty() {
                        let url_split_rs = url.split(r#"//"#).collect::<Vec<&str>>();
                        let url_single = url_split_rs[0].to_string()
                            + "//"
                            + ":"
                            + &self.password
                            + "@"
                            + url_split_rs[1];

                        let cl = redis::Client::open(url_single)?;
                        vec_client.push(cl);
                    } else {
                        let cl = redis::Client::open(url)?;
                        vec_client.push(cl);
                    }
                }
                Ok(vec_client)
            }
        };
    }

    pub fn to_redis_client(&self) -> Result<RedisClient> {
        return match self.instance_type {
            InstanceType::Single => {
                let cl = redis::Client::open(self.urls[0].as_str())?;
                Ok(RedisClient::Single(cl))
            }
            InstanceType::Cluster => {
                let mut cb = ClusterClientBuilder::new(self.urls.clone());
                if !self.password.is_empty() {
                    cb = cb.password(self.password.clone());
                }
                let cl = cb
                    .open()
                    .map_err(|e| {
                        log::error!("{}", e);
                        return;
                    })
                    .unwrap();
                Ok(RedisClient::Cluster(cl))
            }
        };
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Clone)]
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

impl DBInstance {
    pub fn to_single_client_db_vec(&self) -> Result<Vec<DBClient>> {
        let db_clients: Vec<DBClient> = match self.instance.instance_type {
            InstanceType::Single => {
                let mut vec: Vec<DBClient> = vec![];
                let client = self.instance.to_redis_client()?;
                let db_client = DBClient {
                    client,
                    db: self.db,
                };
                vec.push(db_client);
                vec
            }
            InstanceType::Cluster => {
                let mut vec: Vec<DBClient> = vec![];
                let clients = self.instance.to_single_redis_clients()?;
                for c in clients {
                    let db_client = DBClient {
                        client: RedisClient::Single(c),
                        db: self.db,
                    };
                    vec.push(db_client);
                }
                vec
            }
        };
        Ok(db_clients)
    }
}

pub struct RedisConnection {
    single: Option<redis::Connection>,
    cluster: Option<redis::cluster::ClusterConnection>,
}

#[derive(Clone)]
pub struct DBClient {
    client: RedisClient,
    db: usize,
}

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
    // 比较频率，当出现校验失败的key时循环比较的次数
    #[serde(default = "Compare::frequency_default")]
    pub frequency: usize,
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
            frequency: 1,
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

    fn frequency_default() -> usize {
        1
    }

    pub fn exec(&self) {
        // 首次校验
        // 删除中间文件目录
        let _ = remove_result_dir();
        // 生成中间文件目录

        let mut current_dir = match create_result_dir() {
            Ok(s) => s,
            Err(e) => {
                log::error!("{}", e.to_string());
                return;
            }
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .map_err(|e| {
                log::error!("{}", e);
                return;
            })
            .unwrap();

        let current_dir_move = current_dir.clone();
        pool.scope(move |p| {
            // 正向校验
            for si in &self.source {
                let dbmapper = si.dbmapper.clone();
                for (s_db, t_db) in dbmapper {
                    let cd = current_dir_move.clone();
                    p.spawn(move |_| {
                        let begin = Instant::now();
                        // 获取源端 clients 列表，
                        // 集群模式不支持scan，顾将源集群实例转换为单实例，分实例进行校验
                        let source_clients_rs = si.instance.to_single_redis_clients();
                        let source_clients = match source_clients_rs {
                            Ok(v) => v,
                            Err(e) => {
                                log::error!("{}", e);
                                return;
                            }
                        };

                        let target_client_rs = self.target.to_redis_client();
                        let target_client = match target_client_rs {
                            Ok(tc) => tc,
                            Err(e) => {
                                log::error!("{}", e);
                                return;
                            }
                        };

                        let pool_compare_rs = rayon::ThreadPoolBuilder::new()
                            .num_threads(self.threads)
                            .build();
                        let pool_compare = match pool_compare_rs {
                            Ok(p) => p,
                            Err(e) => {
                                log::error!("{}", e);
                                return;
                            }
                        };

                        pool_compare.scope(|pc| {
                            for s_client in source_clients {
                                let s_scan_conn_rs = s_client.get_connection();
                                let mut s_scan_conn = match s_scan_conn_rs {
                                    Ok(scan) => scan,
                                    Err(e) => {
                                        log::error!("{}", e);
                                        continue;
                                    }
                                };

                                let redis_cmd_select = redis::cmd("select");
                                if let Err(e) =
                                    s_scan_conn.req_command(redis_cmd_select.clone().arg(s_db))
                                {
                                    log::error!("{}", e);
                                    continue;
                                };

                                let scan_iter_rs = scan::<String>(&mut s_scan_conn);

                                let scan_iter = match scan_iter_rs {
                                    Ok(iter) => iter,
                                    Err(e) => {
                                        log::error!("{}", e);
                                        continue;
                                    }
                                };

                                let mut vec_keys: Vec<String> = Vec::new();
                                let mut count = 0 as usize;
                                for key in scan_iter {
                                    if count < self.batch_size {
                                        vec_keys.push(key.clone());
                                        count += 1;
                                        continue;
                                    }
                                    let vk = vec_keys.clone();

                                    let s_conn_rs = s_client.get_connection();
                                    let mut s_conn = match s_conn_rs {
                                        Ok(c) => c,
                                        Err(e) => {
                                            log::error!("{}", e);
                                            break;
                                        }
                                    };

                                    let t_redis_conn_rs = target_client.get_redis_connection();
                                    let t_redis_conn = match t_redis_conn_rs {
                                        Ok(tc) => tc,
                                        Err(e) => {
                                            log::error!("{}", e);
                                            break;
                                        }
                                    };
                                    let c_dir = cd.clone();
                                    pc.spawn(move |_| {
                                        let cmd_select = redis::cmd("select");
                                        let mut tconn: Box<dyn ConnectionLike> =
                                            t_redis_conn.get_dyn_connection();

                                        if let Err(e) =
                                            s_conn.req_command(cmd_select.clone().arg(s_db))
                                        {
                                            log::error!("{}", e);
                                            return;
                                        };

                                        if let InstanceType::Single = self.target.instance_type {
                                            if let Err(e) =
                                                tconn.req_command(cmd_select.clone().arg(t_db))
                                            {
                                                log::error!("{}", e);
                                                return;
                                            };
                                        }

                                        let mut rediskeys: Vec<RedisKey> = Vec::new();

                                        for key in vk {
                                            let key_type = key_type(key.clone(), &mut s_conn);
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

                                        let err_keys = compare_rediskeys(&mut comparer, rediskeys);
                                        if !err_keys.is_empty() {
                                            let cfk = CompareFailKeys {
                                                source_instance: si.instance.clone(),
                                                target_instance: self.target.clone(),
                                                source_db: comparer.sconn.get_db() as usize,
                                                target_db: comparer.tconn.get_db() as usize,
                                                keys: err_keys,
                                                reverse: false,
                                            };
                                            if let Err(e) = cfk.write_to_file(&c_dir) {
                                                log::error!("{}", e);
                                            };
                                        }
                                    });

                                    count = 0;
                                    vec_keys.clear();
                                    vec_keys.push(key.clone());
                                    count += 1;
                                }

                                if !vec_keys.is_empty() {
                                    let s_conn_rs = s_client.get_connection();
                                    let mut s_conn = match s_conn_rs {
                                        Ok(c) => c,
                                        Err(e) => {
                                            log::error!("{}", e);
                                            continue;
                                        }
                                    };
                                    let t_redis_conn_rs = target_client.get_redis_connection();
                                    let t_redis_conn = match t_redis_conn_rs {
                                        Ok(tc) => tc,
                                        Err(e) => {
                                            log::error!("{}", e);
                                            continue;
                                        }
                                    };
                                    let c_dir = cd.clone();
                                    pc.spawn(move |_| {
                                        let cmd_select = redis::cmd("select");
                                        let mut tconn: Box<dyn ConnectionLike> =
                                            t_redis_conn.get_dyn_connection();

                                        if let Err(e) =
                                            s_conn.req_command(cmd_select.clone().arg(s_db))
                                        {
                                            log::error!("{}", e);
                                            return;
                                        };

                                        if let InstanceType::Single = self.target.instance_type {
                                            if let Err(e) =
                                                tconn.req_command(cmd_select.clone().arg(t_db))
                                            {
                                                log::error!("{}", e);
                                                return;
                                            };
                                        }

                                        let mut rediskeys: Vec<RedisKey> = Vec::new();

                                        for key in vec_keys {
                                            let key_type = key_type(key.clone(), &mut s_conn);
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

                                        let err_keys = compare_rediskeys(&mut comparer, rediskeys);
                                        if !err_keys.is_empty() {
                                            let cfk = CompareFailKeys {
                                                source_instance: si.instance.clone(),
                                                target_instance: self.target.clone(),
                                                source_db: comparer.sconn.get_db() as usize,
                                                target_db: comparer.tconn.get_db() as usize,
                                                keys: err_keys,
                                                reverse: false,
                                            };

                                            if let Err(e) = cfk.write_to_file(&c_dir) {
                                                log::error!("{}", e);
                                            };
                                        }
                                    });
                                }
                            }
                        });
                        log::info!(
                            "source:{:?};target:{:?};elapsed:{:?}",
                            si.instance.clone(),
                            self.target.clone(),
                            begin.elapsed()
                        );
                    });
                }
            }

            // 反向校验
            if self.bothway {
                // 反向校验只校验target中存在但source中不存在的数据
                println!("执行反向校验");

                // Todo
                // 获取 source 列表，生成souce client vec，其中将集群转换为单实例
                // 生成 taget client vec，其中将集群转换为单实例写入vec
                // 生成 source 与 target 的对应关系 hash<t_conn,Vec<s_conn>>,建立 target to source 的反向索引
                let map = self.dbinstance_map();
                for (t, s) in map {
                    let t_db_clients_rs = t.to_single_client_db_vec();
                    let t_db_clients = match t_db_clients_rs {
                        Ok(tdbc) => tdbc,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };

                    match t.instance.instance_type {
                        InstanceType::Single => {
                            let t_client_rs = redis::Client::open(self.target.urls[0].as_str());
                            let t_client = match t_client_rs {
                                Ok(tc) => tc,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };

                            let t_scan_conn_rs = t_client.get_connection();
                            let mut t_scan_conn = match t_scan_conn_rs {
                                Ok(tsc) => tsc,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };

                            //选择要遍历的库
                            let cmd_select = redis::cmd("select");
                            if let Err(e) = t_scan_conn.req_command(cmd_select.clone().arg(t.db)) {
                                log::error!("{}", e);
                                return;
                            };
                            let s_db_client_rs = self.get_db_client(s);
                            let s_db_client = match s_db_client_rs {
                                Ok(sc) => sc,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };

                            let pool_compare_rs = rayon::ThreadPoolBuilder::new()
                                .num_threads(self.compare_threads)
                                .build();
                            let pool_compare = match pool_compare_rs {
                                Ok(pc) => pc,
                                Err(e) => {
                                    log::error!("{}", e);
                                    return;
                                }
                            };

                            pool_compare.scope(|pc| {
                                let t_scan_iter = scan::<String>(&mut t_scan_conn);
                                match t_scan_iter {
                                    Ok(iter) => {
                                        let mut vec_keys: Vec<String> = Vec::new();
                                        let mut count = 0 as usize;
                                        for key in iter {
                                            if count < self.batch_size {
                                                vec_keys.push(key.clone());
                                                count += 1;
                                                continue;
                                            }
                                            let sdbc = s_db_client.clone();
                                            let t_cl = t_client.clone();

                                            let vk = vec_keys.clone();

                                            pc.spawn(move |_| {
                                                let t_conn_rs = t_cl.get_connection();
                                                let mut t_conn = match t_conn_rs {
                                                    Ok(tc) => tc,
                                                    Err(e) => {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                };
                                                let cmd_select = redis::cmd("select");
                                                if let Err(e) =
                                                    t_conn.req_command(cmd_select.clone().arg(t.db))
                                                {
                                                    log::error!("{}", e);
                                                    return;
                                                };

                                                let s_conn_vec_rs = self.get_connection_vec(sdbc);
                                                let s_conn_vec = match s_conn_vec_rs {
                                                    Ok(scv) => scv,
                                                    Err(e) => {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                };
                                                let mut rediskeys = vec![];

                                                for key in vk.clone() {
                                                    let key_type =
                                                        key_type(key.clone(), &mut t_conn);
                                                    if let Ok(kt) = key_type {
                                                        let rediskey = RedisKey {
                                                            key: key.clone(),
                                                            key_type: kt,
                                                        };
                                                        rediskeys.push(rediskey);
                                                    }
                                                }
                                                let err_keys = keys_exists_any_connections(
                                                    s_conn_vec, rediskeys,
                                                );

                                                if !err_keys.is_empty() {
                                                    println!("err_keys:{:?}", err_keys);
                                                }
                                            });

                                            count = 0;
                                            vec_keys.clear();
                                            vec_keys.push(key.clone());
                                            count += 1;
                                        }

                                        if !vec_keys.is_empty() {
                                            let sdbc = s_db_client.clone();
                                            let t_cl = t_client.clone();
                                            pc.spawn(move |_| {
                                                let t_conn_rs = t_cl.get_connection();
                                                let mut t_conn = match t_conn_rs {
                                                    Ok(tc) => tc,
                                                    Err(e) => {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                };

                                                // 变更目标db
                                                let cmd_select = redis::cmd("select");
                                                if let Err(e) =
                                                    t_conn.req_command(cmd_select.clone().arg(t.db))
                                                {
                                                    log::error!("{}", e);
                                                    return;
                                                };

                                                let s_conn_vec_rs = self.get_connection_vec(sdbc);
                                                let s_conn_vec = match s_conn_vec_rs {
                                                    Ok(scv) => scv,
                                                    Err(e) => {
                                                        log::error!("{}", e);
                                                        return;
                                                    }
                                                };
                                                let mut rediskeys = vec![];

                                                for key in vec_keys {
                                                    let key_type =
                                                        key_type(key.clone(), &mut t_conn);
                                                    match key_type {
                                                        Ok(tp) => {
                                                            let rediskey = RedisKey {
                                                                key: key.clone(),
                                                                key_type: tp,
                                                            };
                                                            rediskeys.push(rediskey);
                                                        }
                                                        Err(e) => {
                                                            log::error!("{}", e);
                                                        }
                                                    }
                                                }

                                                let err_keys = keys_exists_any_connections(
                                                    s_conn_vec, rediskeys,
                                                );

                                                if !err_keys.is_empty() {
                                                    println!("err_keys:{:?}", err_keys);
                                                }
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("scan iter error:{}", e);
                                    }
                                }
                            });
                        }

                        InstanceType::Cluster => {}
                    }
                }
            }
        });

        // 执行循环校验
        if self.frequency > 1 {
            println!("执行循环校验");
            // walk_result_dir(&current_dir.clone());
        }
    }

    //正向校验
    fn compare_forward(&self) {}

    // 反向校验
    fn compare_reverse(&self) {}
}

impl Compare {
    // 返回target DBInstance 与 source DBInstance 的映射关系
    fn dbinstance_map(&self) -> HashMap<DBInstance, Vec<DBInstance>> {
        return match self.target.instance_type {
            InstanceType::Single => {
                let mut dbinstance_map = HashMap::new();
                let mut t_db_to_s_instance_map: HashMap<usize, Vec<DBInstance>> = HashMap::new();

                for s_instance in &self.source {
                    for (k, v) in &s_instance.dbmapper {
                        match t_db_to_s_instance_map.get(&v) {
                            Some(vec_dbinstance) => {
                                let s_dbinstance = DBInstance {
                                    instance: s_instance.instance.clone(),
                                    db: k.clone(),
                                };
                                let mut vec = vec_dbinstance.clone();
                                vec.push(s_dbinstance);
                                t_db_to_s_instance_map.insert(*v, vec);
                            }
                            None => {
                                let mut vec = vec![];
                                let s_dbinstance = DBInstance {
                                    instance: s_instance.instance.clone(),
                                    db: k.clone(),
                                };
                                vec.push(s_dbinstance);
                                t_db_to_s_instance_map.insert(*v, vec);
                            }
                        };
                    }
                }

                for (k, v) in t_db_to_s_instance_map {
                    let t_dbinstance = DBInstance {
                        instance: self.target.clone(),
                        db: k,
                    };

                    dbinstance_map.insert(t_dbinstance, v);
                }

                dbinstance_map
            }
            InstanceType::Cluster => {
                let mut dbinstance_map = HashMap::new();
                let mut source_dbinstances = vec![];
                let t_dbinstance = DBInstance {
                    instance: self.target.clone(),
                    db: 0,
                };
                for instance in &self.source {
                    for (k, v) in &instance.dbmapper {
                        if !v.eq(&0) {
                            log::error!(
                                "{}",
                                "target instance_type is cluster,no db number exists"
                            );
                            continue;
                        }

                        let s_dbinstance = DBInstance {
                            instance: instance.instance.clone(),
                            db: k.clone(),
                        };
                        source_dbinstances.push(s_dbinstance);
                    }
                }
                dbinstance_map.insert(t_dbinstance, source_dbinstances);
                dbinstance_map
            }
        };
    }

    // 根据Vec<DBInstance> 返回 Vec<DBClient>
    fn get_db_client(&self, source_db_instance_vec: Vec<DBInstance>) -> Result<Vec<DBClient>> {
        let mut vec: Vec<DBClient> = vec![];
        for i in source_db_instance_vec {
            match i.instance.instance_type {
                InstanceType::Single => {
                    let client = redis::Client::open(i.instance.urls[0].as_str())?;
                    let dbclient = DBClient {
                        client: RedisClient::Single(client),
                        db: i.db,
                    };
                    vec.push(dbclient);
                }
                InstanceType::Cluster => {
                    let mut cb = ClusterClientBuilder::new(i.instance.urls);
                    if !self.target.password.is_empty() {
                        cb = cb.password(self.target.password.clone());
                    }
                    let cluster_client = cb.open()?;

                    let dbclient = DBClient {
                        client: RedisClient::Cluster(cluster_client),
                        db: i.db,
                    };
                    vec.push(dbclient);
                }
            }
        }
        Ok(vec)
    }

    // 通过 Vec<DBClient> 获取 Vec<RedisConnection>
    fn get_connection_vec(&self, db_clients: Vec<DBClient>) -> Result<Vec<RedisConnection>> {
        let mut vec_conn: Vec<RedisConnection> = vec![];
        let cmd_select = redis::cmd("select");
        for dbc in db_clients {
            match dbc.client {
                RedisClient::Single(sc) => {
                    let mut conn = sc.get_connection()?;
                    conn.req_command(cmd_select.clone().arg(dbc.db))?;
                    let rc = RedisConnection {
                        single: Some(conn),
                        cluster: None,
                    };
                    vec_conn.push(rc);
                }
                RedisClient::Cluster(cc) => {
                    let conn = cc.get_connection()?;
                    let rc = RedisConnection {
                        single: None,
                        cluster: Some(conn),
                    };
                    vec_conn.push(rc);
                }
            }
        }
        Ok(vec_conn)
    }
}

// 创建结果文件目录
fn create_result_dir() -> Result<String> {
    let dt = Local::now();
    let timestampe = dt.timestamp();
    let dir = "cr_".to_owned() + &timestampe.to_string() + &"_".to_string() + &rand_string(4);
    create_dir(dir.clone())?;
    // ".current_compare" 记录结果目录名称，便于后续处理
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(".current_compare")?;
    file.write_all(dir.as_bytes())?;
    Ok(dir)
}

fn remove_result_dir() -> Result<()> {
    let dir_name = fs::read_to_string(".current_compare")?;
    remove_dir_all(dir_name)?;
    Ok(())
}

fn write_result_file(path: &str, buf: &Vec<u8>) -> Result<()> {
    let dt = Local::now();
    let timestampe = dt.timestamp_nanos();
    let file_name = path.to_owned() + "/" + &timestampe.to_string() + "_" + &rand_string(4) + ".cr";
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(file_name)?;
    file.write_all(buf)?;
    Ok(())
}

fn walk_result_dir(dir: &str) -> Result<()> {
    let mut vec = vec![];
    let entries = fs::read_dir(dir)?;
    for e in entries {
        if let Ok(de) = e {
            if let Ok(m) = de.metadata() {
                if m.is_file() {
                    vec.push(de.file_name());
                }
            }
        };
    }

    for fileanme in vec {
        if let Ok(name) = fileanme.into_string() {
            let path = dir.to_string().clone() + "/" + &name;
            let file = fs::File::open(path);
            match file {
                Ok(mut f) => {
                    let mut vec = vec![];
                    let _ = f.read_to_end(&mut vec);
                    let cfk = rmp_serde::from_slice::<CompareFailKeys>(&vec);
                    if let Ok(cl) = cfk {
                        println!("{:?}", cl);
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                }
            }
        };
    }

    Ok(())
}

// 返回校验不成功的key
pub fn compare_rediskeys(comparer: &mut Comparer, keys_vec: Vec<RedisKey>) -> Vec<RedisKey> {
    let mut vec_keys = vec![];
    for key in keys_vec {
        let r = comparer.compare_key(key.clone());
        if let Err(e) = r {
            vec_keys.push(key.clone());
            log::error!("error:{};key:{:?}", e, key);
        }
    }
    vec_keys
}

// 比较key在多个db中是否存在，在任意一个库中存在则返回true，key在所有key中都不存在返回false
// 返回 检查结果为false 的 RedisKey
pub fn keys_exists_any_connections(
    mut conns: Vec<RedisConnection>,
    keys: Vec<RedisKey>,
) -> Vec<RedisKey> {
    let mut vec_rediskeys: Vec<RedisKey> = vec![];
    for key in keys {
        let mut key_existes = false;
        for conn in &mut conns {
            if let Some(sc) = &mut conn.single {
                let exists_rs = redis::cmd("exists").arg(key.key.clone()).query::<bool>(sc);
                match exists_rs {
                    Ok(exists) => {
                        if exists {
                            key_existes = exists;
                        }
                    }
                    Err(_) => {}
                }
            }

            if let Some(cc) = &mut conn.cluster {
                let exists_rs = redis::cmd("exists").arg(key.key.clone()).query(cc);
                match exists_rs {
                    Ok(exists) => {
                        if exists {
                            key_existes = exists;
                        }
                    }
                    Err(_) => {}
                }
            }
        }

        if !key_existes {
            vec_rediskeys.push(key);
        }
    }
    vec_rediskeys
}

fn write_to_file(f: &mut LineWriter<File>, buf: &Vec<u8>) {
    f.write_all(buf);
}

#[cfg(test)]
mod test {
    use std::{
        fs::{File, OpenOptions},
        io::{LineWriter, Read, Write},
        sync::{Arc, Mutex},
    };

    use crate::{
        compare::{
            rediscompare::{write_to_file, Compare, CompareFailKeys},
            RedisInstance,
        },
        util::{RedisKey, RedisKeyType},
    };

    //cargo test compare::rediscompare::test::test_compare --  --nocapture
    #[test]
    fn test_compare() {
        let file = "./a.yml";
        let compare = crate::util::from_yaml_file_to_struct::<Compare>(file);
        println!("{:?}", compare);
    }

    //cargo test compare::rediscompare::test::test_CompareErrorKeys --  --nocapture
    #[test]
    fn test_CompareErrorKeys() {
        let _ = std::fs::remove_file("/tmp/write_one.bin");
        let mut vec_keys = vec![];

        for i in 0..10 {
            let redis_key = RedisKey {
                key: "key".to_string() + &i.to_string(),
                key_type: RedisKeyType::TypeString,
            };
            vec_keys.push(redis_key);
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("/tmp/write_one.bin")
            .unwrap();
        let mut lw = LineWriter::new(&file);
        for i in 0..5 {
            let keys = CompareFailKeys {
                source_instance: RedisInstance::default(),
                target_instance: RedisInstance::default(),
                source_db: i,
                target_db: i + 1,
                keys: vec_keys.clone(),
                reverse: false,
            };
            let mut r = keys.to_vec().unwrap();
            // 添加分隔符
            r.push(10);

            {
                let f = &mut lw;
                let buf = &r;
                f.write_all(buf);
            };
        }
    }
    //cargo test compare::rediscompare::test::test_CompareErrorKeys_multi_threads --  --nocapture
    #[test]
    fn test_CompareErrorKeys_multi_threads() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .map_err(|e| {
                log::error!("{}", e);
                return;
            })
            .unwrap();
        let _ = std::fs::remove_file("/tmp/write_one.bin");

        pool.scope(|s| {
            let mut vec_keys = vec![];

            for i in 0..10 {
                let redis_key = RedisKey {
                    key: "key".to_string() + &i.to_string(),
                    key_type: RedisKeyType::TypeString,
                };
                vec_keys.push(redis_key);
            }
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open("/tmp/write_one.bin")
                .unwrap();
            let lw = LineWriter::new(file);
            let lw_mutex = Mutex::new(lw);

            let lock = Arc::new(lw_mutex);
            for _ in 0..5 {
                let vk = vec_keys.clone();
                // let l = lw_mutex.get_mut();
                let l = Arc::clone(&lock);
                s.spawn(move |_| {
                    let mut f = l.lock().unwrap();
                    for i in 0..10 {
                        let keys = CompareFailKeys {
                            source_instance: RedisInstance::default(),
                            target_instance: RedisInstance::default(),
                            source_db: i,
                            target_db: i + 1,
                            keys: vk.clone(),
                            reverse: false,
                        };
                        let mut r = keys.to_vec().unwrap();
                        // 添加分隔符
                        r.push(10);
                        write_to_file(&mut f, &r);
                    }
                });
            }
        });
    }

    //cargo test compare::rediscompare::test::test_parse_CompareErrorKeys_file --  --nocapture
    #[test]
    fn test_parse_CompareErrorKeys_file() {
        let mut v: Vec<u8> = vec![];
        const BUFFER_LEN: usize = 32;
        // let mut buffer = [0u8; BUFFER_LEN];
        let mut read_file = File::open("/tmp/write_one.bin").unwrap();
        let mut count = 0;
        loop {
            let mut buffer = [0u8; BUFFER_LEN];
            let read_count = read_file.read(&mut buffer).unwrap();
            for i in 0..read_count {
                if buffer[i].eq(&10) {
                    let h = rmp_serde::from_slice::<CompareFailKeys>(&v).unwrap();
                    println!("{:?}", h);
                    count += 1;
                    println!("count is {}", count);
                    v.clear();
                } else {
                    v.push(buffer[i]);
                }
            }

            if read_count != BUFFER_LEN {
                break;
            }
        }
    }
}
