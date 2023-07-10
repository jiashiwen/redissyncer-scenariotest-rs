use crate::compare::{compare_from_file, CompareDB, CompareDBReverse};
use crate::util::RedisClient;
use crate::util::{rand_lettter_number_string, rand_string, RedisClientWithDB};
use anyhow::{anyhow, Result};
use chrono::prelude::Local;
use redis::cluster::ClusterClientBuilder;
use redis::RedisResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, create_dir, remove_dir_all, File, OpenOptions, ReadDir};
use std::io::{LineWriter, Read, Write};
use std::ops::Sub;
use std::str::FromStr;
use std::vec;

pub const COMPARE_STATUS_FILE_NAME: &str = ".compare_status";

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ScenarioType {
    Single2single,
    Single2cluster,
    Cluster2cluster,
    Multisingle2single,
    Multisingle2cluster,
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
    pub fn default_cluster() -> Self {
        Self {
            urls: vec![
                "redis://127.0.0.1:6379".to_string(),
                "redis://127.0.0.1:6380".to_string(),
                "redis://127.0.0.1:6381".to_string(),
            ],
            password: "".to_string(),
            instance_type: InstanceType::Cluster,
        }
    }

    pub fn urls_default() -> Vec<String> {
        vec!["redis://127.0.0.1:6379".to_string()]
    }
    pub fn password_default() -> String {
        "".to_string()
    }
    pub fn instance_type_default() -> InstanceType {
        InstanceType::Single
    }

    pub fn to_single_redis_clients(&self) -> RedisResult<Vec<redis::Client>> {
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

    pub fn to_redis_client(&self) -> RedisResult<RedisClient> {
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
                let cl = cb.open()?;
                Ok(RedisClient::Cluster(cl))
            }
        };
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Clone)]
pub struct RedisInstanceWithDB {
    pub instance: RedisInstance,
    pub db: usize,
}

impl Default for RedisInstanceWithDB {
    fn default() -> Self {
        Self {
            instance: RedisInstance::default(),
            db: 0,
        }
    }
}

impl RedisInstanceWithDB {
    pub fn to_single_redis_instance_with_db_vec(&self) -> Vec<RedisInstanceWithDB> {
        let instances: Vec<RedisInstanceWithDB> = match self.instance.instance_type {
            InstanceType::Single => {
                let mut vec: Vec<RedisInstanceWithDB> = vec![];
                vec.push(self.clone());
                vec
            }
            InstanceType::Cluster => {
                let mut vec: Vec<RedisInstanceWithDB> = vec![];
                for url in self.instance.urls.clone() {
                    if !self.instance.password.is_empty() {
                        let url_split_rs = url.split(r#"//"#).collect::<Vec<&str>>();
                        let url_single = url_split_rs[0].to_string()
                            + "//"
                            + ":"
                            + &self.instance.password
                            + "@"
                            + url_split_rs[1];

                        let redis_instance = RedisInstance {
                            urls: vec![url_single],
                            password: String::from(""),
                            instance_type: InstanceType::Single,
                        };
                        let instance = RedisInstanceWithDB {
                            instance: redis_instance,
                            db: self.db,
                        };
                        vec.push(instance);
                    } else {
                        let redis_instance = RedisInstance {
                            urls: vec![url],
                            password: String::from(""),
                            instance_type: InstanceType::Single,
                        };
                        let instance = RedisInstanceWithDB {
                            instance: redis_instance,
                            db: self.db,
                        };
                        vec.push(instance);
                    }
                }
                vec
            }
        };
        instances
    }

    pub fn to_redis_client_with_db(&self) -> RedisResult<RedisClientWithDB> {
        let client = self.instance.to_redis_client()?;
        let rcwb = RedisClientWithDB {
            client,
            db: self.db,
        };
        Ok(rcwb)
    }
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
        let mut compare_times_remainder = self.frequency;
        // 首次校验
        // 删除中间文件目录
        let _ = remove_result_dir();
        // 生成中间文件目录

        let current_dir = match create_result_dir() {
            Ok(s) => s,
            Err(e) => {
                log::error!("{}", e.to_string());
                return;
            }
        };
        // 获取source to target 对应关系
        let map_dbinstance_s_t = match self.map_dbinstance_source_to_target() {
            Ok(map) => map,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        let pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
        {
            Ok(p) => p,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        let current_dir_move = current_dir.clone();
        pool.scope(move |p| {
            // 正向校验
            for (s, t) in map_dbinstance_s_t {
                let db_compare = CompareDB {
                    source: s,
                    target: t,
                    batch: self.batch_size,
                    ttl_diff: self.ttl_diff,
                    compare_pool: self.compare_threads,
                    result_store_dir: current_dir.clone(),
                };
                p.spawn(move |_| {
                    db_compare.exec();
                });
            }

            // 反向校验
            // 反向校验只校验target中存在但source中不存在的数据
            if self.bothway {
                println!("执行反向校验");

                // 获取 target 实例 与 source 实例的对应关系
                let map = match self.map_dbinstance_target_to_source() {
                    Ok(m) => m,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                for (t, s) in map {
                    // 将 目标 redis instance 转化为但实例的的 redis instance 数组
                    for tc in t.to_single_redis_instance_with_db_vec() {
                        let compare_db_reverse = CompareDBReverse {
                            source: s.clone(),
                            target: tc,
                            batch: self.batch_size,
                            ttl_diff: self.ttl_diff,
                            compare_pool: self.compare_threads,
                            result_store_dir: current_dir.clone(),
                        };
                        compare_db_reverse.exec();
                    }
                }
            }
        });
        compare_times_remainder -= 1;
        print!("compare_times_remainder:{}", compare_times_remainder);

        // 执行循环校验
        loop {
            if compare_times_remainder <= 0 {
                return;
            }
            // Todo 增加错误处理逻辑
            println!("执行循环校验");
            // 获取上次校验结果的目录
            let last_result_dir = match fs::read_to_string(COMPARE_STATUS_FILE_NAME) {
                Ok(dir) => dir,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            // 创建存储当前结果目录
            let current_dir = create_result_dir().unwrap();
            // 遍历目录，反序列化结果文件并重新校验,校验结果写入新的结果目录
            let entries = match fs::read_dir(last_result_dir.as_str()) {
                Ok(dir) => dir,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            for e in entries {
                if let Ok(de) = e {
                    if let Ok(m) = de.metadata() {
                        if m.is_file() {
                            let name = match de.file_name().into_string() {
                                Ok(s) => s,
                                Err(_) => {
                                    log::error!("convert OsString to String error");

                                    continue;
                                }
                            };

                            let path = last_result_dir.clone() + "/" + &name;
                            let fk = match compare_from_file(path.as_str()) {
                                Ok(fk) => fk,
                                Err(e) => {
                                    log::error!("{}", e);
                                    // 复制文件到当前目录
                                    continue;
                                }
                            };
                            if !fk.iffy_keys.is_empty() {
                                log::error!("{:?}", fk);
                                if let Err(e) = fk.write_to_file(&current_dir) {
                                    log::error!("{}", e);
                                };
                            }
                        }
                    }
                };
            }

            // 清理上次校验生成的结果目录
            fs::remove_dir_all(last_result_dir).unwrap();
            compare_times_remainder -= 1;
            print!("compare_times_remainder:{}", compare_times_remainder);
        }
    }
}

impl Compare {
    // source to target DBInstance 映射
    fn map_dbinstance_source_to_target(
        &self,
    ) -> Result<HashMap<RedisInstanceWithDB, RedisInstanceWithDB>> {
        let mut s_t_map: HashMap<RedisInstanceWithDB, RedisInstanceWithDB> = HashMap::new();

        for si in &self.source {
            for (s, t) in &si.dbmapper {
                // 判断 target 是否为 cluster，cluster 不支持非 0 DB
                if let InstanceType::Cluster = self.target.instance_type {
                    if !t.eq(&0) {
                        return Err(anyhow!("target is cluster db must be 0"));
                    }
                };
                let s_instance_with_db = RedisInstanceWithDB {
                    instance: si.instance.clone(),
                    db: *s,
                };

                let t_instance_with_db = RedisInstanceWithDB {
                    instance: self.target.clone(),
                    db: *t,
                };

                s_t_map.insert(s_instance_with_db, t_instance_with_db);
            }
        }

        Ok(s_t_map)
    }

    // 返回target DBInstance 与 source DBInstance 的映射关系
    // ToDo 从新思考
    fn map_dbinstance_target_to_source(
        &self,
    ) -> Result<HashMap<RedisInstanceWithDB, Vec<RedisInstanceWithDB>>> {
        return match self.target.instance_type {
            InstanceType::Single => {
                let mut dbinstance_map = HashMap::new();
                let mut t_db_to_s_instance_map: HashMap<usize, Vec<RedisInstanceWithDB>> =
                    HashMap::new();
                // 遍历 source 中 dbmapper 生成 target db 与 source DBInstance 的 映射关系
                for s_instance in &self.source {
                    for (s, t) in &s_instance.dbmapper {
                        match t_db_to_s_instance_map.get(&t) {
                            Some(vec_dbinstance) => {
                                let s_dbinstance = RedisInstanceWithDB {
                                    instance: s_instance.instance.clone(),
                                    db: s.clone(),
                                };
                                let mut vec = vec_dbinstance.clone();
                                vec.push(s_dbinstance);
                                t_db_to_s_instance_map.insert(*t, vec);
                            }
                            None => {
                                let mut vec = vec![];
                                let s_dbinstance = RedisInstanceWithDB {
                                    instance: s_instance.instance.clone(),
                                    db: s.clone(),
                                };
                                vec.push(s_dbinstance);
                                t_db_to_s_instance_map.insert(*t, vec);
                            }
                        };
                    }
                }

                for (k, v) in t_db_to_s_instance_map {
                    let t_dbinstance = RedisInstanceWithDB {
                        instance: self.target.clone(),
                        db: k,
                    };

                    dbinstance_map.insert(t_dbinstance, v);
                }

                Ok(dbinstance_map)
            }
            InstanceType::Cluster => {
                let mut dbinstance_map = HashMap::new();
                let mut source_dbinstances = vec![];
                let t_dbinstance = RedisInstanceWithDB {
                    instance: self.target.clone(),
                    db: 0,
                };
                for instance in &self.source {
                    for (k, v) in &instance.dbmapper {
                        if !v.eq(&0) {
                            return Err(anyhow!(
                                "target instance_type is cluster,no db number {} exists",
                                v
                            ));
                        }

                        let s_dbinstance = RedisInstanceWithDB {
                            instance: instance.instance.clone(),
                            db: k.clone(),
                        };
                        source_dbinstances.push(s_dbinstance);
                    }
                }
                dbinstance_map.insert(t_dbinstance, source_dbinstances);
                Ok(dbinstance_map)
            }
        };
    }
}

// 创建结果文件目录
fn create_result_dir() -> Result<String> {
    let dt = Local::now();
    let timestampe = dt.timestamp();
    let dir = "cr_".to_owned()
        + &timestampe.to_string()
        + &"_".to_string()
        + &rand_lettter_number_string(4);
    create_dir(dir.clone())?;
    // ".current_compare" 记录结果目录名称，便于后续处理
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(COMPARE_STATUS_FILE_NAME)?;
    file.write_all(dir.as_bytes())?;
    Ok(dir)
}

fn remove_result_dir() -> Result<()> {
    let dir_name = fs::read_to_string(COMPARE_STATUS_FILE_NAME)?;
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
