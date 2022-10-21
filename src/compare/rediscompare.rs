use super::comparekey::Comparer;
use crate::compare::{CompareDB, CompareDBReverse};
use crate::util::{rand_string, RedisClientWithDB};
use crate::util::{RedisClient, RedisKey};
use anyhow::{anyhow, Result};
use chrono::prelude::Local;
use redis::cluster::ClusterClientBuilder;
use redis::RedisResult;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, create_dir, remove_dir_all, File, OpenOptions};
use std::io::{LineWriter, Read, Write};
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

                        // let cl = redis::Client::open(url_single)?;
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
    pub fn to_single_client_db_vec(&self) -> RedisResult<Vec<RedisClientWithDB>> {
        let db_clients: Vec<RedisClientWithDB> = match self.instance.instance_type {
            InstanceType::Single => {
                let mut vec: Vec<RedisClientWithDB> = vec![];
                let client = self.instance.to_redis_client()?;
                let db_client = RedisClientWithDB {
                    client,
                    db: self.db,
                };
                vec.push(db_client);
                vec
            }
            InstanceType::Cluster => {
                let mut vec: Vec<RedisClientWithDB> = vec![];
                let clients = self.instance.to_single_redis_clients()?;
                for c in clients {
                    let db_client = RedisClientWithDB {
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
        // 获取source to target 对应关系
        let map_dbinstance_s_t = match self.map_dbinstance_source_target() {
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
                let map = match self.map_dbinstance_target_source() {
                    Ok(m) => m,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                for (t, s) in map {
                    let t_clients = match t.to_single_client_db_vec() {
                        Ok(tcs) => tcs,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };

                    // let mut s_clients: Vec<RedisClientWithDB> = vec![];
                    // for s_instance in s {
                    //     let redis_client = match s_instance.instance.to_redis_client() {
                    //         Ok(rc) => rc,
                    //         Err(e) => {
                    //             log::error!("{}", e);
                    //             return;
                    //         }
                    //     };
                    //     let r_c_w_db = RedisClientWithDB {
                    //         client: redis_client,
                    //         db: s_instance.db,
                    //     };
                    //     s_clients.push(r_c_w_db);
                    // }

                    for tc in t.to_single_redis_instance_with_db_vec() {
                        let compare_db_reverse = CompareDBReverse {
                            source: s.clone(),
                            target: tc,
                            batch: self.batch_size,
                            ttl_diff: self.ttl_diff,
                            compare_pool: self.compare_threads,
                        };
                        compare_db_reverse.exec();
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
}

impl Compare {
    // source to target DBInstance 映射
    fn map_dbinstance_source_target(
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
    fn map_dbinstance_target_source(
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
fn compare_rediskeys(comparer: &mut Comparer, keys_vec: Vec<RedisKey>) -> Vec<RedisKey> {
    let mut vec_keys = vec![];
    for key in keys_vec {
        let r = comparer.compare_key(key.clone());
        if let Err(e) = r {
            vec_keys.push(key.clone());
            log::error!("{}", e);
        }
    }
    vec_keys
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
                key_name: "key".to_string() + &i.to_string(),
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
                    key_name: "key".to_string() + &i.to_string(),
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
