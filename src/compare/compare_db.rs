use anyhow::Result;
use redis::ConnectionLike;
use serde::{Deserialize, Serialize};

use crate::util::{key_type, scan, RedisClient, RedisKey};

use super::{
    comparekey::Comparer,
    rediscompare::{RedisClientWithDB, RedisInstanceWithDB},
    InstanceType,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FailKeys {
    pub source: RedisInstanceWithDB,
    pub target: RedisInstanceWithDB,
    pub keys: Vec<RedisKey>,
    // 是否为反向校验
    pub reverse: bool,
}

// impl FailKeys {
//     pub fn to_vec(&self) -> Result<Vec<u8>> {
//         let mut buf = Vec::new();
//         self.serialize(&mut Serializer::new(&mut buf))?;
//         Ok(buf)
//     }

//     pub fn write_to_file(&self, path: &str) -> Result<()> {
//         let mut buf = Vec::new();
//         self.serialize(&mut Serializer::new(&mut buf))?;
//         write_result_file(path, &buf)?;
//         Ok(())
//     }

//     // 正向校验
//     pub fn compare(&self) {}

//     // 反向校验
// }

// 给定 souce DBClient，target DBClient，执行正向校验，并输出日志和写入结果文件
pub struct CompareDB {
    pub source: RedisInstanceWithDB,
    pub target: RedisInstanceWithDB,
    pub batch: usize,
    pub ttl_diff: usize,
    pub compare_pool: usize,
}

impl CompareDB {
    pub fn compare(&self) {
        // 判断 source 是否为 单实例 redis，cluster 模式不支持 sacn
        if let InstanceType::Cluster = self.source.instance.instance_type {
            log::error!("source redis instance is not single redis instance");
            return;
        }

        let s_client_rs = self.source.instance.to_redis_client();
        let s_client = match s_client_rs {
            Ok(sc) => sc,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        let pool_compare_rs = rayon::ThreadPoolBuilder::new()
            .num_threads(self.compare_pool)
            .build();
        let pool_compare = match pool_compare_rs {
            Ok(p) => p,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        let t_client_rs = self.target.instance.to_redis_client();
        let t_client = match t_client_rs {
            Ok(rc) => rc,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        pool_compare.scope(move |pc| {
            let s_scan_conn_rs = s_client.get_redis_connection();
            let s_scan_conn = match s_scan_conn_rs {
                Ok(ssc) => ssc,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let mut sscan: Box<dyn ConnectionLike> = s_scan_conn.get_dyn_connection();

            let redis_cmd_select = redis::cmd("select");
            if let Err(e) = sscan
                .as_mut()
                .req_command(redis_cmd_select.clone().arg(self.source.db))
            {
                log::error!("{}", e);
                return;
            };

            let scan_iter_rs = scan::<String>(sscan.as_mut());
            let scan_iter = match scan_iter_rs {
                Ok(iter) => iter,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let mut vec_keys: Vec<String> = Vec::new();
            let mut count = 0 as usize;
            for key in scan_iter {
                if count < self.batch {
                    vec_keys.push(key.clone());
                    count += 1;
                    continue;
                }
                let s_redis_conn_rs = s_client.get_redis_connection();
                let s_redis_conn = match s_redis_conn_rs {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        break;
                    }
                };

                let t_redis_conn_rs = t_client.get_redis_connection();
                let t_redis_conn = match t_redis_conn_rs {
                    Ok(tc) => tc,
                    Err(e) => {
                        log::error!("{}", e);
                        break;
                    }
                };

                let vk = vec_keys.clone();

                pc.spawn(move |_| {
                    let cmd_select = redis::cmd("select");
                    let mut sconn = s_redis_conn.get_dyn_connection();
                    let mut tconn: Box<dyn ConnectionLike> = t_redis_conn.get_dyn_connection();

                    if let Err(e) = sconn.req_command(cmd_select.clone().arg(self.source.db)) {
                        log::error!("{}", e);
                        return;
                    };

                    if let InstanceType::Single = self.target.instance.instance_type {
                        if let Err(e) = tconn.req_command(cmd_select.clone().arg(self.target.db)) {
                            log::error!("{}", e);
                            return;
                        };
                    }

                    let rediskeys = keys_type(vk, sconn.as_mut());

                    let mut comparer = Comparer {
                        sconn: sconn.as_mut(),
                        tconn: tconn.as_mut(),
                        ttl_diff: self.ttl_diff,
                        batch: self.batch,
                    };

                    // ToDo 错误输出内置到 compare_rediskeys 函数
                    let err_keys = compare_rediskeys(&mut comparer, rediskeys);
                    if !err_keys.is_empty() {
                        let cfk = FailKeys {
                            // source_db: s_db,
                            // target_db: t_db,
                            keys: err_keys,
                            reverse: false,
                            source: self.source.clone(),
                            target: self.target.clone(),
                        };
                        // if let Err(e) = cfk.write_to_file(&c_dir) {
                        //     log::error!("{}", e);
                        // };
                    }
                });
                count = 0;
                vec_keys.clear();
                vec_keys.push(key.clone());
                count += 1;
            }
        });
    }
}

// 批量获取key type
fn keys_type(keys: Vec<String>, con: &mut dyn redis::ConnectionLike) -> Vec<RedisKey> {
    let mut redis_key_vec = vec![];
    for key in keys {
        let key_type = key_type(key.clone(), con);
        match key_type {
            Ok(kt) => {
                let redis_key = RedisKey {
                    key_name: key,
                    key_type: kt,
                };
                redis_key_vec.push(redis_key);
            }
            Err(e) => {
                log::error!("{}", e);
            }
        }
    }
    redis_key_vec
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
