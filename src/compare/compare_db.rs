use redis::ConnectionLike;
use serde::{Deserialize, Serialize};

use crate::util::{key_type, scan, RedisConnection, RedisKey};

use super::{
    comparekey::{Comparer, IffyKey},
    rediscompare::RedisInstanceWithDB,
    InstanceType,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FailKeys {
    pub source: RedisInstanceWithDB,
    pub target: RedisInstanceWithDB,
    pub keys: Vec<IffyKey>,
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
    pub fn exec(&self) {
        // 判断 source 是否为 单实例 redis，cluster 模式不支持 sacn
        if let InstanceType::Cluster = self.source.instance.instance_type {
            log::error!("source redis instance is not single redis instance");
            return;
        }

        let s_client = match self.source.instance.to_redis_client() {
            Ok(sc) => sc,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        let pool_compare = match rayon::ThreadPoolBuilder::new()
            .num_threads(self.compare_pool)
            .build()
        {
            Ok(p) => p,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        let t_client = match self.target.instance.to_redis_client() {
            Ok(rc) => rc,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        pool_compare.scope(move |pc| {
            let s_scan_conn = match s_client.get_redis_connection() {
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

            let scan_iter = match scan::<String>(sscan.as_mut()) {
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

                let s_redis_conn = match s_client.get_redis_connection() {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        break;
                    }
                };

                let t_redis_conn = match t_client.get_redis_connection() {
                    Ok(tc) => tc,
                    Err(e) => {
                        log::error!("{}", e);
                        break;
                    }
                };

                let vk = vec_keys.clone();
                pc.spawn(move |_| {
                    self.compare_keys(s_redis_conn, t_redis_conn, vk);
                });

                count = 0;
                vec_keys.clear();
                vec_keys.push(key.clone());
                count += 1;
            }

            if !vec_keys.is_empty() {
                let s_redis_conn = match s_client.get_redis_connection() {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                let t_redis_conn = match t_client.get_redis_connection() {
                    Ok(tc) => tc,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                let vk = vec_keys;

                pc.spawn(move |_| {
                    self.compare_keys(s_redis_conn, t_redis_conn, vk);
                });
            }
        });
    }

    /// .用与进行keys批量正向校验
    /// 正向校验判断 key 在 target 是否存在，校验key的值是否相等以及source 和 target 的 ttl 差值是否在合理范围内
    fn compare_keys(&self, source: RedisConnection, target: RedisConnection, keys: Vec<String>) {
        let cmd_select = redis::cmd("select");
        let mut sconn = source.get_dyn_connection();
        let mut tconn: Box<dyn ConnectionLike> = target.get_dyn_connection();

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

        let rediskeys = keys_type(keys, sconn.as_mut());
        let comparer = Comparer {
            sconn: sconn.as_mut(),
            tconn: tconn.as_mut(),
            ttl_diff: self.ttl_diff,
            batch: self.batch,
        };

        // ToDo 错误输出内置到 compare_rediskeys 函数
        let iffy_keys = comparer.compare_rediskeys(&rediskeys);
        if !iffy_keys.is_empty() {
            let cfk = FailKeys {
                keys: iffy_keys,
                source: self.source.clone(),
                target: self.target.clone(),
                reverse: false,
            };
            log::error!("{:?}", cfk);
            // if let Err(e) = cfk.write_to_file(&c_dir) {
            //     log::error!("{}", e);
            // };
        }
    }
}

/// .批量获取key type
fn keys_type(keys: Vec<String>, con: &mut dyn redis::ConnectionLike) -> Vec<RedisKey> {
    let mut redis_key_vec = vec![];
    for key in keys {
        match key_type(key.clone(), con) {
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
