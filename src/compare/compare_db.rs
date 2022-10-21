use redis::{ConnectionLike, RedisResult};
use serde::{Deserialize, Serialize};

use crate::util::{key_type, scan, RedisClientWithDB, RedisConnection, RedisKey};

use super::{
    compare_error::CompareErrorType,
    comparekey::{Comparer, IffyKey},
    rediscompare::RedisInstanceWithDB,
    CompareError, InstanceType,
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

// 逆向校验
// 校验 target 中的 key 是否在任意 source 中存在
pub struct CompareDBReverse {
    pub source: Vec<RedisInstanceWithDB>,
    pub target: RedisInstanceWithDB,
    pub batch: usize,
    pub ttl_diff: usize,
    pub compare_pool: usize,
}

impl CompareDBReverse {
    pub fn exec(&self) {
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

        pool_compare.scope(move |pc| {
            // 判断 target client 是否为 Client，ClusterClient 不能scan
            let t_client = match self.target.to_redis_client_with_db() {
                Ok(tc) => tc,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let s_clients = match self.get_source_clients_with_db() {
                Ok(s) => s,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let mut t_scan_conn = match t_client.get_redis_connection() {
                Ok(tsc) => tsc,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            }
            .get_dyn_connection();

            let t_scan_iter = match scan::<String>(t_scan_conn.as_mut()) {
                Ok(iter) => iter,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let mut vec_keys: Vec<String> = Vec::new();
            let mut count = 0 as usize;
            for key in t_scan_iter {
                if count < self.batch {
                    vec_keys.push(key.clone());
                    count += 1;
                    continue;
                }

                let t_conn = match t_client.get_redis_connection() {
                    Ok(tc) => tc,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                let mut s_conns: Vec<RedisConnection> = vec![];
                for sc in s_clients.clone() {
                    let conn = match sc.get_redis_connection() {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };
                    s_conns.push(conn);
                }
                let vk = vec_keys.clone();
                pc.spawn(move |_| {
                    self.compare_keys_reverse(t_conn, s_conns, vk);
                });

                count = 0;
                vec_keys.clear();
                vec_keys.push(key.clone());
                count += 1;
            }

            if !vec_keys.is_empty() {
                let t_conn = match t_client.get_redis_connection() {
                    Ok(tc) => tc,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                let mut s_conns: Vec<RedisConnection> = vec![];
                for sc in s_clients {
                    let conn = match sc.get_redis_connection() {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };
                    s_conns.push(conn);
                }
                pc.spawn(move |_| {
                    self.compare_keys_reverse(t_conn, s_conns, vec_keys);
                });
            }
        });
    }

    pub fn compare_keys_reverse(
        &self,
        target_conn: RedisConnection,
        source_conns: Vec<RedisConnection>,
        keys: Vec<String>,
    ) {
        let mut t_conn = target_conn.get_dyn_connection();

        let rediskeys = keys_type(keys, t_conn.as_mut());
        let err_keys = keys_exists_any_connections(source_conns, &rediskeys);

        if !err_keys.is_empty() {
            println!("err_keys:{:?}", err_keys);
        }
    }

    // 根据Vec<DBInstance> 返回 Vec<DBClient>
    // fn source_to_redis_client_with_db_vec(&self) -> RedisResult<Vec<RedisClientWithDB>> {
    //     let mut vec: Vec<RedisClientWithDB> = vec![];
    //     for i in self.source.clone() {
    //         match i.instance.instance_type {
    //             InstanceType::Single => {
    //                 let client = redis::Client::open(i.instance.urls[0].as_str())?;
    //                 let dbclient = RedisClientWithDB {
    //                     client: RedisClient::Single(client),
    //                     db: i.db,
    //                 };
    //                 vec.push(dbclient);
    //             }
    //             InstanceType::Cluster => {
    //                 let mut cb = ClusterClientBuilder::new(i.instance.urls);
    //                 if !self.target.instance.password.is_empty() {
    //                     cb = cb.password(self.target.instance.password.clone());
    //                 }
    //                 let cluster_client = cb.open()?;

    //                 let dbclient = RedisClientWithDB {
    //                     client: RedisClient::Cluster(cluster_client),
    //                     db: i.db,
    //                 };
    //                 vec.push(dbclient);
    //             }
    //         }
    //     }
    //     Ok(vec)
    // }

    fn get_source_clients_with_db(&self) -> RedisResult<Vec<RedisClientWithDB>> {
        let mut clients: Vec<RedisClientWithDB> = vec![];
        for s in self.source.clone() {
            let c = s.to_redis_client_with_db()?;
            clients.push(c);
        }
        Ok(clients)
    }

    // fn get_souce_connection_vec(&self) -> RedisResult<Vec<RedisConnection>> {
    //     let mut redis_connections = vec![];
    //     for s_client_db in &self.source {
    //         let conn = s_client_db.get_redis_connection()?;
    //         redis_connections.push(conn);
    //     }

    //     Ok(redis_connections)
    // }

    // fn get_connection_vec(
    //     &self,
    //     db_clients: Vec<RedisClientWithDB>,
    // ) -> RedisResult<Vec<RedisConnection>> {
    //     let mut vec_conn: Vec<RedisConnection> = vec![];
    //     let cmd_select = redis::cmd("select");
    //     for dbc in db_clients {
    //         match dbc.client {
    //             RedisClient::Single(sc) => {
    //                 let mut conn = sc.get_connection()?;
    //                 conn.req_command(cmd_select.clone().arg(dbc.db))?;
    //                 let rc = RedisConnection::Single(conn);
    //                 vec_conn.push(rc);
    //             }
    //             RedisClient::Cluster(cc) => {
    //                 let conn = cc.get_connection()?;
    //                 let rc = RedisConnection::Cluster(conn);
    //                 vec_conn.push(rc);
    //             }
    //         }
    //     }
    //     Ok(vec_conn)
    // }
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

// 比较key在多个db中是否存在，在任意一个库中存在则返回true，key在所有key中都不存在返回false
// 返回 检查结果为false 的 RedisKey
fn keys_exists_any_connections(
    mut conns: Vec<RedisConnection>,
    keys: &Vec<RedisKey>,
) -> Vec<RedisKey> {
    let mut vec_rediskeys: Vec<RedisKey> = vec![];

    for key in keys {
        let mut key_existes = false;
        for conn in &mut conns {
            if let RedisConnection::Single(sc) = conn {
                match redis::cmd("exists")
                    .arg(key.key_name.clone())
                    .query::<bool>(sc)
                {
                    Ok(exists) => {
                        if exists {
                            key_existes = exists;
                        }
                    }
                    Err(_) => {}
                }
            }

            if let RedisConnection::Cluster(cc) = conn {
                match redis::cmd("exists").arg(key.key_name.clone()).query(cc) {
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
            let iffy = IffyKey {
                key: key.clone(),
                error: CompareError {
                    message: Some("key not in any db".to_owned()),
                    error_type: CompareErrorType::ExistsErr,
                    reason: None,
                },
            };
            vec_rediskeys.push(key.clone());
        }
    }
    vec_rediskeys
}
