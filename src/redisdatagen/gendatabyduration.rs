use crate::redisdatagen::OptType;
use crate::redisdatagen::RedisOpt;
use crate::util::rand_string;
use anyhow::{anyhow, Result};
use crossbeam::channel::{at, select};
use redis::ConnectionLike;
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct GeneratorByDuration {
    #[serde(default = "GeneratorByDuration::duration_default")]
    pub duration: usize,
    #[serde(default = "GeneratorByDuration::redis_url_default")]
    pub redis_url: String,
    #[serde(default = "GeneratorByDuration::redis_version_default")]
    pub redis_version: String,
    #[serde(default = "GeneratorByDuration::threads_default")]
    pub threads: usize,
    #[serde(default = "GeneratorByDuration::key_len_default")]
    pub key_len: usize,
    #[serde(default = "GeneratorByDuration::loopstep_default")]
    pub loopstep: usize,
    #[serde(default = "GeneratorByDuration::expire_default")]
    pub expire: usize,
    #[serde(default = "GeneratorByDuration::log_out_default")]
    pub log_out: bool,
}

impl Default for GeneratorByDuration {
    fn default() -> Self {
        Self {
            duration: 0,
            redis_url: "redis://:127.0.0.1:6379/?timeout=1s".to_string(),
            redis_version: "0".to_string(),
            threads: 1,
            key_len: 4,
            loopstep: 1,
            expire: 1,
            log_out: false,
        }
    }
}

// 生成默认值
impl GeneratorByDuration {
    fn duration_default() -> usize {
        0
    }
    fn redis_url_default() -> String {
        "redis://:127.0.0.1:6379/?timeout=1s".to_string()
    }
    fn redis_version_default() -> String {
        "4".to_string()
    }
    fn threads_default() -> usize {
        1
    }
    fn key_len_default() -> usize {
        4
    }
    fn loopstep_default() -> usize {
        1
    }
    fn expire_default() -> usize {
        1
    }
    fn log_out_default() -> bool {
        false
    }
}

impl GeneratorByDuration {
    pub fn exec(&self) -> Result<()> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .map_err(|err| anyhow!("{}", err.to_string()))?;

        let client = redis::Client::open(self.redis_url.clone())?;
        let deadline = Instant::now() + Duration::from_secs(self.duration as u64);

        pool.scope(|s| {
            for _ in 0..self.threads {
                let mut conn = client.get_connection().unwrap();
                s.spawn(move |_| {
                    loop {
                        select! {
                            recv(at(deadline)) -> _ => {
                                return;
                            },
                            default => {
                                // do your task
                                let subffix = rand_string(self.key_len);
                                let db = conn.get_db();
                                let mut opt = RedisOpt {
                                    redis_conn: &mut conn,
                                    redis_version: self.redis_version.clone(),
                                    opt_type: OptType::OPT_APPEND,
                                    key_suffix: subffix,
                                    loopstep: self.loopstep,
                                    expire: self.expire,
                                    db: db as usize,
                                    log_out: self.log_out,
                                };
                                opt.exec_all();
                                thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                });
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::init_log;
    use crate::redisdatagen::gendatabyduration::GeneratorByDuration;

    const rurl: &str = "redis://:redistest0102@114.67.76.82:16374/";

    //cargo test redisdatagen::gendatabyduration::test::test_GeneratorByDuration_exec --  --nocapture
    #[test]
    fn test_GeneratorByDuration_exec() {
        init_log();
        let mut gbd = GeneratorByDuration::default();
        gbd.redis_url = rurl.to_string();
        gbd.duration = 30;
        gbd.loopstep = 10;
        gbd.threads = 2;
        gbd.log_out = true;
        gbd.exec();
    }
}
