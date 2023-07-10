// 通过命令组合生成key，尽量覆盖redis所有命令操作

use crate::util::rand_string;
use rand::Rng;
use redis::ConnectionLike;
use redis::{RedisResult, ToRedisArgs};
use std::fmt;
use std::fmt::Formatter;
use std::time::Duration;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio::time::Instant;

#[derive(Debug, PartialEq, EnumIter, Clone)]
pub enum OptType {
    OptAppend,
    OptBitop,
    OptDecrDecrby,
    OptIncrIncrbyIncrbyfloat,
    OptMsetMsetnx,
    OptPsetexSetex,
    OptPfadd,
    OptPfmerge,
    OptSetSetnx,
    OptSetbit,
    OptSetrange,
    OptHincrbyHincrbyfloat,
    OptHsetHsetnxHdelHmset,
    OptLpushLpopLpushxLset,
    OptLremLtrimLinsert,
    OptRpushRpushxRpopRpoplpush,
    OptBlpopBrpopBrpoplpush,
    OptLmoveBlmoveLmpopBlmpop,
    OptSaddSmoveSpopSrem,
    OptSdiffstoreSinertstoreSunionstore,
    OptZaddZincrbyZerm,
    OptZmpopBzmpop,
    OptZpopmaxZpopmin,
    OptBzpopmaxBzpopmin,
    OptZremrangebylexZremrangebyrankZremrangebyscore,
    OptZunionstoreZinterstore,
}

impl fmt::Display for OptType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OptType::OptAppend => {
                write!(f, "opt_append")
            }
            OptType::OptBitop => {
                write!(f, "opt_bitop")
            }
            OptType::OptDecrDecrby => {
                write!(f, "opt_decr_decrby")
            }
            OptType::OptIncrIncrbyIncrbyfloat => {
                write!(f, "opt_incr_incrby_incrbyfloat")
            }
            OptType::OptMsetMsetnx => {
                write!(f, "opt_mset_msetnx")
            }
            OptType::OptPsetexSetex => {
                write!(f, "opt_psetex_setex")
            }
            OptType::OptPfadd => {
                write!(f, "opt_pfadd")
            }
            OptType::OptPfmerge => {
                write!(f, "opt_pfmerge")
            }
            OptType::OptSetSetnx => {
                write!(f, "opt_set_setnx")
            }
            OptType::OptSetbit => {
                write!(f, "opt_setbit")
            }
            OptType::OptSetrange => {
                write!(f, "opt_setrange")
            }
            OptType::OptHincrbyHincrbyfloat => {
                write!(f, "opt_hincrby_hincrbyfloat")
            }
            OptType::OptHsetHsetnxHdelHmset => {
                write!(f, "opt_hset_hsetnx_hdel_hmset")
            }
            OptType::OptLpushLpopLpushxLset => {
                write!(f, "opt_lpush_lpop_lpushx_lset")
            }
            OptType::OptLremLtrimLinsert => {
                write!(f, "opt_lrem_ltrim_linsert")
            }
            OptType::OptRpushRpushxRpopRpoplpush => {
                write!(f, "opt_rpush_rpushx_rpop_rpoplpush")
            }
            OptType::OptLmoveBlmoveLmpopBlmpop => {
                write!(f, "opt_lmove_blmove_lmpop_blmpop")
            }
            OptType::OptBlpopBrpopBrpoplpush => {
                write!(f, "opt_blpop_brpop_brpoplpush")
            }
            OptType::OptSaddSmoveSpopSrem => {
                write!(f, "opt_sadd_smove_spop_srem")
            }
            OptType::OptSdiffstoreSinertstoreSunionstore => {
                write!(f, "opt_sdiffstore_sinertstore_sunionstore")
            }
            OptType::OptZaddZincrbyZerm => {
                write!(f, "opt_zadd_zincrby_zerm")
            }
            OptType::OptZpopmaxZpopmin => {
                write!(f, "opt_zpopmax_zpopmin")
            }
            OptType::OptZmpopBzmpop => {
                write!(f, "opt_zmpop_bzmpop")
            }
            OptType::OptBzpopmaxBzpopmin => {
                write!(f, "opt_bzpopmax_bzpopmin")
            }
            OptType::OptZremrangebylexZremrangebyrankZremrangebyscore => {
                write!(f, "opt_zremrangebylex_zremrangebyrank_zremrangebyscore")
            }
            OptType::OptZunionstoreZinterstore => {
                write!(f, "opt_zunionstore_zinterstore")
            }
        }
    }
}

pub struct RedisOpt<'a> {
    pub redis_conn: &'a mut dyn ConnectionLike,
    pub redis_version: String,
    pub opt_type: OptType,
    pub key_suffix: String,
    pub loopstep: usize,
    pub expire: usize,
    pub db: usize,
    pub log_out: bool,
}

#[derive(Debug)]
pub struct ExecuteResult {
    pub elapsed: Duration,
    pub opt_type: OptType,
    pub result: RedisResult<()>,
    pub db: usize,
}

impl<'a> RedisOpt<'a> {
    pub fn exec(&mut self) -> RedisResult<()> {
        let start = Instant::now();
        let r = match self.opt_type {
            OptType::OptAppend => self.opt_append(),
            OptType::OptBitop => self.opt_bitop(),
            OptType::OptDecrDecrby => self.opt_decr_decrby(),
            OptType::OptIncrIncrbyIncrbyfloat => self.opt_incr_incrby_incrbyfloat(),
            OptType::OptMsetMsetnx => self.opt_mset_msetnx(),
            OptType::OptPsetexSetex => self.opt_psetex_setex(),
            OptType::OptPfadd => self.opt_pfadd(),
            OptType::OptPfmerge => self.opt_pfmerge(),
            OptType::OptSetSetnx => self.opt_set_setnx(),
            OptType::OptSetbit => self.opt_setbit(),
            OptType::OptSetrange => self.opt_setrange(),
            OptType::OptHincrbyHincrbyfloat => self.opt_hincrby_hincrbyfloat(),
            OptType::OptHsetHsetnxHdelHmset => self.opt_hset_hsetnx_hdel_hmset(),
            OptType::OptLpushLpopLpushxLset => self.opt_lpush_lpop_lpushx_lset(),
            OptType::OptLremLtrimLinsert => self.opt_lrem_ltrim_linsert(),
            OptType::OptRpushRpushxRpopRpoplpush => self.opt_rpush_rpushx_rpop_rpoplpush(),
            OptType::OptLmoveBlmoveLmpopBlmpop => self.opt_lmove_blmove_lmpop_blmpop(),
            OptType::OptBlpopBrpopBrpoplpush => self.opt_blpop_brpop_brpoplpush(),
            OptType::OptSaddSmoveSpopSrem => self.opt_sadd_smove_spop_srem(),
            OptType::OptSdiffstoreSinertstoreSunionstore => {
                self.opt_sdiffstore_sinterstore_sunionstore()
            }
            OptType::OptZaddZincrbyZerm => self.opt_zadd_zincrby_zerm(),
            OptType::OptZmpopBzmpop => self.opt_zmpop_bzmpop(),
            OptType::OptZpopmaxZpopmin => self.opt_zpopmax_zpopmin(),
            OptType::OptBzpopmaxBzpopmin => self.opt_bzpopmax_bzpopmin(),
            OptType::OptZremrangebylexZremrangebyrankZremrangebyscore => {
                self.opt_zremrangebylex_zremrangebyrank_zremrangebyscore()
            }
            OptType::OptZunionstoreZinterstore => self.opt_zunionstore_zinterstore(),
        };

        let result = ExecuteResult {
            elapsed: start.elapsed(),
            opt_type: self.opt_type.clone(),
            result: r,
            db: self.db,
        };
        if self.log_out {
            log::info!("{:?}", result);
        }

        result.result
    }

    pub fn exec_all(&mut self) {
        let opttype = self.opt_type.clone();
        for ot in OptType::iter() {
            // println!("{:?}", ot);
            self.opt_type = ot;

            let _ = self.exec();
            // println!("{:?}", self.exec());
        }

        self.opt_type = opttype;
    }
}

// 执行函数
// ToDo 调整命令顺序，保证版本小的命令先执行
impl<'a> RedisOpt<'a> {
    // append 操作
    pub fn opt_append(&mut self) -> RedisResult<()> {
        let key = "append_".to_string() + &*self.key_suffix;
        for i in 0..self.loopstep {
            let cmd_append = redis::cmd("append");
            let cmd_expire = redis::cmd("expire");
            let _ = self.redis_conn.req_command(
                cmd_append
                    .clone()
                    .arg(key.clone().to_redis_args())
                    .arg(&*i.to_string()),
            )?;
            let _ = self.redis_conn.req_command(
                cmd_expire
                    .clone()
                    .arg(key.to_redis_args())
                    .arg(self.expire.to_redis_args()),
            )?;
        }
        Ok(())
    }

    // bitmap操作
    pub fn opt_bitop(&mut self) -> RedisResult<()> {
        let bitopand = "bitop_and_".to_string() + &*self.key_suffix;
        let bitopor = "bitop_or_".to_string() + &*self.key_suffix;
        let bitopxor = "bitop_xor_".to_string() + &*self.key_suffix;
        let bitopnot = "bitop_not_".to_string() + &*self.key_suffix;

        let mut opvec = vec![];
        for i in 0..self.loopstep {
            let bitop = "bitop_".to_string() + &*self.key_suffix + &*i.to_string();
            let cmd_set = redis::cmd("set");
            let _ = self.redis_conn.req_command(
                cmd_set
                    .clone()
                    .arg(bitop.clone().to_redis_args())
                    .arg(bitop.clone().to_redis_args())
                    .arg("EX")
                    .arg(self.expire.to_redis_args()),
            )?;
            opvec.push(bitop);
        }

        let cmd_bitop = redis::cmd("bitop");
        let cmd_expire = redis::cmd("expire");
        //执行 and 操作
        let _ = self.redis_conn.req_command(
            cmd_bitop
                .clone()
                .arg("and".to_redis_args())
                .arg(bitopand.to_redis_args())
                .arg(opvec.to_redis_args()),
        )?;
        let _ = self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bitopand.to_redis_args())
                .arg(self.expire.to_redis_args()),
        )?;

        //执行 or 操作
        let _ = self.redis_conn.req_command(
            cmd_bitop
                .clone()
                .arg("or".to_redis_args())
                .arg(bitopor.to_redis_args())
                .arg(opvec.to_redis_args()),
        )?;
        let _ = self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bitopor.to_redis_args())
                .arg(self.expire.to_redis_args()),
        )?;

        //执行 xor 操作
        let _ = self.redis_conn.req_command(
            cmd_bitop
                .clone()
                .arg("xor".to_redis_args())
                .arg(bitopxor.to_redis_args())
                .arg(opvec.to_redis_args()),
        )?;
        let _ = self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bitopxor.to_redis_args())
                .arg(self.expire.to_redis_args()),
        )?;

        //执行 not 操作
        let _ = self.redis_conn.req_command(
            cmd_bitop
                .clone()
                .arg("not".to_redis_args())
                .arg(bitopnot.clone().to_redis_args())
                .arg(opvec[0].to_redis_args()),
        )?;
        let _ = self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bitopnot.to_redis_args())
                .arg(self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //DECR and DECRBY
    pub fn opt_decr_decrby(&mut self) -> RedisResult<()> {
        let decr = "decr_".to_string() + &*self.key_suffix.clone().to_string();
        let cmd_set = redis::cmd("set");
        let _ = self.redis_conn.req_command(
            cmd_set
                .clone()
                .arg(decr.clone().to_redis_args())
                .arg(&*self.loopstep.clone().to_redis_args())
                .arg("EX")
                .arg(&*self.expire.to_redis_args()),
        )?;

        let cmd_decr = redis::cmd("decr");
        let _ = self
            .redis_conn
            .req_command(cmd_decr.clone().arg(decr.clone().to_redis_args()))?;

        let cmd_decrby = redis::cmd("decrby");
        let mut rng = rand::thread_rng();
        let step = rng.gen_range(0..self.loopstep);
        let _ = self.redis_conn.req_command(
            cmd_decrby
                .clone()
                .arg(decr.clone().to_redis_args())
                .arg(step.to_redis_args()),
        )?;

        Ok(())
    }

    //INCR and INCRBY and INCRBYFLOAT
    pub fn opt_incr_incrby_incrbyfloat(&mut self) -> RedisResult<()> {
        let incr = "incr_".to_string() + &*self.key_suffix.clone().to_string();
        let cmd_set = redis::cmd("set");
        let mut rng = rand::thread_rng();

        self.redis_conn.req_command(
            cmd_set
                .clone()
                .arg(incr.clone())
                .arg(rng.gen_range(0..self.loopstep)),
        )?;

        let cmd_incr = redis::cmd("incr");
        let cmd_incrby = redis::cmd("incrby");
        let cmd_incrbyfloat = redis::cmd("incrbyfloat");

        let step = rng.gen_range(0..self.loopstep);
        let step_float: f64 = rng.gen();

        self.redis_conn
            .req_command(cmd_incr.clone().arg(incr.clone()))?;
        self.redis_conn.req_command(
            cmd_incrby
                .clone()
                .arg(incr.clone())
                .arg(step.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_incrbyfloat
                .clone()
                .arg(incr.clone())
                .arg(step_float.to_redis_args()),
        )?;

        Ok(())
    }

    // MSET and MSETNX
    pub fn opt_mset_msetnx(&mut self) -> RedisResult<()> {
        let mut msetarry = vec![];
        let mut msetnxarry = vec![];

        let mset = "mset_".to_string() + &*self.key_suffix.clone();
        let msetnx = "msetnx_".to_string() + &*self.key_suffix.clone();

        for i in 0..self.loopstep {
            msetarry.push(mset.clone() + &*i.to_string());
            msetarry.push(mset.clone() + &*i.to_string());
            msetnxarry.push(msetnx.clone() + &*i.to_string());
            msetnxarry.push(msetnx.clone() + &*i.to_string());
        }

        let cmd_mset = redis::cmd("mset");
        let cmd_msetnx = redis::cmd("msetnx");

        let _ = self
            .redis_conn
            .req_command(cmd_msetnx.clone().arg(msetnxarry.clone()))?;
        let _ = self
            .redis_conn
            .req_command(cmd_mset.clone().arg(msetarry.clone()))?;
        let _ = self
            .redis_conn
            .req_command(cmd_mset.clone().arg(msetnxarry.clone()))?;
        let _ = self
            .redis_conn
            .req_command(cmd_msetnx.clone().arg(msetarry.clone()))?;

        let cmd_expire = redis::cmd("expire");

        for i in 0..self.loopstep {
            let _ = self.redis_conn.req_command(
                cmd_expire
                    .clone()
                    .arg(mset.clone() + &*i.to_string())
                    .arg(&*self.expire.to_redis_args()),
            )?;
            let _ = self.redis_conn.req_command(
                cmd_expire
                    .clone()
                    .arg(msetnx.clone() + &*i.to_string())
                    .arg(&*self.expire.to_redis_args()),
            )?;
        }
        Ok(())
    }

    // PSETEX and SETEX
    pub fn opt_psetex_setex(&mut self) -> RedisResult<()> {
        let psetex = "psetex_".to_string() + &*self.key_suffix.clone();
        let setex = "setex".to_string() + &*self.key_suffix.clone();

        let cmd_psetex = redis::cmd("psetex");
        let cmd_setex = redis::cmd("setex");
        self.redis_conn.req_command(
            cmd_setex
                .clone()
                .arg(setex.clone())
                .arg(&self.expire.clone())
                .arg(setex.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_psetex
                .clone()
                .arg(psetex.clone())
                .arg(&self.expire.clone() * 1000)
                .arg(psetex.clone()),
        )?;
        Ok(())
    }

    //PFADD
    pub fn opt_pfadd(&mut self) -> RedisResult<()> {
        let pfadd = "pfadd_".to_string() + &*self.key_suffix.clone();
        let element_prefix = rand_string(4);
        let cmd_pfadd = redis::cmd("pfadd");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_pfadd
                    .clone()
                    .arg(pfadd.clone())
                    .arg(element_prefix.clone() + &*i.to_string()),
            )?;
        }
        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(pfadd.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        Ok(())
    }

    //PFMERGE
    pub fn opt_pfmerge(&mut self) -> RedisResult<()> {
        let pfadd = "pfadd_".to_string() + &*self.key_suffix.clone();
        let pfmerge = "pfmerge_".to_string() + &*self.key_suffix.clone();
        let mut pfvec = vec![];

        let cmd_pfadd = redis::cmd("pfadd");
        let cmd_pfmerge = redis::cmd("pfmerge");
        let cmd_del = redis::cmd("del");

        for i in 0..self.loopstep {
            let key = pfadd.clone() + &*i.to_string();
            self.redis_conn
                .req_command(cmd_pfadd.clone().arg(key.clone()).arg(rand_string(4)))?;
            pfvec.push(key);
        }

        self.redis_conn.req_command(
            cmd_pfmerge
                .clone()
                .arg(pfmerge)
                .arg(pfvec.clone().to_redis_args()),
        )?;

        for item in pfvec.iter() {
            self.redis_conn.req_command(cmd_del.clone().arg(item))?;
        }
        Ok(())
    }

    //SET and SETNX
    pub fn opt_set_setnx(&mut self) -> RedisResult<()> {
        let set = "set_".to_string() + &*self.key_suffix.clone();
        let setnx = "setnx_".to_string() + &*self.key_suffix.clone();

        let cmd_set = redis::cmd("set");
        let cmd_setnx = redis::cmd("setnx");

        self.redis_conn.req_command(
            cmd_set
                .clone()
                .arg(set.clone())
                .arg(set.clone())
                .arg("EX")
                .arg(&*self.expire.clone().to_redis_args()),
        )?;

        self.redis_conn
            .req_command(cmd_setnx.clone().arg(setnx.clone()).arg(setnx.clone()))?;

        self.redis_conn
            .req_command(cmd_setnx.clone().arg(setnx.clone()).arg(set.clone()))?;

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(setnx.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //SETBIT
    pub fn opt_setbit(&mut self) -> RedisResult<()> {
        let setbit = "setbit_".to_string() + &*self.key_suffix.clone();
        let cmd_setbit = redis::cmd("setbit");
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0..=self.loopstep);
        self.redis_conn
            .req_command(cmd_setbit.clone().arg(setbit.clone()).arg(offset).arg(1))?;

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(setbit)
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //SETRANGE
    pub fn opt_setrange(&mut self) -> RedisResult<()> {
        let setrange = "setrange_".to_string() + &*self.key_suffix.clone();
        let cmd_setrange = redis::cmd("setrange");
        let cmd_set = redis::cmd("set");
        self.redis_conn
            .req_command(cmd_set.clone().arg(setrange.clone()).arg(setrange.clone()))?;
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0..setrange.len());
        self.redis_conn.req_command(
            cmd_setrange
                .clone()
                .arg(setrange.clone())
                .arg(offset)
                .arg(rand_string(4)),
        )?;
        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(setrange)
                .arg(&*self.expire.to_redis_args()),
        )?;
        Ok(())
    }

    //HINCRBY and HINCRBYFLOAT
    pub fn opt_hincrby_hincrbyfloat(&mut self) -> RedisResult<()> {
        let hincrby = "hincrby_".to_string() + &*self.key_suffix.clone();
        let hincrbyfloat = "hincrbyfloat_".to_string() + &*self.key_suffix.clone();
        let cmd_hincrby = redis::cmd("hincrby");
        let cmd_hincrbyfloat = redis::cmd("hincrbyfloat");
        let mut rng = rand::thread_rng();
        for _i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_hincrby
                    .clone()
                    .arg(hincrby.clone())
                    .arg(rng.gen_range(0..self.loopstep))
                    .arg(rng.gen_range(0..self.loopstep)),
            )?;
            self.redis_conn.req_command(
                cmd_hincrbyfloat
                    .clone()
                    .arg(hincrbyfloat.clone())
                    .arg(rng.gen_range(0..self.loopstep))
                    .arg(rng.gen::<f64>()),
            )?;
        }
        Ok(())
    }

    //HSET HSETNX HDEL HMSET
    pub fn opt_hset_hsetnx_hdel_hmset(&mut self) -> RedisResult<()> {
        let hset = "hset_".to_string() + &*self.key_suffix.clone();
        let hmset = "hmset_".to_string() + &*self.key_suffix.clone();
        let mut rng = rand::thread_rng();

        // let mut fieldmap = HashMap::new();
        let mut fieldvec = vec![];
        let cmd_hset = redis::cmd("hset");
        let cmd_hmset = redis::cmd("hmset");
        let cmd_hsetnx = redis::cmd("hsetnx");
        let cmd_hdel = redis::cmd("hdel");

        for i in 0..self.loopstep {
            let field = hmset.clone() + &*i.to_string();
            fieldvec.push(field.clone());
            fieldvec.push(field.clone());
        }

        // hset
        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_hset
                    .clone()
                    .arg(hset.clone())
                    .arg(hset.clone() + &*i.to_string())
                    .arg(hset.clone() + &*i.to_string()),
            )?;
        }

        for i in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_hsetnx
                        .clone()
                        .arg(hset.clone())
                        .arg(hset.clone() + &*rng.gen_range(0..self.loopstep).to_string())
                        .arg(hset.clone() + &*i.to_string()),
                )?;
            }
        }

        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_hdel
                        .clone()
                        .arg(hset.clone())
                        .arg(hset.clone() + &*rng.gen_range(0..self.loopstep).to_string()),
                )?;
            }
        }

        self.redis_conn.req_command(
            cmd_hmset
                .clone()
                .arg(hmset.clone())
                .arg(fieldvec.to_redis_args()),
        )?;

        Ok(())
    }

    //LPUSH and LPOP and LPUSHX and LSET
    pub fn opt_lpush_lpop_lpushx_lset(&mut self) -> RedisResult<()> {
        let lpush = "lpush_".to_string() + &*self.key_suffix.clone();
        let lpushx = "lpushx_".to_string() + &*self.key_suffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        let cmd_lpush = redis::cmd("lpush");
        let cmd_lset = redis::cmd("lset");
        let cmd_lpushx = redis::cmd("lpushx");

        for i in 0..self.loopstep {
            let element = lpush.clone() + &*i.to_string();
            elementsvec.push(element);
        }

        self.redis_conn.req_command(
            cmd_lpush
                .clone()
                .arg(lpush.clone())
                .arg(elementsvec.to_redis_args()),
        )?;

        for i in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_lset
                        .clone()
                        .arg(lpush.clone())
                        .arg(rng.gen_range(0..self.loopstep))
                        .arg(lpush.clone() + &*i.to_string()),
                )?;
            }
        }

        self.redis_conn.req_command(
            cmd_lpushx
                .clone()
                .arg(lpushx.clone())
                .arg(elementsvec.clone()),
        )?;

        self.redis_conn.req_command(
            cmd_lpushx
                .clone()
                .arg(lpush.clone())
                .arg(elementsvec.clone()),
        )?;

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(lpush.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        Ok(())
    }

    //LREM and LTRIM and LINSERT
    pub fn opt_lrem_ltrim_linsert(&mut self) -> RedisResult<()> {
        let lrem = "lrem_".to_string() + &*self.key_suffix.clone();
        let ltrim = "ltrime".to_string() + &*self.key_suffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        let cmd_rpush = redis::cmd("rpush");
        let cmd_lrem = redis::cmd("lrem");
        let cmd_ltrime = redis::cmd("ltrim");
        let cmd_linsert = redis::cmd("linsert");
        for i in 0..self.loopstep {
            elementsvec.push(lrem.clone() + &*i.to_string());
        }

        self.redis_conn
            .req_command(cmd_rpush.clone().arg(lrem.clone()).arg(elementsvec.clone()))?;
        self.redis_conn.req_command(
            cmd_rpush
                .clone()
                .arg(ltrim.clone())
                .arg(elementsvec.clone()),
        )?;

        for i in 0..self.loopstep {
            let mut op = "BEFORE";
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                op = "AFTER";
            }

            let pivot = elementsvec
                .get(rng.gen_range(0..self.loopstep))
                .unwrap_or(&String::from(""))
                .clone();

            self.redis_conn.req_command(
                cmd_linsert
                    .clone()
                    .arg(lrem.clone())
                    .arg(op)
                    .arg(pivot.clone().to_redis_args())
                    .arg(lrem.clone() + &*i.to_string()),
            )?;
        }

        let position = 2 * rng.gen_range(0..self.loopstep) as isize - self.loopstep as isize;

        self.redis_conn.req_command(
            cmd_lrem
                .clone()
                .arg(lrem.clone())
                .arg(position)
                .arg(lrem.clone() + &*rng.gen_range(0..self.loopstep).to_string()),
        )?;

        self.redis_conn.req_command(
            cmd_ltrime
                .clone()
                .arg(ltrim.clone())
                .arg(rng.gen_range(0..self.loopstep))
                .arg(position),
        )?;
        Ok(())
    }

    //RPUSH RPUSHX RPOP rpoplpush
    pub fn opt_rpush_rpushx_rpop_rpoplpush(&mut self) -> RedisResult<()> {
        let rpush = "rpush_".to_string() + &*self.key_suffix.clone();
        let rpushx = "rpushx_".to_string() + &*self.key_suffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        for i in 0..self.loopstep {
            let element = rpush.clone() + &*i.to_string();
            elementsvec.push(element);
        }

        let cmd_rpush = redis::cmd("rpush");
        let cmd_rpushx = redis::cmd("rpushx");
        let cmd_rpoplpush = redis::cmd("rpoplpush");
        let cmd_rpop = redis::cmd("rpop");

        self.redis_conn.req_command(
            cmd_rpush
                .clone()
                .arg(rpush.clone())
                .arg(elementsvec.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_rpushx
                .clone()
                .arg(rpushx.clone())
                .arg(elementsvec.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_rpushx
                .clone()
                .arg(rpush.clone())
                .arg(elementsvec.clone()),
        )?;

        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn
                    .req_command(cmd_rpop.clone().arg(rpush.clone()))?;
            }
        }

        //rpoplpush 操作同一个key相当于将列表逆转
        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn
                    .req_command(cmd_rpoplpush.clone().arg(rpush.clone()).arg(rpush.clone()))?;
            }
        }

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(rpush.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(rpushx.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //  LMOVE BLMOVE LMPOP BLMPOP
    pub fn opt_lmove_blmove_lmpop_blmpop(&mut self) -> RedisResult<()> {
        let lmove = "lmove_".to_string() + &*self.key_suffix.clone();
        let blmove = "blmove_".to_string() + &*self.key_suffix.clone();
        let lmpop = "lmpop_".to_string() + &*self.key_suffix.clone();
        let blmpop = "blmpop_".to_string() + &*self.key_suffix.clone();

        let cmd_rpush = redis::cmd("rpush");
        let cmd_lmove = redis::cmd("lmove");
        let cmd_blmove = redis::cmd("blmove");
        let cmd_lmpop = redis::cmd("lmpop");
        let cmd_blmpop = redis::cmd("blmpop");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_rpush
                    .clone()
                    .arg(lmove.clone())
                    .arg(lmove.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_rpush
                    .clone()
                    .arg(blmove.clone())
                    .arg(blmove.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_rpush
                    .clone()
                    .arg(lmpop.clone())
                    .arg(lmpop.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_rpush
                    .clone()
                    .arg(blmpop.clone())
                    .arg(blmpop.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn.req_command(
            cmd_lmove
                .clone()
                .arg(lmove.clone())
                .arg(lmove.clone() + &*"_d".to_string())
                .arg("RIGHT")
                .arg("LEFT"),
        )?;
        self.redis_conn.req_command(
            cmd_lmove
                .clone()
                .arg(lmove.clone())
                .arg(lmove.clone() + &*"_d".to_string())
                .arg("LEFT")
                .arg("RIGHT"),
        )?;

        self.redis_conn.req_command(
            cmd_blmove
                .clone()
                .arg(blmove.clone())
                .arg(blmove.clone() + &*"_d".to_string())
                .arg("RIGHT")
                .arg("LEFT")
                .arg(10),
        )?;
        self.redis_conn.req_command(
            cmd_blmove
                .clone()
                .arg(blmove.clone())
                .arg(blmove.clone() + &*"_d".to_string())
                .arg("LEFT")
                .arg("RIGHT")
                .arg(10),
        )?;

        self.redis_conn
            .req_command(cmd_lmpop.clone().arg(1).arg(lmpop.clone()).arg("LEFT"))?;
        self.redis_conn
            .req_command(cmd_lmpop.clone().arg(1).arg(lmpop.clone()).arg("RIGHT"))?;

        self.redis_conn.req_command(
            cmd_blmpop
                .clone()
                .arg(10)
                .arg(1)
                .arg(blmpop.clone())
                .arg("LEFT")
                .arg("COUNT")
                .arg(1),
        )?;
        self.redis_conn.req_command(
            cmd_blmpop
                .clone()
                .arg(10)
                .arg(1)
                .arg(blmpop.clone())
                .arg("RIGHT")
                .arg("COUNT")
                .arg(1),
        )?;

        Ok(())
    }

    //BLPOP BRPOP BRPOPLPUSH
    pub fn opt_blpop_brpop_brpoplpush(&mut self) -> RedisResult<()> {
        let blpop = "blpop_".to_string() + &*self.key_suffix.clone();
        let brpop = "brpop_".to_string() + &*self.key_suffix.clone();
        let mut rng = rand::thread_rng();
        let mut elementvec = vec![];
        for i in 0..self.loopstep {
            let element = blpop.clone() + &*i.to_string();
            elementvec.push(element);
        }

        let cmd_rpush = redis::cmd("rpush");
        let cmd_blpop = redis::cmd("blpop");
        let cmd_brpop = redis::cmd("brpop");
        let cmd_brpoplpush = redis::cmd("brpoplpush");

        self.redis_conn
            .req_command(cmd_rpush.clone().arg(blpop.clone()).arg(elementvec.clone()))?;
        self.redis_conn
            .req_command(cmd_rpush.clone().arg(brpop.clone()).arg(elementvec.clone()))?;

        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_brpop
                        .clone()
                        .arg(brpop.clone())
                        .arg(self.expire.clone()),
                )?;
                self.redis_conn.req_command(
                    cmd_blpop
                        .clone()
                        .arg(blpop.clone())
                        .arg(self.expire.clone()),
                )?;
            }
        }

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(blpop.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(brpop.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_brpoplpush
                        .clone()
                        .arg(blpop.clone())
                        .arg(blpop.clone())
                        .arg(10),
                )?;
            }
        }

        Ok(())
    }

    //SADD SMOVE SPOP SREM
    pub fn opt_sadd_smove_spop_srem(&mut self) -> RedisResult<()> {
        let sadd = "sadd_".to_string() + &*self.key_suffix.clone();
        let smove = "smove_".to_string() + &*self.key_suffix.clone();
        let spop = "spop_".to_string() + &*self.key_suffix.clone();
        let srem = "srem_".to_string() + &*self.key_suffix.clone();

        let cmd_sadd = redis::cmd("sadd");
        let cmd_smove = redis::cmd("smove");
        let cmd_spop = redis::cmd("spop");
        let cmd_srem = redis::cmd("srem");

        let mut rng = rand::thread_rng();

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_sadd
                    .clone()
                    .arg(sadd.clone())
                    .arg(sadd.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_sadd
                    .clone()
                    .arg(smove.clone())
                    .arg(smove.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_sadd
                    .clone()
                    .arg(spop.clone())
                    .arg(spop.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_sadd
                    .clone()
                    .arg(srem.clone())
                    .arg(srem.clone() + &*i.to_string()),
            )?;
        }

        for i in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn
                    .req_command(cmd_spop.clone().arg(spop.clone()))?;
                self.redis_conn.req_command(
                    cmd_srem
                        .clone()
                        .arg(srem.clone())
                        .arg(srem.clone() + &*i.to_string()),
                )?;
                self.redis_conn.req_command(
                    cmd_smove
                        .clone()
                        .arg(smove.clone())
                        .arg(smove.clone())
                        .arg(smove.clone() + &*i.to_string()),
                )?;
            }
        }

        let cmd_del = redis::cmd("del");
        let cmd_expire = redis::cmd("expire");
        self.redis_conn
            .req_command(cmd_del.clone().arg(sadd.clone()))?;

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(spop.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(srem.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(smove.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        Ok(())
    }

    // SDIFFSTORE SINTERSTORE SUNIONSTORE
    // 集群模式下key分布在不同节点会报错(error) CROSSSLOT Keys in request don't hash to the same slot
    pub fn opt_sdiffstore_sinterstore_sunionstore(&mut self) -> RedisResult<()> {
        let sdiff1 = "sdiff1_".to_string() + &*self.key_suffix.clone();
        let sdiff2 = "sdiff2_".to_string() + &*self.key_suffix.clone();
        let sdiffstore = "sdiffstore_".to_string() + &*self.key_suffix.clone();
        let sinterstore = "sinterstore_".to_string() + &*self.key_suffix.clone();
        let sunionstore = "sunionstore_".to_string() + &*self.key_suffix.clone();

        let mut rng = rand::thread_rng();

        let cmd_sadd = redis::cmd("sadd");
        let cmd_del = redis::cmd("del");
        let cmd_sdiffstore = redis::cmd("sdiffstore");
        let cmd_sintersore = redis::cmd("sinterstore");
        let cmd_sunionstore = redis::cmd("sunionstore");

        for _ in 0..self.loopstep {
            self.redis_conn
                .req_command(cmd_sadd.clone().arg(sdiff1.clone()).arg(
                    self.key_suffix.clone() + &*rng.gen_range(0..2 * self.loopstep).to_string(),
                ))?;
            self.redis_conn
                .req_command(cmd_sadd.clone().arg(sdiff2.clone()).arg(
                    self.key_suffix.clone() + &*rng.gen_range(0..2 * self.loopstep).to_string(),
                ))?;
        }

        self.redis_conn.req_command(
            cmd_sdiffstore
                .clone()
                .arg(sdiffstore.clone())
                .arg(sdiff1.clone())
                .arg(sdiff2.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_sintersore
                .clone()
                .arg(sinterstore.clone())
                .arg(sdiff1.clone())
                .arg(sdiff2.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_sunionstore
                .clone()
                .arg(sunionstore.clone())
                .arg(sdiff1.clone())
                .arg(sdiff2.clone()),
        )?;

        let cmd_expire = redis::cmd("expire");

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(sdiffstore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(sinterstore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(sunionstore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        self.redis_conn
            .req_command(cmd_del.clone().arg(sdiff1.clone()).arg(sdiff2.clone()))?;

        Ok(())
    }

    //ZADD ZINCRBY ZREM
    pub fn opt_zadd_zincrby_zerm(&mut self) -> RedisResult<()> {
        let zadd = "zadd_".to_string() + &*self.key_suffix.clone();
        let zincrby = "zincrby_".to_string() + &*self.key_suffix.clone();
        let zrem = "zrem_".to_string() + &*self.key_suffix.clone();
        let mut rng = rand::thread_rng();

        let cmd_zadd = redis::cmd("zadd");
        let cmd_zincrby = redis::cmd("zincrby");
        let cmd_zrem = redis::cmd("zrem");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zadd.clone())
                    .arg(i)
                    .arg(zadd.clone() + &*i.clone().to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zincrby.clone())
                    .arg(i)
                    .arg(zincrby.clone() + &*i.clone().to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zrem.clone())
                    .arg(i)
                    .arg(zrem.clone() + &*i.clone().to_string()),
            )?;
        }

        for _ in 0..self.loopstep {
            if rng.gen_range(0..self.loopstep) % 2 == 0 {
                self.redis_conn.req_command(
                    cmd_zincrby
                        .clone()
                        .arg(zincrby.clone())
                        .arg(rng.gen_range(0..2 * self.loopstep) as isize - self.loopstep as isize)
                        .arg(zadd.clone() + &*rng.gen_range(0..self.loopstep).to_string()),
                )?;
                self.redis_conn.req_command(
                    cmd_zrem
                        .clone()
                        .arg(zrem.clone())
                        .arg(zadd.clone() + &*rng.gen_range(0..self.loopstep).to_string()),
                )?;
            }
        }
        let cmd_expire = redis::cmd("expire");

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zincrby.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zrem.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //ZPOPMAX ZPOPMIN
    pub fn opt_zpopmax_zpopmin(&mut self) -> RedisResult<()> {
        let zpopmax = "zpopmax_".to_string() + &*self.key_suffix.clone();
        let zpopmin = "zpopmin_".to_string() + &*self.key_suffix.clone();
        let mut rng = rand::thread_rng();

        let cmd_zadd = redis::cmd("zadd");
        let cmd_zpopmax = redis::cmd("zpopmax");
        let cmd_zpopmin = redis::cmd("zpopmin");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zpopmax.clone())
                    .arg(i)
                    .arg(zpopmax.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zpopmin.clone())
                    .arg(i)
                    .arg(zpopmin.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn.req_command(
            cmd_zpopmax
                .clone()
                .arg(zpopmax.clone())
                .arg(&*rng.gen_range(0..self.loopstep).to_string()),
        )?;
        self.redis_conn.req_command(
            cmd_zpopmin
                .clone()
                .arg(zpopmin.clone())
                .arg(&*rng.gen_range(0..self.loopstep).to_string()),
        )?;

        let cmd_expire = redis::cmd("expire");

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zpopmax.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zpopmin.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    // ZMPOP BZMPOP
    pub fn opt_zmpop_bzmpop(&mut self) -> RedisResult<()> {
        let zmpop = "zmpop_".to_string() + &*self.key_suffix.clone();
        let bzmpop = "bzmpop_".to_string() + &*self.key_suffix.clone();

        let cmd_zadd = redis::cmd("zadd");
        let cmd_zmpop = redis::cmd("zmpop");
        let cmd_bzmpop = redis::cmd("bzmpop");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zmpop.clone())
                    .arg(i)
                    .arg(zmpop.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(bzmpop.clone())
                    .arg(i)
                    .arg(bzmpop.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn.req_command(
            cmd_zmpop
                .clone()
                .arg(1 as isize)
                .arg(zmpop.clone())
                .arg("max")
                .arg("count")
                .arg(1 as isize),
        )?;
        self.redis_conn.req_command(
            cmd_zmpop
                .clone()
                .arg(1 as isize)
                .arg(zmpop.clone())
                .arg("min")
                .arg("count")
                .arg(1 as isize),
        )?;

        self.redis_conn.req_command(
            cmd_bzmpop
                .clone()
                .arg(10 as isize)
                .arg(1 as isize)
                .arg(bzmpop.clone())
                .arg("max")
                .arg("count")
                .arg(1 as isize),
        )?;
        self.redis_conn.req_command(
            cmd_bzmpop
                .clone()
                .arg(10 as isize)
                .arg(1 as isize)
                .arg(bzmpop.clone())
                .arg("min")
                .arg("count")
                .arg(1 as isize),
        )?;

        Ok(())
    }

    // BZPOPMAX BZPOPMIN
    pub fn opt_bzpopmax_bzpopmin(&mut self) -> RedisResult<()> {
        let bzpopmax = "bzpopmax_".to_string() + &*self.key_suffix.clone();
        let bzpopmin = "bzpopmin_".to_string() + &*self.key_suffix.clone();

        let cmd_zadd = redis::cmd("zadd");
        let cmd_bzpopmax = redis::cmd("bzpopmax");
        let cmd_bzpopmin = redis::cmd("bzpopmin");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(bzpopmax.clone())
                    .arg(i)
                    .arg(bzpopmax.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(bzpopmin.clone())
                    .arg(i)
                    .arg(bzpopmin.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn
            .req_command(cmd_bzpopmax.clone().arg(bzpopmax.clone()).arg(10 as usize))?;
        self.redis_conn
            .req_command(cmd_bzpopmin.clone().arg(bzpopmin.clone()).arg(10 as usize))?;

        let cmd_expire = redis::cmd("expire");

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bzpopmax.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(bzpopmin.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    //ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE
    pub fn opt_zremrangebylex_zremrangebyrank_zremrangebyscore(&mut self) -> RedisResult<()> {
        let zremrangebylex = "zremrangebylex_".to_string() + &*self.key_suffix.clone();
        let zremrangebyrank = "zremrangebyrank_".to_string() + &*self.key_suffix.clone();
        let zremrangebyscore = "zremrangebyscore_".to_string() + &*self.key_suffix.clone();
        let mut rng = rand::thread_rng();

        let cmd_zadd = redis::cmd("zadd");
        let cmd_zremrangebylex = redis::cmd("zremrangebylex");
        let cmd_zremrangebyrank = redis::cmd("zremrangebyrank");
        let cmd_zremrangebyscore = redis::cmd("zremrangebyscore");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zremrangebylex.clone())
                    .arg(i)
                    .arg(zremrangebylex.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zremrangebyrank.clone())
                    .arg(i)
                    .arg(zremrangebyrank.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zremrangebyscore.clone())
                    .arg(i)
                    .arg(zremrangebyscore.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn.req_command(
            cmd_zremrangebylex
                .clone()
                .arg(zremrangebylex.clone())
                .arg("[".to_string() + &*zremrangebylex.clone() + &*0.to_string())
                .arg(
                    "[".to_string()
                        + &*zremrangebylex.clone()
                        + &*rng.gen_range(0..self.loopstep).to_string(),
                ),
        )?;
        self.redis_conn.req_command(
            cmd_zremrangebyrank
                .clone()
                .arg(zremrangebyrank.clone())
                .arg(
                    (rng.gen_range(0..2 * self.loopstep) as isize - self.loopstep as isize)
                        .to_redis_args(),
                )
                .arg(
                    (rng.gen_range(0..2 * self.loopstep) as isize - self.loopstep as isize)
                        .to_redis_args(),
                ),
        )?;
        self.redis_conn.req_command(
            cmd_zremrangebyscore
                .clone()
                .arg(zremrangebyscore.clone())
                .arg(rng.gen_range(0..self.loopstep))
                .arg(rng.gen_range(0..self.loopstep)),
        )?;

        let cmd_expire = redis::cmd("expire");

        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zremrangebylex.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zremrangebyrank.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zremrangebyscore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    // BO_ZUNIONSTORE_ZINTERSTORE,集群模式下key分布在不同节点会报错(error) CROSSSLOT Keys in request don't hash to the same slot
    pub fn opt_zunionstore_zinterstore(&mut self) -> RedisResult<()> {
        let zset1 = "zset1_".to_string() + &*self.key_suffix.clone();
        let zset2 = "zset2_".to_string() + &*self.key_suffix.clone();
        let zset3 = "zset3_".to_string() + &*self.key_suffix.clone();
        let zinterstore = "zinterstore_".to_string() + &*self.key_suffix.clone();
        let zunionstore = "zunionstore_".to_string() + &*self.key_suffix.clone();

        let cmd_del = redis::cmd("del");
        let cmd_zadd = redis::cmd("zadd");
        let cmd_zinterstore = redis::cmd("zinterstore");
        let cmd_zunionstore = redis::cmd("zunionstore");

        for i in 0..self.loopstep {
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zset1.clone())
                    .arg(i)
                    .arg(zset1.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zset2.clone())
                    .arg(i)
                    .arg(zset2.clone() + &*i.to_string()),
            )?;
            self.redis_conn.req_command(
                cmd_zadd
                    .clone()
                    .arg(zset3.clone())
                    .arg(i)
                    .arg(zset3.clone() + &*i.to_string()),
            )?;
        }

        self.redis_conn.req_command(
            cmd_zinterstore
                .clone()
                .arg(zinterstore.clone())
                .arg(3)
                .arg(zset1.clone())
                .arg(zset2.clone())
                .arg(zset3.clone()),
        )?;
        self.redis_conn.req_command(
            cmd_zunionstore
                .clone()
                .arg(zunionstore.clone())
                .arg(3)
                .arg(zset1.clone())
                .arg(zset2.clone())
                .arg(zset3.clone()),
        )?;

        self.redis_conn.req_command(
            cmd_del
                .clone()
                .arg(zset1.clone())
                .arg(zset2.clone())
                .arg(zset3.clone()),
        )?;

        let cmd_expire = redis::cmd("expire");
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zinterstore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;
        self.redis_conn.req_command(
            cmd_expire
                .clone()
                .arg(zunionstore.clone())
                .arg(&*self.expire.to_redis_args()),
        )?;

        Ok(())
    }

    // ToDo Stream 类型相关操作
}

#[cfg(test)]
mod test {

    // use enum_iterator::all;
    use crate::init_log;
    use crate::util::rand_string;

    use super::*;

    const rurl: &str = "redis://:redistest0102@114.67.76.82:16377/";

    //cargo test redisdatagen::gencmddata::test::test_exec_all -- --nocapture
    #[test]
    fn test_exec_all() {
        init_log();

        let subffix = rand_string(4);
        let client = redis::Client::open(rurl).unwrap();
        let mut conn = client.get_connection().unwrap();
        let cmd_select = redis::cmd("select");
        conn.req_command(cmd_select.clone().arg("1"));
        let mut opt = RedisOpt {
            redis_conn: &mut conn,
            redis_version: "4".to_string(),
            opt_type: OptType::OptAppend,
            key_suffix: subffix,
            loopstep: 10,
            expire: 100,
            db: 0,
            log_out: false,
        };
        opt.exec_all()
    }

    //cargo test redisdatagen::gencmddata::test::test_opt_append -- --nocapture
    #[test]
    fn test_opt_append() {
        let subffix = rand_string(16);
        let client = redis::Client::open(rurl).unwrap();
        let mut conn = client.get_connection().unwrap();

        let mut opt = RedisOpt {
            redis_conn: &mut conn,
            redis_version: "4".to_string(),
            opt_type: OptType::OptAppend,
            key_suffix: subffix,
            loopstep: 10,
            expire: 100,
            db: 0,
            log_out: false,
        };
        let r = opt.opt_append();
        println!("{:?}", r);
    }

    //cargo test redisdatagen::gencmddata::test::test_opt_bitop -- --nocapture
    #[test]
    fn test_opt_bitop() {
        let subffix = rand_string(16);
        let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16375/").unwrap();
        let mut conn = client.get_connection().unwrap();

        let mut opt = RedisOpt {
            redis_conn: &mut conn,
            redis_version: "4".to_string(),
            opt_type: OptType::OptAppend,
            key_suffix: subffix,
            loopstep: 10,
            expire: 100,
            db: 0,
            log_out: false,
        };
        let r = opt.opt_bitop();
        println!("{:?}", r);
    }

    //cargo test redisdatagen::gencmddata::test::test_opt_decr_decrby -- --nocapture
    #[test]
    fn test_opt_decr_decrby() {
        let subffix = rand_string(16);
        let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16375/").unwrap();
        let mut conn = client.get_connection().unwrap();

        let mut opt = RedisOpt {
            redis_conn: &mut conn,
            redis_version: "5".to_string(),
            opt_type: OptType::OptAppend,
            key_suffix: subffix,
            loopstep: 10,
            expire: 100,
            db: 0,
            log_out: false,
        };
        let r = opt.opt_decr_decrby();
        println!("{:?}", r);
    }
}
