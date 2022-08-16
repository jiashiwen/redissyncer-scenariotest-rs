// 通过命令组合生成key，尽量覆盖redis所有命令操作

use std::borrow::Borrow;
use std::collections::HashMap;
use rand::{random, Rng};
use redis::{cmd, Commands, ConnectionLike, RedisResult, ToRedisArgs, Value};
use crate::redisdatagen::generator::gen_key;
use crate::util::rand_string;

pub enum OptType {
    OPT_APPEND
}

pub struct RedisOpt<'a> {
    pub RedisConn: &'a mut dyn redis::ConnectionLike,
    pub RedisVersion: String,
    pub OptType: OptType,
    pub KeySuffix: String,
    pub Loopstep: usize,
    pub EXPIRE: usize,
    pub DB: usize,
}

impl<'a> RedisOpt<'a> {
    // append 操作
    pub fn opt_append(&mut self) -> RedisResult<()> {
        let key = "append_".to_string() + &*self.KeySuffix;
        for i in 0..self.Loopstep {
            let mut cmd_append = redis::cmd("append");
            let mut cmd_expire = redis::cmd("EXPIRE");
            let _ = self.RedisConn.req_command(
                cmd_append
                    .arg(key.clone().to_redis_args())
                    .arg(&*i.to_string()))?;
            let _ = self.RedisConn.req_command(
                cmd_expire
                    .arg(key.to_redis_args())
                    .arg(self.EXPIRE.to_redis_args()))?;
        }
        Ok(())
    }

    // bitmap操作
    pub fn opt_bitop(&mut self) -> RedisResult<()> {
        let opand = "opand_".to_string() + &*self.KeySuffix;
        let opor = "opor_".to_string() + &*self.KeySuffix;
        let opxor = "opxor_".to_string() + &*self.KeySuffix;
        let opnot = "opnot_".to_string() + &*self.KeySuffix;

        let mut opvec = vec![];
        for i in 0..self.Loopstep {
            let bitop = "bitop_".to_string() + &*self.KeySuffix + &*i.to_string();
            let mut cmd_set = redis::cmd("set");
            let _ = self.RedisConn.req_command(
                cmd_set
                    .arg(bitop.clone().to_redis_args())
                    .arg(bitop.clone().to_redis_args())
                    .arg("EX")
                    .arg(self.EXPIRE.to_redis_args())
            )?;
            opvec.push(bitop);
        }

        let mut cmd_bitop = redis::cmd("bitop");
        let mut cmd_expire = redis::cmd("expire");
        //执行 and 操作
        let _ = self.RedisConn.req_command(
            cmd_bitop.clone().arg("and".to_redis_args())
                .arg(opand.to_redis_args())
                .arg(opvec.to_redis_args())
        )?;
        let _ = self.RedisConn.req_command(
            cmd_expire.clone()
                .arg(opand.to_redis_args())
                .arg(self.EXPIRE.to_redis_args()))?;

        //执行 or 操作
        let _ = self.RedisConn.req_command(
            cmd_bitop.clone().arg("or".to_redis_args())
                .arg(opor.to_redis_args())
                .arg(opvec.to_redis_args())
        )?;
        let _ = self.RedisConn.req_command(
            cmd_expire.clone()
                .arg(opor.to_redis_args())
                .arg(self.EXPIRE.to_redis_args()))?;

        //执行 xor 操作
        let _ = self.RedisConn.req_command(
            cmd_bitop.clone().arg("xor".to_redis_args())
                .arg(opxor.to_redis_args())
                .arg(opvec.to_redis_args())
        )?;
        let _ = self.RedisConn.req_command(
            cmd_expire.clone()
                .arg(opxor.to_redis_args())
                .arg(self.EXPIRE.to_redis_args()))?;

        //执行 not 操作
        let _ = self.RedisConn.req_command(
            cmd_bitop.clone().arg("not".to_redis_args())
                .arg(opnot.clone().to_redis_args())
                .arg(opvec[0].to_redis_args())
        )?;
        let _ = self.RedisConn.req_command(
            cmd_expire.clone()
                .arg(opnot.to_redis_args())
                .arg(self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //DECR and DECRBY
    pub fn opt_decr_decrby(&mut self) -> RedisResult<()> {
        let decr = "decr_".to_string() + &*self.KeySuffix.clone().to_string();
        let mut cmd_set = redis::cmd("set");
        let _ = self.RedisConn.req_command(cmd_set
            .arg(decr.clone().to_redis_args())
            .arg(&*self.Loopstep.clone().to_redis_args())
            .arg("EX")
            .arg(&*self.EXPIRE.to_redis_args()))?;

        let mut cmd_decr = redis::cmd("decr");
        let _ = self.RedisConn.req_command(cmd_decr
            .arg(decr.clone().to_redis_args()))?;

        let mut cmd_decrby = redis::cmd("decrby");
        let mut rng = rand::thread_rng();
        let step = rng.gen_range(0..self.Loopstep);
        let _ = self.RedisConn.req_command(cmd_decrby
            .arg(decr.clone().to_redis_args())
            .arg(step.to_redis_args()))?;

        Ok(())
    }

    //INCR and INCRBY and INCRBYFLOAT
    pub fn opt_incr_incrby_incrbyfloat(&mut self) -> RedisResult<()> {
        let incr = "incr_".to_string() + &*self.KeySuffix.clone().to_string();
        let mut cmd_set = redis::cmd("set");
        let _ = self.RedisConn.req_command(cmd_set.arg(incr.clone())
            .arg("EX")
            .arg(&*self.EXPIRE.to_redis_args()))?;

        let mut cmd_incr = redis::cmd("incr");
        let mut cmd_incrby = redis::cmd("incrby");
        let mut cmd_incrbyfloat = redis::cmd("incrbyfloat");
        let mut rng = rand::thread_rng();
        let step = rng.gen_range(0..self.Loopstep);
        let step_float: f64 = rng.gen();
        let _ = self.RedisConn.req_command(cmd_incr.arg(incr.clone()))?;
        let _ = self.RedisConn.req_command(cmd_incrby
            .arg(incr.clone())
            .arg(step.to_redis_args()))?;
        let _ = self.RedisConn.req_command(cmd_incrbyfloat.arg(incr.clone())
            .arg(step_float.to_redis_args()))?;

        Ok(())
    }

    // MSET and MSETNX
    pub fn opt_mset_msetnx(&mut self) -> RedisResult<()> {
        let mut msetarry = vec![];
        let mut msetnxarry = vec![];

        let mset = "mset_".to_string() + &*self.KeySuffix.clone();
        let msetnx = "msetnx_".to_string() + &*self.KeySuffix.clone();


        for i in 0..self.Loopstep {
            msetarry.push(mset.clone() + &*i.to_string());
            msetarry.push(mset.clone() + &*i.to_string());
            msetnxarry.push(msetnx.clone() + &*i.to_string());
            msetnxarry.push(msetnx.clone() + &*i.to_string());
        }

        let mut cmd_mset = redis::cmd("mset");
        let mut cmd_msetnx = redis::cmd("msetnx");

        let _ = self.RedisConn.req_command(cmd_msetnx.clone()
            .arg(msetnx.clone()))?;
        let _ = self.RedisConn.req_command(cmd_mset.clone()
            .arg(mset.clone()))?;
        let _ = self.RedisConn.req_command(cmd_msetnx.clone()
            .arg(msetnx.clone()))?;

        let mut cmd_expire = redis::cmd("expire");

        for i in 0..self.Loopstep {
            let _ = self.RedisConn.req_command(cmd_expire.clone()
                .arg(mset.clone() + &*i.to_string())
                .arg(&*self.EXPIRE.to_redis_args()))?;
            let _ = self.RedisConn.req_command(cmd_expire.clone()
                .arg(msetnx.clone() + &*i.to_string())
                .arg(&*self.EXPIRE.to_redis_args()))?;
        }
        Ok(())
    }

    ////PSETEX and SETEX

    //PFADD
    pub fn opt_pfadd(&mut self) -> RedisResult<()> {
        let pfadd = "pfadd_".to_string() + &*self.KeySuffix.clone();
        let element_prefix = rand_string(4);
        let mut cmd_pfadd = redis::cmd("pfadd");

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_pfadd.clone()
                .arg(pfadd.clone())
                .arg(element_prefix.clone() + &*i.to_string()))?;
        }
        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(pfadd.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        Ok(())
    }

    //PFMERGE
    pub fn opt_pfmerge(&mut self) -> RedisResult<()> {
        let pfadd = "pfadd_".to_string() + &*self.KeySuffix.clone();
        let pfmerge = "pfmerge_".to_string() + &*self.KeySuffix.clone();
        let mut pfvec = vec![];

        let mut cmd_pfadd = redis::cmd("pfadd");
        let mut cmd_pfmerge = redis::cmd("pfmerge");
        let mut cmd_del = redis::cmd("del");

        for i in 0..self.Loopstep {
            let key = pfadd.clone() + &*i.to_string();
            self.RedisConn.req_command(cmd_pfadd.clone()
                .arg(key.clone())
                .arg(rand_string(4)))?;
            pfvec.push(key);
        }

        self.RedisConn.req_command(cmd_pfmerge
            .arg(pfmerge)
            .arg(pfvec.clone().to_redis_args()))?;

        for item in pfvec.iter() {
            self.RedisConn.req_command(cmd_del.arg(item))?;
        }
        Ok(())
    }

    //SET and SETNX
    pub fn opt_set_setnx(&mut self) -> RedisResult<()> {
        let set = "set_".to_string() + &*self.KeySuffix.clone();
        let setnx = "setnx_".to_string() + &*self.KeySuffix.clone();

        let mut cmd_set = redis::cmd("set");
        let mut cmd_setnx = redis::cmd("setnx");

        self.RedisConn.req_command(cmd_set.clone()
            .arg(set.clone())
            .arg(set.clone())
            .arg("EX")
            .arg(&*self.EXPIRE.clone().to_redis_args()))?;

        self.RedisConn.req_command(cmd_setnx.clone()
            .arg(setnx.clone())
            .arg(setnx.clone()))?;

        self.RedisConn.req_command(cmd_setnx.clone()
            .arg(setnx.clone())
            .arg(set.clone()))?;

        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(setnx.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //SETBIT
    pub fn opt_setbit(&mut self) -> RedisResult<()> {
        let setbit = "setbit_".to_string() + &*self.KeySuffix.clone();
        let mut cmd_setbit = redis::cmd("setbit");
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0..=self.Loopstep);
        self.RedisConn.req_command(cmd_setbit
            .arg(setbit.clone())
            .arg(offset)
            .arg(1))?;

        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(setbit)
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //SETRANGE
    pub fn opt_setrange(&mut self) -> RedisResult<()> {
        let setrange = "setrange_".to_string() + &*self.KeySuffix.clone();
        let mut cmd_setrange = redis::cmd("setrange");
        let mut cmd_set = redis::cmd("set");
        self.RedisConn.req_command(cmd_set
            .arg(setrange.clone())
            .arg(setrange.clone()))?;
        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(0..setrange.len());
        self.RedisConn.req_command(cmd_setrange
            .arg(setrange.clone())
            .arg(offset)
            .arg(rand_string(4)))?;
        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(setrange)
            .arg(&*self.EXPIRE.to_redis_args()))?;
        Ok(())
    }

    //HINCRBY and HINCRBYFLOAT
    pub fn opt_hincrby_hincrbyfloat(&mut self) -> RedisResult<()> {
        let hincrby = "hincrby_".to_string() + &*self.KeySuffix.clone();
        let hincrbyfloat = "hincrbyfloat_".to_string() + &*self.KeySuffix.clone();
        let mut cmd_hincrby = redis::cmd("hincrby");
        let mut cmd_hincrbyfloat = redis::cmd("hincrbyfloat");
        let mut rng = rand::thread_rng();
        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_hincrby.clone()
                .arg(hincrby.clone())
                .arg(rng.gen_range(0..self.Loopstep))
                .arg(rng.gen_range(0..self.Loopstep)))?;
            self.RedisConn.req_command(cmd_hincrbyfloat.clone()
                .arg(hincrbyfloat.clone())
                .arg(rng.gen_range(0..self.Loopstep))
                .arg(rng.gen::<f64>()))?;
        }
        Ok(())
    }

    //HSET HMSET HSETNX HDEL
    pub fn opt_hset_hmset_hsetnx_hdel(&mut self) -> RedisResult<()> {
        let hset = "hset_".to_string() + &*self.KeySuffix.clone();
        let hmset = "hmset_".to_string() + &*self.KeySuffix.clone();
        let mut rng = rand::thread_rng();

        // let mut fieldmap = HashMap::new();
        let mut fieldvec = vec![];
        let mut cmd_hset = redis::cmd("hset");
        let mut cmd_hmset = redis::cmd("hmset");
        let mut cmd_hsetnx = redis::cmd("hsetnx");
        let mut cmd_hdel = redis::cmd("hdel");

        for i in 0..self.Loopstep {
            let field = hmset.clone() + &*i.to_string();
            fieldvec.push(field.clone());
            fieldvec.push(field.clone());
            // fieldmap.set(field.clone(), field);
        }

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_hset.clone()
                .arg(hset.clone())
                .arg(hset.clone() + &*i.to_string())
                .arg(hset.clone() + &*i.to_string()))?;
        }

        self.RedisConn.req_command(cmd_hmset
            .arg(hmset.clone())
            .arg(fieldvec.to_redis_args()))?;

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_hsetnx.clone()
                .arg(hmset.clone())
                .arg(hmset.clone() + &*rng.gen_range(0..self.Loopstep).to_string())
                .arg(hmset.clone() + &*i.to_string()))?;
        }

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_hdel.clone()
                    .arg(hmset.clone())
                    .arg(hmset.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
            }
        }

        Ok(())
    }

    //LPUSH and LPOP and LPUSHX and LSET
    pub fn opt_lpush_lpop_lpushx_lset(&mut self) -> RedisResult<()> {
        let lpush = "lpush_".to_string() + &*self.KeySuffix.clone();
        let lpushx = "lpushx_".to_string() + &*self.KeySuffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        let mut cmd_lpush = redis::cmd("lpush");
        let mut cmd_lset = redis::cmd("lset");
        let cmd_lpushx = redis::cmd("lpushx");

        for i in 0..self.Loopstep {
            let element = lpush.clone() + &*i.to_string();
            elementsvec.push(element);
        }

        self.RedisConn.req_command(cmd_lpush
            .arg(lpush.clone())
            .arg(elementsvec.to_redis_args()))?;

        for i in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_lset.clone()
                    .arg(lpush.clone())
                    .arg(rng.gen_range(0..self.Loopstep))
                    .arg(lpush.clone() + &*i.to_string()))?;
            }
        }

        self.RedisConn.req_command(cmd_lpushx.clone()
            .arg(lpushx.clone())
            .arg(elementsvec.clone()))?;

        self.RedisConn.req_command(cmd_lpushx.clone()
            .arg(lpush.clone())
            .arg(elementsvec.clone()))?;

        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(lpush.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        Ok(())
    }

    //LREM and LTRIM and LINSERT
    pub fn opt_lrem_ltrim_linsert(&mut self) -> RedisResult<()> {
        let lrem = "lrem_".to_string() + &*self.KeySuffix.clone();
        let ltrim = "ltrime".to_string() + &*self.KeySuffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        let mut cmd_rpush = redis::cmd("rpush");
        let mut cmd_lrem = redis::cmd("lrem");
        let mut cmd_ltrime = redis::cmd("ltrim");
        let mut cmd_linsert = redis::cmd("linsert");
        for i in 0..self.Loopstep {
            elementsvec.push(lrem.clone() + &*i.to_string());
        }

        self.RedisConn.req_command(cmd_rpush.clone()
            .arg(lrem.clone())
            .arg(elementsvec.clone()))?;
        self.RedisConn.req_command(cmd_rpush.clone()
            .arg(ltrim.clone())
            .arg(elementsvec.clone()))?;

        for i in 0..self.Loopstep {
            let mut op = "BEFORE";
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                op = "AFTER";
            }

            let pivot = elementsvec.get(rng.gen_range(0..self.Loopstep))
                .unwrap_or(&String::from("")).clone();

            self.RedisConn.req_command(cmd_linsert.clone()
                .arg(lrem.clone())
                .arg(op)
                .arg(pivot.clone().to_redis_args())
                .arg(lrem.clone() + &*i.to_string()))?;
        }

        let position = 2 * rng.gen_range(0..self.Loopstep) as isize - self.Loopstep as isize;

        self.RedisConn.req_command(cmd_lrem.clone()
            .arg(lrem.clone())
            .arg(position)
            .arg(lrem.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;

        self.RedisConn.req_command(cmd_ltrime.clone()
            .arg(ltrim.clone())
            .arg(rng.gen_range(0..self.Loopstep))
            .arg(position))?;
        Ok(())
    }

    //RPUSH RPUSHX RPOP rpoplpush
    // ToDo add LMOVE LMPOP
    pub fn opt_rpush_rpushx_rpop_rpoplpush(&mut self) -> RedisResult<()> {
        let rpush = "rpush_".to_string() + &*self.KeySuffix.clone();
        let rpushx = "rpushx_".to_string() + &*self.KeySuffix.clone();
        let mut elementsvec = vec![];
        let mut rng = rand::thread_rng();

        for i in 0..self.Loopstep {
            let element = rpush.clone() + &*i.to_string();
            elementsvec.push(element);
        }

        let mut cmd_rpush = redis::cmd("rpush");
        let mut cmd_rpushx = redis::cmd("rpushx");
        let mut cmd_rpoplpush = redis::cmd("rpoplpush");
        let mut cmd_rpop = redis::cmd("rpop");

        self.RedisConn.req_command(cmd_rpush.clone()
            .arg(rpush.clone())
            .arg(elementsvec.clone()))?;
        self.RedisConn.req_command(cmd_rpushx.clone()
            .arg(rpushx.clone())
            .arg(elementsvec.clone()))?;
        self.RedisConn.req_command(cmd_rpushx.clone()
            .arg(rpush.clone())
            .arg(elementsvec.clone()))?;

        //rpoplpush 操作同一个key相当于将列表逆转
        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_rpoplpush.clone()
                    .arg(rpush.clone())
                    .arg(rpush.clone()))?;
            }
        }

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_rpop.clone()
                    .arg(rpush.clone()))?;
            }
        }

        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(rpush.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(rpushx.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //BLPOP BRPOP BRPOPLPUSH
    // ToDo add BLMOVE BLMPOP
    pub fn opt_blpop_brpop_brpoplpush(&mut self) -> RedisResult<()> {
        let blpop = "blpop_".to_string() + &*self.KeySuffix.clone();
        let brpop = "brpop_".to_string() + &*self.KeySuffix.clone();
        let mut rng = rand::thread_rng();
        let mut elementvec = vec![];
        for i in 0..self.Loopstep {
            let element = blpop.clone() + &*i.to_string();
            elementvec.push(element);
        }

        let mut cmd_rpush = redis::cmd("rpush");
        let mut cmd_blpop = redis::cmd("blpop");
        let mut cmd_brpop = redis::cmd("brpop");
        let mut cmd_brpoplpush = redis::cmd("brpoplpush");

        self.RedisConn.req_command(cmd_rpush.clone()
            .arg(blpop.clone())
            .arg(elementvec.clone()))?;
        self.RedisConn.req_command(cmd_rpush.clone()
            .arg(brpop.clone())
            .arg(elementvec.clone()))?;

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_brpoplpush.clone()
                    .arg(blpop.clone())
                    .arg(blpop.clone())
                    .arg(self.EXPIRE.clone()))?;
            }
        }

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_brpop.clone()
                    .arg(brpop.clone())
                    .arg(self.EXPIRE.clone()))?;
                self.RedisConn.req_command(cmd_blpop.clone()
                    .arg(blpop.clone())
                    .arg(self.EXPIRE.clone()))?;
            }
        }


        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(blpop.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(brpop.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //SADD SMOVE SPOP SREM
    pub fn opt_sadd_smove_spop_srem(&mut self) -> RedisResult<()> {
        let sadd = "sadd_".to_string() + &*self.KeySuffix.clone();
        let smove = "smove_".to_string() + &*self.KeySuffix.clone();
        let spop = "spop_".to_string() + &*self.KeySuffix.clone();
        let srem = "srem_".to_string() + &*self.KeySuffix.clone();

        let mut cmd_sadd = redis::cmd("sadd");
        let mut cmd_smove = redis::cmd("smove");
        let mut cmd_spop = redis::cmd("smove");
        let mut cmd_srem = redis::cmd("srem");

        let mut rng = rand::thread_rng();

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(sadd.clone())
                .arg(sadd.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(smove.clone())
                .arg(smove.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(spop.clone())
                .arg(spop.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(srem.clone())
                .arg(srem.clone() + &*i.to_string()))?;
        }

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_spop.clone()
                    .arg(spop.clone()))?;
                self.RedisConn.req_command(cmd_srem.clone()
                    .arg(srem.clone())
                    .arg(srem.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
                self.RedisConn.req_command(cmd_smove.clone()
                    .arg(smove.clone())
                    .arg(smove.clone())
                    .arg(smove.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
            }
        }

        let mut cmd_del = redis::cmd("del");
        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_del.clone()
            .arg(sadd.clone()))?;

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(spop.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(srem.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(smove.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        Ok(())
    }

    // SDIFFSTORE SINTERSTORE SUNIONSTORE
    // 集群模式下key分布在不同节点会报错(error) CROSSSLOT Keys in request don't hash to the same slot
    pub fn opt_sdiffstore_sinertstore_sunionstore(&mut self) -> RedisResult<()> {
        let sdiff1 = "sdiff1_".to_string() + &*self.KeySuffix.clone();
        let sdiff2 = "sdiff2_".to_string() + &*self.KeySuffix.clone();
        let sdiffstore = "sdiffstore_".to_string() + &*self.KeySuffix.clone();
        let sinterstore = "sinterstore_".to_string() + &*self.KeySuffix.clone();
        let sunionstore = "sunionstore_".to_string() + &*self.KeySuffix.clone();

        let mut rng = rand::thread_rng();

        let mut cmd_sadd = redis::cmd("sadd");
        let mut cmd_del = redis::cmd("del");
        let mut cmd_sdiffstore = redis::cmd("sdiffstore");
        let mut cmd_sintersore = redis::cmd("sinterstore");
        let mut cmd_sunionstore = redis::cmd("sunionstore");

        for _ in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(sdiff1.clone())
                .arg(self.KeySuffix.clone() + &*rng.gen_range(0..2 * self.Loopstep).to_string()))?;
            self.RedisConn.req_command(cmd_sadd.clone()
                .arg(sdiff2.clone())
                .arg(self.KeySuffix.clone() + &*rng.gen_range(0..2 * self.Loopstep).to_string()))?;
        }

        self.RedisConn.req_command(cmd_sdiffstore.clone()
            .arg(sdiffstore.clone())
            .arg(sdiff1.clone())
            .arg(sdiff2.clone()))?;
        self.RedisConn.req_command(cmd_sintersore.clone()
            .arg(sinterstore.clone())
            .arg(sdiff1.clone())
            .arg(sdiff2.clone()))?;
        self.RedisConn.req_command(cmd_sunionstore.clone()
            .arg(sunionstore.clone())
            .arg(sdiff1.clone())
            .arg(sdiff2.clone()))?;

        let mut cmd_expire = redis::cmd("expire");

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(sdiffstore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(sinterstore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(sunionstore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        self.RedisConn.req_command(cmd_del.clone()
            .arg(sdiff1.clone())
            .arg(sdiff2.clone()))?;

        Ok(())
    }

    //ZADD ZINCRBY ZREM
    pub fn opt_zadd_zincrby_zerm(&mut self) -> RedisResult<()> {
        let zadd = "zadd_".to_string() + &*self.KeySuffix.clone();
        let zincrby = "zincrby_".to_string() + &*self.KeySuffix.clone();
        let zrem = "zrem_".to_string() + &*self.KeySuffix.clone();
        let mut rng = rand::thread_rng();

        let mut cmd_zadd = redis::cmd("zadd");
        let mut cmd_zincrby = redis::cmd("zincrby");
        let mut cmd_zrem = redis::cmd("zrem");

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zadd.clone())
                .arg(i)
                .arg(zadd.clone() + &*i.clone().to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zincrby.clone())
                .arg(i)
                .arg(zincrby.clone() + &*i.clone().to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zrem.clone())
                .arg(i)
                .arg(zrem.clone() + &*i.clone().to_string()))?;
        }

        for _ in 0..self.Loopstep {
            if rng.gen_range(0..self.Loopstep) % 2 == 0 {
                self.RedisConn.req_command(cmd_zincrby.clone()
                    .arg(zincrby.clone())
                    .arg(rng.gen_range(0..2 * self.Loopstep) - self.Loopstep)
                    .arg(zadd.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
                self.RedisConn.req_command(cmd_zrem.clone()
                    .arg(zrem.clone())
                    .arg(zadd.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
            }
        }
        let mut cmd_expire = redis::cmd("expire");

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zincrby.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zrem.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;


        Ok(())
    }

    //ZPOPMAX ZPOPMIN
    pub fn opt_zpopmax_zpopmin(&mut self) -> RedisResult<()> {
        let zpopmax = "zpopmax_".to_string() + &*self.KeySuffix.clone();
        let zpopmin = "zpopmin_".to_string() + &*self.KeySuffix.clone();
        let mut rng = rand::thread_rng();

        let mut cmd_zadd = redis::cmd("zadd");
        let mut cmd_zpopmax = redis::cmd("zpopmax");
        let mut cmd_zpopmin = redis::cmd("zpopmin");

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zpopmax.clone())
                .arg(i)
                .arg(zpopmax.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zpopmin.clone())
                .arg(i)
                .arg(zpopmin.clone() + &*i.to_string()))?;
        }

        self.RedisConn.req_command(cmd_zpopmax.clone()
            .arg(zpopmax.clone())
            .arg(&*rng.gen_range(0..self.Loopstep).to_string()))?;
        self.RedisConn.req_command(cmd_zpopmin.clone()
            .arg(zpopmin.clone())
            .arg(&*rng.gen_range(0..self.Loopstep).to_string()))?;

        let mut cmd_expire = redis::cmd("expire");

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zpopmax.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zpopmin.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    //ToDo BZPOPMAX BZPOPMIN

    //ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE
    pub fn opt_zremrangebylex_zremrangebyrank_zermrangebyscore(&mut self) -> RedisResult<()> {
        let zremrangebylex = "zremrangebylex_".to_string() + &*self.KeySuffix.clone();
        let zremrangebyrank = "zremrangebyrank_".to_string() + &*self.KeySuffix.clone();
        let zremrangebyscore = "zremrangebyscore_".to_string() + &*self.KeySuffix.clone();
        let mut rng = rand::thread_rng();

        let mut cmd_zadd = redis::cmd("zadd");
        let mut cmd_zremrangebylex = redis::cmd("zremrangebylex");
        let mut cmd_zremrangebyrank = redis::cmd("zremrangebyrank");
        let mut cmd_zremrangebyscore = redis::cmd("zremrangebyscore");

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zremrangebylex.clone())
                .arg(i)
                .arg(zremrangebylex.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zremrangebyrank.clone())
                .arg(i)
                .arg(zremrangebyrank.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zremrangebyscore.clone())
                .arg(i)
                .arg(zremrangebyscore.clone() + &*i.to_string()))?;
        }

        self.RedisConn.req_command(cmd_zremrangebylex.clone()
            .arg(zremrangebylex.clone())
            .arg("[".to_string() + &*zremrangebylex.clone() + &*0.to_string())
            .arg("[".to_string() + &*zremrangebylex.clone() + &*rng.gen_range(0..self.Loopstep).to_string()))?;
        self.RedisConn.req_command(cmd_zremrangebyrank.clone()
            .arg(zremrangebyrank.clone())
            .arg((rng.gen_range(0..2 * self.Loopstep) - self.Loopstep).to_redis_args())
            .arg((rng.gen_range(0..2 * self.Loopstep) - self.Loopstep).to_redis_args()))?;
        self.RedisConn.req_command(cmd_zremrangebyscore.clone()
            .arg(zremrangebyscore.clone())
            .arg(rng.gen_range(0..self.Loopstep))
            .arg(rng.gen_range(0..self.Loopstep)))?;

        let mut cmd_expire = redis::cmd("expire");

        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zremrangebylex.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zremrangebyrank.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zremrangebyscore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }

    // BO_ZUNIONSTORE_ZINTERSTORE,集群模式下key分布在不同节点会报错(error) CROSSSLOT Keys in request don't hash to the same slot
    pub fn opt_zunionstore_zinterstore(&mut self) -> RedisResult<()> {
        let zset1 = "zset1_".to_string() + &*self.KeySuffix.clone();
        let zset2 = "zset2_".to_string() + &*self.KeySuffix.clone();
        let zset3 = "zset3_".to_string() + &*self.KeySuffix.clone();
        let zinterstore = "zinterstore_".to_string() + &*self.KeySuffix.clone();
        let zunionstore = "zunionstore_".to_string() + &*self.KeySuffix.clone();

        let mut cmd_del = redis::cmd("del");
        let mut cmd_zadd = redis::cmd("zadd");
        let mut cmd_zinterstore = redis::cmd("zinterstore");
        let mut cmd_zunionstore = redis::cmd("zunionstore");

        for i in 0..self.Loopstep {
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zset1.clone())
                .arg(i)
                .arg(zset1.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zset2.clone())
                .arg(i)
                .arg(zset2.clone() + &*i.to_string()))?;
            self.RedisConn.req_command(cmd_zadd.clone()
                .arg(zset3.clone())
                .arg(i)
                .arg(zset3.clone() + &*i.to_string()))?;
        }

        self.RedisConn.req_command(cmd_zinterstore.clone()
            .arg(zinterstore.clone())
            .arg(3)
            .arg(zset1.clone())
            .arg(zset2.clone())
            .arg(zset3.clone()))?;
        self.RedisConn.req_command(cmd_zunionstore.clone()
            .arg(zunionstore.clone())
            .arg(3)
            .arg(zset1.clone())
            .arg(zset2.clone())
            .arg(zset3.clone()))?;

        self.RedisConn.req_command(cmd_del.clone()
            .arg(zset1.clone())
            .arg(zset2.clone())
            .arg(zset3.clone()))?;


        let mut cmd_expire = redis::cmd("expire");
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zinterstore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;
        self.RedisConn.req_command(cmd_expire.clone()
            .arg(zunionstore.clone())
            .arg(&*self.EXPIRE.to_redis_args()))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Instant;
    use crate::util::rand_string;
    use super::*;

    //cargo test redisdatagen::gencmddata::test::test_opt_append -- --nocapture
    #[test]
    fn test_opt_append() {
        let subffix = rand_string(16);
        let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16376/").unwrap();
        let mut conn = client.get_connection().unwrap();

        let mut opt = RedisOpt {
            RedisConn: &mut conn,
            RedisVersion: "4".to_string(),
            OptType: OptType::OPT_APPEND,
            KeySuffix: subffix,
            Loopstep: 10,
            EXPIRE: 100,
            DB: 0,
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
            RedisConn: &mut conn,
            RedisVersion: "4".to_string(),
            OptType: OptType::OPT_APPEND,
            KeySuffix: subffix,
            Loopstep: 10,
            EXPIRE: 100,
            DB: 0,
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
            RedisConn: &mut conn,
            RedisVersion: "5".to_string(),
            OptType: OptType::OPT_APPEND,
            KeySuffix: subffix,
            Loopstep: 10,
            EXPIRE: 100,
            DB: 0,
        };
        let r = opt.opt_decr_decrby();
        println!("{:?}", r);
    }
}

// pub fn opt_append<C>(key_len: usize, hash_size: usize, conn: &mut C) -> RedisResult<()>
//     where
//         C: redis::ConnectionLike, {
//     let prefix = gen_key(RedisKeyType::TypeString, key_len);
//     for i in 0..keys {
//         let key = prefix.clone() + "_" + &*i.to_string();
//         let mut cmd_set = redis::cmd("set");
//         let r_set = conn
//             .req_command(&cmd_set.arg(key.to_redis_args()).arg(key.to_redis_args()))?;
//     }
//     Ok(())
// }