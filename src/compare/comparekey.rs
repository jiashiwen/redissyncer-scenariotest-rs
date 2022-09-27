use crate::compare::compare_error::{CompareError, CompareErrorType};

use super::compare_error::CompareErrorReason;
use crate::util::{
    hget, hlen, key_exists, list_len, lrange, scard, sismumber, ttl, zcard, zscore, RedisKey,
    RedisKeyType,
};
use anyhow::{Error, Result};
use redis::{ConnectionLike, Iter, Value};

pub struct Comparer<'a> {
    pub sconn: &'a mut (dyn ConnectionLike + 'a),
    pub tconn: &'a mut (dyn ConnectionLike + 'a),
    pub ttl_diff: usize,
    pub batch: usize,
}

impl<'a> Comparer<'a> {
    pub fn new(s_conn: &'a mut dyn ConnectionLike, t_conn: &'a mut dyn ConnectionLike) -> Self {
        Self {
            sconn: s_conn,
            tconn: t_conn,
            ttl_diff: 1,
            batch: 10,
        }
    }

    pub fn compare_key(&mut self, key: RedisKey) -> Result<()> {
        return match key.key_type {
            RedisKeyType::TypeString => self.compare_string(key),
            RedisKeyType::TypeList => self.compare_list(key),
            RedisKeyType::TypeSet => self.compare_set(key),
            RedisKeyType::TypeZSet => self.compare_zset(key),
            RedisKeyType::TypeHash => self.compare_hash(key),
        };
    }
}

//ToDo 错误输出统一到CompareError，使用into 获得CompareError，便于存储错误信息
impl<'a> Comparer<'a> {
    pub fn compare_string(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        self.target_key_exists(key.key.clone())?;

        // source target 值是否相等
        self.string_value_equal(key.clone())?;

        // ttl差值是否在规定范围内
        self.ttl_diff(key.key.clone())?;

        Ok(())
    }

    pub fn compare_list(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        self.target_key_exists(key.key.clone())?;

        //比较 list 长度是否一致
        let (s_len, _t_len) = self.list_len_equal(&key)?;

        // 遍历source，核对target中相应的值是否一致
        let quotient = s_len / self.batch; // integer division, decimals are truncated
        let remainder = s_len % self.batch;

        let mut lrangeend: isize = 0;
        if quotient != 0 {
            for i in 0..quotient {
                if i == quotient - 1 {
                    lrangeend = (quotient * self.batch) as isize;
                } else {
                    lrangeend = ((self.batch - 1) + i * self.batch) as isize;
                }

                let s_elements = lrange(
                    key.key.clone(),
                    (0 + i * self.batch) as isize,
                    lrangeend,
                    self.sconn,
                )
                .map_err(|e| -> CompareError {
                    CompareError::from_str(
                        e.to_string().as_str(),
                        CompareErrorType::RedisConnectionErr,
                    )
                })?;
                let t_elements = lrange(
                    key.key.clone(),
                    (0 + i * self.batch) as isize,
                    lrangeend,
                    self.tconn,
                )
                .map_err(|e| -> CompareError {
                    CompareError::from_str(
                        e.to_string().as_str(),
                        CompareErrorType::RedisConnectionErr,
                    )
                })?;

                for i in 0..s_elements.len() {
                    let s_val = match s_elements.get(i) {
                        Some(s) => s.clone(),
                        None => "".to_string(),
                    };

                    let t_val = match t_elements.get(i) {
                        Some(s) => s.clone(),
                        None => "".to_string(),
                    };

                    if !s_val.eq(&t_val) {
                        let name = format!("name:{};index:{}", key.key.clone(), i.to_string());
                        let reason: CompareErrorReason = CompareErrorReason {
                            key_name: name,
                            source: Some(Value::Data(Vec::<u8>::from(s_val.clone()))),
                            target: Some(Value::Data(Vec::<u8>::from(t_val.clone()))),
                            source_db_num: self.sconn.get_db(),
                            target_db_num: self.tconn.get_db(),
                        };
                        return Err(Error::from(CompareError::from_reason(
                            reason,
                            CompareErrorType::ListIndexValueDiff,
                        )));
                    }
                }
            }
        }

        if remainder != 0 {
            let mut start: isize = 0;
            if quotient == 0 {
                start = 0;
            } else {
                start = (quotient * self.batch) as isize + 1;
            }

            let s_elements = lrange(
                key.key.clone(),
                start,
                (remainder + quotient * self.batch) as isize,
                self.sconn,
            )
            .map_err(|e| -> CompareError {
                CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
            })?;
            let t_elements = lrange(
                key.key.clone(),
                start,
                (remainder + quotient * self.batch) as isize,
                self.tconn,
            )
            .map_err(|e| -> CompareError {
                CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
            })?;
            for i in 0..s_elements.len() {
                let s_val = match s_elements.get(i) {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                };
                let t_val = match t_elements.get(i) {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                };

                if !s_val.eq(&t_val) {
                    let name = format!("name:{};index:{}", key.key.clone(), i.to_string());
                    let reason: CompareErrorReason = CompareErrorReason {
                        key_name: name,
                        source: Some(Value::Data(Vec::<u8>::from(s_val.clone()))),
                        target: Some(Value::Data(Vec::<u8>::from(t_val.clone()))),
                        source_db_num: self.sconn.get_db(),
                        target_db_num: self.tconn.get_db(),
                    };
                    return Err(Error::from(CompareError::from_reason(
                        reason,
                        CompareErrorType::ListIndexValueDiff,
                    )));
                }
            }
        }

        // ttl差值是否在规定范围内
        self.ttl_diff(key.key.clone())?;

        Ok(())
    }

    pub fn compare_set(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        self.target_key_exists(key.key.clone())?;

        // 比较 set 元素数量 是否一致
        self.set_members_number_equal(&key)?;

        // 遍历source，核对在target是否存在
        self.set_source_member_in_target(&key)?;

        // ttl差值是否在规定范围内
        self.ttl_diff(key.key.clone())?;
        Ok(())
    }

    pub fn compare_zset(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        self.target_key_exists(key.key.clone())?;

        // 比较 zset 元素数量 是否一致
        self.zset_members_number_equal(&key)?;

        // 遍历source，核对在target score 和 值是否一致
        self.zset_source_members_in_target(&key)?;

        // ttl差值是否在规定范围内
        self.ttl_diff(key.key.clone())?;
        Ok(())
    }

    pub fn compare_hash(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        self.target_key_exists(key.key.clone())?;

        // 比较 hash 元素数量 是否一致
        self.hash_len_equal(&key)?;

        // 遍历source，核对在target field 和 value 是否一致
        self.hash_field_vale_equal(&key)?;

        // ttl差值是否在规定范围内
        self.ttl_diff(key.key.clone())?;

        Ok(())
    }
}

impl<'a> Comparer<'a> {
    // fn target_key_exists(&mut self, key: String) -> Result<()> {
    fn target_key_exists(&mut self, key: String) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        if !t_exist {
            let reason = CompareErrorReason {
                key_name: key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(key.clone()))),
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::ExistsErr,
            )));
        }
        Ok(())
    }

    fn ttl_diff(&mut self, key: String) -> Result<()> {
        let s_ttl = ttl(key.clone(), self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let t_ttl = ttl(key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            let reason: CompareErrorReason = CompareErrorReason {
                key_name: key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(s_ttl.to_string()))),
                target: Some(Value::Data(Vec::<u8>::from(t_ttl.to_string()))),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::TTLDiff,
            )));
        }
        Ok(())
    }

    fn string_value_equal(&mut self, key: RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeString) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotString,
            )));
        }

        let sval = self
            .sconn
            .req_command(redis::cmd("get").arg(key.key.clone()))
            .map_err(|e| -> CompareError {
                CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
            })?;
        let tval = self
            .tconn
            .req_command(redis::cmd("get").arg(key.key.clone()))
            .map_err(|e| -> CompareError {
                CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
            })?;
        if !sval.eq(&tval) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: Some(sval.clone()),
                target: Some(tval.clone()),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::StringValueNotEqual,
            )));
        }
        Ok(())
    }

    fn list_len_equal(&mut self, key: &RedisKey) -> Result<(usize, usize)> {
        if !key.key_type.eq(&RedisKeyType::TypeList) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotList,
            )));
        }

        let s_len = list_len(key.key.clone(), self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let t_len = list_len(key.key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;

        if !s_len.eq(&t_len) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(s_len.to_string()))),
                target: Some(Value::Data(Vec::<u8>::from(t_len.to_string()))),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::ListLenDiff,
            )));
        }
        Ok((s_len, t_len))
    }

    // 比较 set 元素数量 是否一致
    fn set_members_number_equal(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeSet) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotSet,
            )));
        }

        let s_size = scard(key.key.clone(), self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let t_size = scard(key.key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;

        if !s_size.eq(&t_size) {
            let reason: CompareErrorReason = CompareErrorReason {
                key_name: key.key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(s_size.to_string()))),
                target: Some(Value::Data(Vec::<u8>::from(t_size.to_string()))),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::SetCardDiff,
            )));
        }
        Ok(())
    }

    // 遍历source，核对在target是否存在
    fn set_source_member_in_target(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeSet) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotSet,
            )));
        }
        let mut cmd_sscan = redis::cmd("sscan");
        cmd_sscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_sscan.iter(self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        for item in iter {
            let is = sismumber(key.key.clone(), item.clone(), self.tconn).map_err(
                |e| -> CompareError {
                    CompareError::from_str(
                        e.to_string().as_str(),
                        CompareErrorType::RedisConnectionErr,
                    )
                },
            )?;
            if !is {
                let reason: CompareErrorReason = CompareErrorReason {
                    key_name: key.key.clone(),
                    source: Some(Value::Data(Vec::<u8>::from(item.clone()))),
                    target: None,
                    source_db_num: self.sconn.get_db(),
                    target_db_num: self.tconn.get_db(),
                };
                return Err(Error::from(CompareError::from_reason(
                    reason,
                    CompareErrorType::SetMemberNotIn,
                )));
            }
        }
        Ok(())
    }

    fn zset_members_number_equal(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeZSet) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotZSet,
            )));
        }
        let s_size = zcard(key.key.clone(), self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let t_size = zcard(key.key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;

        if !s_size.eq(&t_size) {
            let reason: CompareErrorReason = CompareErrorReason {
                key_name: key.key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(s_size.to_string()))),
                target: Some(Value::Data(Vec::<u8>::from(t_size.to_string()))),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::ZSetCardDiff,
            )));
        }
        Ok(())
    }

    fn zset_source_members_in_target(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeZSet) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotZSet,
            )));
        }
        let mut cmd_zscan = redis::cmd("zscan");
        cmd_zscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_zscan.iter(self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let mut count = 0 as usize;
        let mut member = "".to_string();

        for item in iter {
            if count % 2 == 0 {
                member = item.clone();
            } else {
                let t_scroe = zscore(key.key.clone(), member.clone(), self.tconn).map_err(
                    |e| -> CompareError {
                        CompareError::from_str(
                            e.to_string().as_str(),
                            CompareErrorType::RedisConnectionErr,
                        )
                    },
                )?;

                if !t_scroe.to_string().eq(&item) {
                    let reason: CompareErrorReason = CompareErrorReason {
                        key_name: key.key.clone(),
                        source: Some(Value::Data(Vec::<u8>::from(item.clone()))),
                        target: Some(Value::Data(Vec::<u8>::from(t_scroe.to_string()))),
                        source_db_num: self.sconn.get_db(),
                        target_db_num: self.tconn.get_db(),
                    };
                    return Err(Error::from(CompareError::from_reason(
                        reason,
                        CompareErrorType::ZSetMemberScoreDiff,
                    )));
                }
            }
            count += 1;
        }
        Ok(())
    }

    fn hash_len_equal(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeHash) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotHash,
            )));
        }
        let s_len = hlen(key.key.clone(), self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let t_len = hlen(key.key.clone(), self.tconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        if s_len != t_len {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: Some(Value::Data(Vec::<u8>::from(s_len.to_string()))),
                target: Some(Value::Data(Vec::<u8>::from(t_len.to_string()))),
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::HashLenDiff,
            )));
        }
        Ok(())
    }

    fn hash_field_vale_equal(&mut self, key: &RedisKey) -> Result<()> {
        if !key.key_type.eq(&RedisKeyType::TypeHash) {
            let reason = CompareErrorReason {
                key_name: key.key.clone(),
                source: None,
                target: None,
                source_db_num: self.sconn.get_db(),
                target_db_num: self.tconn.get_db(),
            };
            return Err(Error::from(CompareError::from_reason(
                reason,
                CompareErrorType::KeyTypeNotHash,
            )));
        }
        let mut cmd_hscan = redis::cmd("hscan");
        cmd_hscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_hscan.iter(self.sconn).map_err(|e| -> CompareError {
            CompareError::from_str(e.to_string().as_str(), CompareErrorType::RedisConnectionErr)
        })?;
        let mut tag = true;
        let mut field = "".to_string();
        for item in iter {
            if tag {
                field = item;
                tag = false;
            } else {
                let t_val = hget(key.key.clone(), field.clone(), self.tconn).map_err(
                    |e| -> CompareError {
                        CompareError::from_str(
                            e.to_string().as_str(),
                            CompareErrorType::RedisConnectionErr,
                        )
                    },
                )?;
                if !item.eq(&t_val) {
                    let reason = CompareErrorReason {
                        key_name: key.key.clone(),
                        source: Some(Value::Data(Vec::<u8>::from(item.clone()))),
                        target: Some(Value::Data(Vec::<u8>::from(t_val.clone()))),
                        source_db_num: self.sconn.get_db(),
                        target_db_num: self.tconn.get_db(),
                    };
                    return Err(Error::from(CompareError::from_reason(
                        reason,
                        CompareErrorType::HashFieldValueDiff,
                    )));
                }
                tag = true;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::util::{get_instance_parameters, pttl};

    use super::*;
    use redis::{ConnectionLike, ToRedisArgs};

    static S_URL: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";
    static T_URL: &str = "redis://:redistest0102@114.67.120.120:16376/?timeout=1s";

    //cargo test compare::comparekey::test::test_comparer_string --  --nocapture
    #[test]
    fn test_comparer_string() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let _cmd_set = redis::cmd("set");

        let cmd_expire = redis::cmd("expire");

        let _ = comparer
            .sconn
            .req_command(cmd_expire.clone().arg("a").arg(100 as isize));
        let _ = comparer
            .tconn
            .req_command(cmd_expire.clone().arg("a").arg(100 as isize));
        let key_str = RedisKey {
            key: "a".to_string(),
            key_type: RedisKeyType::TypeString,
        };

        let r = comparer.compare_string(key_str);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_list --  --nocapture
    #[test]
    fn test_comparer_list() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);
        let cmd_rpush = redis::cmd("rpush");

        let key_list = RedisKey {
            key: "r1".to_string(),
            key_type: RedisKeyType::TypeList,
        };

        for i in 0..20 as i32 {
            let _ = comparer
                .sconn
                .req_command(cmd_rpush.clone().arg(key_list.key.clone()).arg(i));
            let _ = comparer
                .tconn
                .req_command(cmd_rpush.clone().arg(key_list.key.clone()).arg(i));
        }

        let r = comparer.compare_list(key_list);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_set --  --nocapture
    #[test]
    fn test_comparer_set() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let cmd_sadd = redis::cmd("sadd");

        let key_set = RedisKey {
            key: "s1".to_string(),
            key_type: RedisKeyType::TypeList,
        };

        for i in 0..100 as i32 {
            let _ = comparer
                .sconn
                .req_command(cmd_sadd.clone().arg(key_set.key.clone()).arg(i));
            let _ = comparer
                .tconn
                .req_command(cmd_sadd.clone().arg(key_set.key.clone()).arg(i));
        }

        let r = comparer.compare_set(key_set);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_sorted_set --  --nocapture
    #[test]
    fn test_comparer_sorted_set() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let cmd_zadd = redis::cmd("zadd");

        let key_zset = RedisKey {
            key: "z1".to_string(),
            key_type: RedisKeyType::TypeZSet,
        };

        for i in 0..20 as i32 {
            let _ = comparer.sconn.req_command(
                cmd_zadd
                    .clone()
                    .arg(key_zset.key.clone())
                    .arg(i)
                    .arg(key_zset.key.clone() + &*i.to_string()),
            );
            let _ = comparer.tconn.req_command(
                cmd_zadd
                    .clone()
                    .arg(key_zset.key.clone())
                    .arg(i)
                    .arg(key_zset.key.clone() + &*i.to_string()),
            );
        }

        let r = comparer.compare_zset(key_zset);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_hash --  --nocapture
    #[test]
    fn test_comparer_hash() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let cmd_hset = redis::cmd("hset");

        let key_hash = RedisKey {
            key: "h1".to_string(),
            key_type: RedisKeyType::TypeHash,
        };

        for i in 0..20 as i32 {
            let _ = comparer.sconn.req_command(
                cmd_hset
                    .clone()
                    .arg(key_hash.key.clone())
                    .arg(key_hash.key.clone() + &"__".to_string() + &*i.to_string())
                    .arg(key_hash.key.clone() + &*i.to_string()),
            );
            let _ = comparer.tconn.req_command(
                cmd_hset
                    .clone()
                    .arg(key_hash.key.clone())
                    .arg(key_hash.key.clone() + &"__".to_string() + &*i.to_string())
                    .arg(key_hash.key.clone() + &*i.to_string()),
            );
        }

        let r = comparer.compare_hash(key_hash);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_new_comparer --  --nocapture
    #[test]
    fn test_new_comparer() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut scon = client.get_connection().unwrap();
        let mut tcon = client.get_connection().unwrap();

        let comparer: Comparer = Comparer::new(&mut scon, &mut tcon);
        let cmd = redis::cmd("ping");
        let r = comparer.sconn.req_command(&cmd);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_key_exists --  --nocapture
    #[test]
    fn test_key_exists() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let exists = key_exists("hll".to_redis_args(), &mut conn);
        println!("{:?}", exists);
    }

    //cargo test compare::comparekey::test::test_ttl --  --nocapture
    #[test]
    fn test_ttl() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let ttl = ttl("hl", &mut conn);
        let pttl = pttl("hll", &mut conn);
        println!("ttl is:{:?} , pttl is: {:?}", ttl, pttl);
    }

    //cargo test compare::comparekey::test::test_get_instance_parameters --  --nocapture
    #[test]
    fn test_get_instance_parameters() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let k_type = get_instance_parameters(&mut conn).unwrap();
        for item in k_type.iter() {
            println!("{}:{}", item.0, item.1)
        }
    }

    //cargo test compare::comparekey::test::test_select --  --nocapture
    #[test]
    fn test_select() {
        let client = redis::Client::open(S_URL).unwrap();

        let mut conn = client.get_connection().unwrap();
        let mut cmd_select = redis::cmd("select");
        let _ = conn.req_command(&cmd_select.arg(2));
        let mut cmd_set = redis::cmd("set");
        cmd_set.arg("a").arg("aa").execute(&mut conn);
    }
}
