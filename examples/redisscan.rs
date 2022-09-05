use redis::{aio, cmd, Commands, Iter, RedisResult, ToRedisArgs};
use redis::{Client, Value};

use std::collections::HashMap;

use std::ops::Add;
use std::str::{from_utf8, Utf8Error};

use clap::arg;
use futures::stream::{Next, StreamExt};
use rand::Rng;
use redis::aio::Connection;
use redis::cluster::ClusterClient;
use redis::ConnectionLike;
use redis::{AsyncCommands, AsyncIter};
use std::string::String;
use std::time::Instant;

// fn main() -> redis::RedisResult<()> {
//     println!("redisscan");
//     let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16374/").unwrap();
//     let mut con = client.get_connection()?;
//     for i in 0..1000 as i32 {
//         let key = "k".to_owned() + &i.to_string();
//         let r = con.set(key.to_redis_args(), i.to_redis_args())?;
//         println!("{:?}", r);
//     }
//
//
//     // let rs = con.scan().unwrap();
//     // // let rs = redis::cmd("scan").cursor_arg(0).arg("MATCH *").arg("count 2").query::<u64>(&mut con);
//     // for item in rs {
//     //     println!("{:?}", item);
//     // }
//
//     Ok(())
// }

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let i: usize = 10;

    let mut rng = rand::thread_rng();

    // let cluster = ClusterClient::open("redis://127.0.0.1/").unwrap();
    let client = Client::open("redis://:redistest0102@114.67.76.82:16377/").unwrap();

    let mut con = client.get_connection().unwrap();

    let cmd_info = redis::cmd("info");

    let r = con.req_command(&cmd_info.clone().arg("server"))?;
    if let Value::Data(ref d) = r {
        let info = from_utf8(&*d)?;
        let s = info.split("\r\n");
        for kv in s {
            println!("{:?}", kv);
        }
    }

    // ToDo
    // 大数据量SCAN 测试
    let mut c = cmd("SCAN");
    c.cursor_arg(0);
    let i: Iter<Value> = c.iter(&mut con).unwrap();
    let mut tag = 0;
    for item in i {
        tag += 1;
        println!("{:?}", item);
    }
    println!("tag is {}", tag);

    let (k1, k2): (i32, i32) = redis::pipe()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("key_2")
        .arg(43)
        .ignore()
        .cmd("GET")
        .arg("key_1")
        .cmd("GET")
        .arg("key_2")
        .query(&mut con)
        .unwrap();
    println!("k1 is : {}; k2 is: {}", k1, k2);

    // ToDo
    // pipeline add command
    // pipeline 返回值处理
    let mut pip = redis::pipe();
    let mut tag = 0;

    for i in 0..5 {
        let key = "pip_key_".to_owned() + &*i.to_string();
        let mut cmd = redis::cmd("set");
        cmd.arg(key.clone().to_redis_args())
            .arg(key.to_redis_args());
        pip.add_command(cmd);
        tag += 1;
        if tag == 10 || i == 99 {
            // pip.execute(&mut con);
            let r: RedisResult<Value> = pip.query(&mut con);
            tag = 0;
            pip = redis::pipe();
            println!("{:?}", r);
        }
    }

    Ok(())
}

// get redis parameters
async fn get_instance_parameters<C>(con: &mut C) -> RedisResult<HashMap<String, String>>
where
    C: aio::ConnectionLike,
{
    let mut cmd_config = redis::cmd("config");
    let r_config = con
        .req_packed_command(&cmd_config.arg("get").arg("*"))
        .await?;
    let mut parameters = HashMap::new();
    if let Value::Bulk(ref values) = r_config {
        for i in (0..values.len()).step_by(2) {
            let key = values.get(i);
            let val = values.get(i + 1);

            if let Some(k) = key {
                if let Value::Data(ref s) = k {
                    match from_utf8(s) {
                        Ok(kstr) => {
                            let mut vstr = String::from("");
                            match val {
                                None => vstr = "".to_string(),
                                Some(vv) => {
                                    if let Value::Data(ref val) = vv {
                                        match from_utf8(val) {
                                            Ok(x) => vstr = x.to_string(),
                                            Err(_) => {}
                                        };
                                    }
                                }
                            };
                            parameters.insert(kstr.to_string(), vstr);
                        }
                        Err(_) => {}
                    }
                }
            }
        }
    }

    Ok(parameters)
}

// value to String
fn Value_to_String(val: &Value) -> String {
    return match val {
        Value::Nil => "nil".to_string(),
        Value::Int(val) => val.to_string(),
        Value::Data(ref val) => match from_utf8(val) {
            Ok(x) => x.to_string(),
            Err(_) => "".to_string(),
        },
        Value::Bulk(ref values) => "".to_string(),
        Value::Okay => "ok".to_string(),
        Value::Status(ref s) => s.to_string(),
    };
}

// scan
async fn scansample(mut conn: redis::aio::Connection) -> redis::RedisResult<()> {
    let mut count = 0;
    let mut iter: AsyncIter<String> = conn.scan().await?;
    while let Some(element) = iter.next_item().await {
        println!("{}", element);
        count += 1;
    }
    println!("{}", count);
    Ok(())
}

// 获取key类型
async fn key_type<C>(key: &str, con: &mut C) -> redis::RedisResult<String>
where
    C: crate::aio::ConnectionLike,
{
    let r: String = redis::cmd("TYPE").arg(key).query_async(con).await?;
    Ok(r)
}
