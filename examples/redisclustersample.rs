use redis::{cluster::ClusterClientBuilder, Commands, ConnectionLike};

//ToDo 验证枚举 singleclient 和 clusterclient 以及如何访问
pub enum Redisclient {
    Single(redis::Client),
    Cluster(redis::cluster::ClusterClient),
}

fn main() {
    println!("redis cluster sample");
    let mut vec_rc: Vec<Redisclient> = vec![];

    let sc = redis::Client::open("redis://:redistest0102@114.67.76.82:16377").unwrap();
    let nodes = vec![
        "redis://114.67.76.82:26379/",
        "redis://114.67.76.82:26380/",
        "redis://114.67.76.82:26381/",
    ];

    let cc = ClusterClientBuilder::new(nodes)
        .password("redistest0102".to_string())
        .open()
        .unwrap();
    let _cc1 = cc.clone();

    vec_rc.push(Redisclient::Single(sc));
    vec_rc.push(Redisclient::Cluster(cc));

    let vec_conn = vec_rc
        .iter()
        .map(|c| -> Box<dyn ConnectionLike> {
            match c {
                Redisclient::Single(sc) => {
                    let conn = sc.get_connection().unwrap();
                    Box::new(conn)
                }
                Redisclient::Cluster(cc) => {
                    let conn = cc.get_connection().unwrap();
                    Box::new(conn)
                }
            }
        })
        .collect::<Vec<Box<dyn ConnectionLike>>>();

    for mut c in vec_conn {
        let r = c.as_mut().req_command(&redis::cmd("ping"));
        let _ = c
            .as_mut()
            .req_command(&redis::cmd("set").arg("a").arg("abcjse"));
        let r1: String = redis::cmd("get").arg("a").query(c.as_mut()).unwrap();
        println!("{:?}", r);
        println!("{:?}", r1);
    }

    let nodes = vec![
        "redis://114.67.76.82:26379/",
        "redis://114.67.76.82:26380/",
        "redis://114.67.76.82:26381/",
    ];

    let client = ClusterClientBuilder::new(nodes)
        .password("redistest0102".to_string())
        .open()
        .unwrap();

    let mut connection = client.get_connection().unwrap();

    let scan_iter = connection.scan::<String>();
    match scan_iter {
        Ok(iter) => {
            for iterm in iter {
                println!("{}", iterm);
            }
        }
        Err(e) => {
            eprintln!("{}", e);
        }
    }

    // for i in 0..10 {
    //     let key = "test".to_string() + &*i.to_string();
    //     println!("key is {}", key.clone());
    //     // let _: () = connection.set(key.to_redis_args(), "test_data").unwrap();
    //     let r = connection.req_command(
    //         redis::cmd("set")
    //             .arg("test".to_string() + &i.to_string())
    //             .arg("test"),
    //     );
    //     println!("r is {:?}", r);
    // }
}
