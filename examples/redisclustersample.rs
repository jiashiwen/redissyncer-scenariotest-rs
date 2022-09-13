use redis::{cluster::ClusterClientBuilder, Commands, ConnectionLike, ToRedisArgs};

fn main() {
    println!("redis cluster sample");

    // let nodes = vec![
    //     "redis://:redistest0102@114.67.76.82:26379/",
    //     "redis://:redistest0102@114.67.76.82:26380/",
    //     "redis://:redistest0102@114.67.76.82:26381/",
    // ];

    // let client = ClusterClient::open(nodes).unwrap();
    // let mut connection = client.get_connection().unwrap();
    let nodes = vec![
        "redis://114.67.76.82:26379/",
        "redis://114.67.76.82:26380/",
        "redis://114.67.76.82:26381/",
    ];
    // let ccb = ClusterClientBuilder::new(nodes);
    // ccb.password("redistest0102".to_string());

    let client = ClusterClientBuilder::new(nodes)
        .password("redistest0102".to_string())
        .open()
        .unwrap();

    let mut connection = client.get_connection().unwrap();
    for i in 0..10 {
        let key = "test".to_string() + &*i.to_string();
        println!("key is {}", key.clone());
        // let _: () = connection.set(key.to_redis_args(), "test_data").unwrap();
        let r = connection.req_command(
            redis::cmd("set")
                .arg("test".to_string() + &i.to_string())
                .arg("test"),
        );
        println!("r is {:?}", r);
    }

    // let _: () = connection.set("test", "test_data").unwrap();
    // let rv: String = connection.get("test").unwrap();

    // assert_eq!(rv, "test_data");
}
