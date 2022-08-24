// 生成redis数据
mod generator;
mod gencmddata;
mod gendatabyduration;

pub use generator::gen_string;
pub use generator::RedisKeyType;
pub use gencmddata::RedisOpt;
pub use gencmddata::OptType;