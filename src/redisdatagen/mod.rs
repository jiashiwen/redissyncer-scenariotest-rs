// 生成redis数据
mod generator;
mod gencmddata;

pub use generator::gen_string;
pub use generator::RedisKeyType;