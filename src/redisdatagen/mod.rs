// 生成redis数据
mod gencmddata;
mod gendatabyduration;
mod generator;

pub use gencmddata::OptType;
pub use gencmddata::RedisOpt;
pub use gendatabyduration::GeneratorByDuration;
pub use generator::gen_string;
pub use generator::GenerateBigKey;
pub use generator::RedisKeyType;
