use anyhow::Result;
use serde::de::DeserializeOwned;
use serde_yaml::from_str;
use std::fs;

pub fn from_yaml_file_to_struct<T>(path: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let contents = fs::read_to_string(path)?;
    let yml_struct = from_str::<T>(contents.as_str())?;
    Ok(yml_struct)
}
