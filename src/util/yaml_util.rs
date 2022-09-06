use anyhow::Result;
use serde::de::DeserializeOwned;
use serde_yaml::from_str;
use std::fs;
use serde::ser;

pub fn from_yaml_file_to_struct<T>(path: &str) -> Result<T>
    where
        T: DeserializeOwned,
{
    let contents = fs::read_to_string(path)?;
    let yml_struct = from_str::<T>(contents.as_str())?;
    Ok(yml_struct)
}

pub fn flash_struct_to_yaml_file<T>(val: &T, path: &str) -> Result<()>
    where
        T: ser::Serialize, {
    let yml = serde_yaml::to_string(val)?;
    fs::write(path, yml)?;
    Ok(())
}
