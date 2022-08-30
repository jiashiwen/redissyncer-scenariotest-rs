use clap::{Arg, Command};

pub fn new_gendata_cmd() -> Command<'static> {
    clap::Command::new("gendata")
        .about("generate data for redis")
        .subcommand(gendata_bigkey_cmd())
        .subcommand(gendata_continuous_cmd())
}

pub fn gendata_bigkey_cmd() -> Command<'static> {
    clap::Command::new("bigkey")
        .about("generate bigkey for redis ")
        .subcommand(gendata_bigkey_template_cmd())
        .subcommand(gendata_bigkey_from_cmd())
}

pub fn gendata_bigkey_template_cmd() -> Command<'static> {
    clap::Command::new("template")
        .about("create a generate bigkey template file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_bigkey_from_cmd() -> Command<'static> {
    clap::Command::new("from")
        .about("generate big key from a yaml file")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .index(1)
            .required(true)])
}

pub fn gendata_continuous_cmd() -> Command<'static> {
    clap::Command::new("continuous")
        .about("Continuously producing data over a period of time")
        .subcommand(gendata_continuous_template_cmd())
        .subcommand(gendata_continuous_from_cmd())
}

pub fn gendata_continuous_template_cmd() -> Command<'static> {
    clap::Command::new("template")
        .about("create a generate bigkey template file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_continuous_from_cmd() -> Command<'static> {
    clap::Command::new("from")
        .about("generate big key from a yaml file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}
