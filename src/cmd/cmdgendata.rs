use clap::{Arg, Command};

pub fn new_gendata_cmd() -> Command {
    clap::Command::new("gendata")
        .about("generate data for redis")
        .subcommand(gendata_bigkey_cmd())
        .subcommand(gendata_continuous_cmd())
}

pub fn gendata_bigkey_cmd() -> Command {
    clap::Command::new("bigkey")
        .about("generate bigkey for redis ")
        .subcommand(gendata_bigkey_template_cmd())
        .subcommand(gendata_bigkey_from_cmd())
}

pub fn gendata_bigkey_template_cmd() -> Command {
    clap::Command::new("template")
        .about("create a generate bigkey template file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_bigkey_from_cmd() -> Command {
    clap::Command::new("from")
        .about("generate big key from a yaml file")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .index(1)
            .required(true)])
}

pub fn gendata_continuous_cmd() -> Command {
    clap::Command::new("continuous")
        .about("Continuously producing data over a period of time")
        .subcommand(gendata_continuous_template_cmd())
        .subcommand(gendata_continuous_from_cmd())
}

pub fn gendata_continuous_template_cmd() -> Command {
    clap::Command::new("template")
        .about("create a generate continuous template file")
        .subcommand(gendata_continuous_template_single_cmd())
        .subcommand(gendata_continuous_template_cluster_cmd())
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_continuous_template_single_cmd() -> Command {
    clap::Command::new("single")
        .about("create a generate continuous template file for single redis instance")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_continuous_template_cluster_cmd() -> Command {
    clap::Command::new("cluster")
        .about("create a generate continuous template file for cluster redis instance")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

pub fn gendata_continuous_from_cmd() -> Command {
    clap::Command::new("from")
        .about("generate big key from a yaml file")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .index(1)
            .required(true)])
}
