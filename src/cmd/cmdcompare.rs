use clap::{arg, Arg, Command};

pub fn new_compare_cmd() -> Command<'static> {
    clap::Command::new("compare")
        .about("compare redis data by description file")
        .subcommand(compare_sample_cmd())
        .subcommand(compare_execute_cmd())
        .subcommand(compare_yaml_sample_cmd())
}

pub fn compare_execute_cmd() -> Command<'static> {
    clap::Command::new("exec")
        .about("execute compare task by yaml description file")
        .arg(arg!(<file> "compare description file"))
}

pub fn compare_yaml_sample_cmd() -> Command<'static> {
    clap::Command::new("yaml").about("generate yaml sample by scenario")
}

fn compare_sample_cmd() -> Command<'static> {
    clap::Command::new("sample")
        .about("generate a sample compare yaml file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}
