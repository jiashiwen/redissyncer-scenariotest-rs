use clap::{arg, Command};

pub fn new_compare_cmd() -> Command<'static> {
    clap::Command::new("compare")
        .about("compare redis data by description file")
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
