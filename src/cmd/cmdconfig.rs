use clap::{Arg, Command};

pub fn new_config_cmd() -> Command<'static> {
    clap::Command::new("config")
        .about("config")
        .subcommand(config_show_cmd())
        .subcommand(config_generate_default())
}

fn config_show_cmd() -> Command<'static> {
    clap::Command::new("show")
        .about("show some info ")
        .subcommand(config_show_info_cmd())
        .subcommand(config_show_all_cmd())
}

fn config_generate_default() -> Command<'static> {
    clap::Command::new("gendefault")
        .about("generate default config to file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

fn config_show_info_cmd() -> Command<'static> {
    clap::Command::new("default").about("show config template")
}

fn config_show_all_cmd() -> Command<'static> {
    clap::Command::new("current").about("show current configuration ")
}
