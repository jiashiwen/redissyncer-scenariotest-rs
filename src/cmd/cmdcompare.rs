use clap::{arg, Arg, Command};

pub fn new_compare_cmd() -> Command<'static> {
    clap::Command::new("compare")
        .about("compare redis data by description file")
        .subcommand(compare_sample_cmd())
        .subcommand(compare_execute_cmd())
}

fn compare_execute_cmd() -> Command<'static> {
    clap::Command::new("exec")
        .about("execute compare task by yaml description file")
        .arg(arg!(<file> "compare description file"))
}

fn compare_sample_cmd() -> Command<'static> {
    clap::Command::new("sample")
        .about("generate a sample compare yaml file")
        .subcommand(compare_sample_single2single_cmd())
        .subcommand(compare_sample_single2cluster_cmd())
        .subcommand(compare_sample_multisingle2single_cmd())
        .subcommand(compare_sample_cluster2cluster_cmd())
        .subcommand(compare_sample_multisingle2cluster_cmd())
}

fn compare_sample_single2single_cmd() -> Command<'static> {
    clap::Command::new("single2single")
        .about("generate a sample compare yaml file for scenario single2single")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

fn compare_sample_single2cluster_cmd() -> Command<'static> {
    clap::Command::new("single2cluster")
        .about("generate a sample compare yaml file for scenario single2cluster")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

fn compare_sample_multisingle2single_cmd() -> Command<'static> {
    clap::Command::new("multisingle2single")
        .about("generate a sample compare yaml file for scenario multisingle2single")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

fn compare_sample_cluster2cluster_cmd() -> Command<'static> {
    clap::Command::new("cluster2cluster")
        .about("generate a sample compare yaml file for scenario cluster2cluster")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}

fn compare_sample_multisingle2cluster_cmd() -> Command<'static> {
    clap::Command::new("multisingle2cluster")
        .about("generate a sample compare yaml file for scenario multisingle2cluster")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}
