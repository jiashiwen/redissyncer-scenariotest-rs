use crate::cmd::requestsample::new_requestsample_cmd;
use crate::cmd::{new_compare_cmd, new_config_cmd};
use crate::commons::CommandCompleter;
use crate::commons::SubCmd;
use crate::compare::RedisCompare;
use crate::configure::{self, get_config, get_config_file_path, Config};
use crate::configure::{generate_default_config, set_config_file_path};
use crate::request::{req, Request};
use crate::util::from_yaml_file_to_struct;
use crate::{configure::set_config_from_file, interact};
use clap::{Arg, ArgMatches, Command as clap_Command};
use lazy_static::lazy_static;
use serde_yaml::from_str;
use std::borrow::Borrow;

use crate::cmd::cmdgendata::new_gendata_cmd;
use crate::redisdatagen::{GenerateBigKey, GeneratorByDuration};
use std::fs;
use sysinfo::{PidExt, System, SystemExt};

lazy_static! {
    static ref CLIAPP: clap::Command<'static> = clap::Command::new("rediscompare-rs")
        .version("0.1.0")
        .author("Shiwen Jia. <jiashiwen@gmail.com>")
        .about("For compare different redis instance data")
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true)
        )
        .arg(
            Arg::new("interact")
                .short('i')
                .long("interact")
                .help("run as interact mod")
        )
        .arg(
            Arg::new("v")
                .short('v')
                .multiple_occurrences(true)
                .takes_value(true)
                .help("Sets the level of verbosity")
        )
        .subcommand(new_config_cmd())
        .subcommand(new_compare_cmd())
        .subcommand(new_gendata_cmd())
        .subcommand(new_requestsample_cmd());
    static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    let matches = CLIAPP.clone().get_matches();
    if let Some(c) = matches.value_of("config") {
        println!("config path is:{}", c);
        set_config_file_path(c.to_string());
    }
    // set_config(&get_config_file_path());
    cmd_match(&matches);
}

pub fn run_from(args: Vec<String>) {
    match clap_Command::try_get_matches_from(CLIAPP.to_owned(), args.clone()) {
        Ok(matches) => {
            cmd_match(&matches);
        }
        Err(err) => {
            err.print().expect("Error writing Error");
        }
    };
}

// 获取全部子命令，用于构建commandcompleter
pub fn all_subcommand(app: &clap_Command, beginlevel: usize, input: &mut Vec<SubCmd>) {
    let nextlevel = beginlevel + 1;
    let mut subcmds = vec![];
    for iterm in app.get_subcommands() {
        subcmds.push(iterm.get_name().to_string());
        if iterm.has_subcommands() {
            all_subcommand(iterm, nextlevel, input);
        } else {
            if beginlevel == 0 {
                all_subcommand(iterm, nextlevel, input);
            }
        }
    }
    let subcommand = SubCmd {
        level: beginlevel,
        command_name: app.get_name().to_string(),
        subcommands: subcmds,
    };
    input.push(subcommand);
}

pub fn get_command_completer() -> CommandCompleter {
    CommandCompleter::new(SUBCMDS.to_vec())
}

fn subcommands() -> Vec<SubCmd> {
    let mut subcmds = vec![];
    all_subcommand(CLIAPP.clone().borrow(), 0, &mut subcmds);
    subcmds
}

pub fn process_exists(pid: &u32) -> bool {
    let mut sys = System::new_all();
    sys.refresh_all();
    for (syspid, _) in sys.processes() {
        if syspid.as_u32().eq(pid) {
            return true;
        }
    }
    return false;
}

fn cmd_match(matches: &ArgMatches) {
    if let Some(c) = matches.value_of("config") {
        set_config_file_path(c.to_string());
        set_config_from_file(&get_config_file_path());
    } else {
        set_config_from_file("");
    }

    let config = get_config().unwrap();
    let server = config.server;
    let req = Request::new(server.clone()).unwrap();

    if matches.is_present("interact") {
        interact::run();
        return;
    }

    if let Some(ref matches) = matches.subcommand_matches("requestsample") {
        if let Some(_) = matches.subcommand_matches("baidu") {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let async_req = async {
                let result = req::get_baidu().await;
                println!("{:?}", result);
            };
            rt.block_on(async_req);
        };
    }

    if let Some(ref compare) = matches.subcommand_matches("compare") {
        if let Some(execute) = compare.subcommand_matches("exec") {
            let file = execute.value_of("file");
            if let Some(path) = file {
                println!("{:?}", path);
                // let contents = fs::read_to_string(path).expect("Read execution description file error!");
                let contents = fs::read_to_string(path);
                match contents {
                    Ok(c) => {
                        let compare =
                            from_str::<RedisCompare>(c.as_str()).expect("Parse config.yml error!");
                        println!("{:?}", compare);
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
        }
    }

    if let Some(config) = matches.subcommand_matches("config") {
        if let Some(show) = config.subcommand_matches("show") {
            match show.subcommand_name() {
                Some("current") => {
                    let current = configure::get_config().expect("get current configure error!");
                    let yml =
                        serde_yaml::to_string(&current).expect("pars configure to yaml error!");
                    println!("{}", yml);
                }
                Some("default") => {
                    let config = Config::default();
                    let yml = serde_yaml::to_string(&config);
                    match yml {
                        Ok(y) => {
                            println!("{}", y);
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(gen_config) = config.subcommand_matches("gendefault") {
            let mut file = String::from("");
            if let Some(path) = gen_config.value_of("filepath") {
                file.push_str(path);
            } else {
                file.push_str("config_default.yml")
            }
            if let Err(e) = generate_default_config(file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }

    if let Some(gendata) = matches.subcommand_matches("gendata") {
        if let Some(bigkey) = gendata.subcommand_matches("bigkey") {
            if let Some(template) = bigkey.subcommand_matches("template") {
                let mut file = String::from("bigkey_template.yml");
                if let Some(path) = template.value_of("filepath") {
                    file = path.to_string();
                }

                let template = GenerateBigKey::default();
                let yml = serde_yaml::to_string(&template);
                match yml {
                    Ok(y) => {
                        let r = fs::write(file.clone(), y);
                        if let Err(e) = r {
                            println!("{}", e);
                            return;
                        }
                        println!("gen big key template,file is {}", file);
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            if let Some(from) = bigkey.subcommand_matches("from") {
                let file = from.value_of("filepath").expect("flag filepath error");

                let r = from_yaml_file_to_struct::<GenerateBigKey>(file);
                match r {
                    Ok(gbk) => {
                        let r = gbk.exec();
                        if let Err(e) = r {
                            eprintln!("{}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
        }
        if let Some(continuous) = gendata.subcommand_matches("continuous") {
            if let Some(template) = continuous.subcommand_matches("template") {
                let mut file = String::from("continuous_gen_data_template.yml");
                if let Some(path) = template.value_of("filepath") {
                    file = path.to_string();
                }

                let template = GeneratorByDuration::default();
                let yml = serde_yaml::to_string(&template);
                match yml {
                    Ok(y) => {
                        let r = fs::write(file.clone(), y);
                        if let Err(e) = r {
                            println!("{}", e);
                            return;
                        }
                        println!("gen key continuous template,file is {}", file);
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            if let Some(from) = continuous.subcommand_matches("from") {
                println!("gen  key continuous from a file");
            }
        }
    }
}
