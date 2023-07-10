use crate::cmd::requestsample::new_requestsample_cmd;
use crate::cmd::{new_compare_cmd, new_config_cmd};
use crate::commons::CommandCompleter;
use crate::commons::SubCmd;
use crate::compare::{Compare, InstanceType, RedisInstance, ScenarioType, SourceInstance};
use crate::configure::{self, get_config_file_path, Config};
use crate::configure::{generate_default_config, set_config_file_path};
use crate::util::{flash_struct_to_yaml_file, from_yaml_file_to_struct};
use crate::{configure::set_config_from_file, interact};
use clap::{Arg, ArgAction, ArgMatches, Command as clap_Command};
use lazy_static::lazy_static;
use std::borrow::Borrow;
use std::collections::HashMap;

use crate::cmd::cmdgendata::new_gendata_cmd;
use crate::redisdatagen::{GenerateBigKey, GeneratorByDuration};
use std::fs;
use sysinfo::{PidExt, System, SystemExt};

lazy_static! {
    static ref CLIAPP: clap::Command = clap::Command::new("rediscompare-rs")
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
        )
        .arg(
            Arg::new("interact")
                .short('i')
                .long("interact")
                .action(ArgAction::SetTrue)
                .help("run as interact mod")
        )
        .arg(Arg::new("v").short('v').help("Sets the level of verbosity"))
        .subcommand(new_config_cmd())
        .subcommand(new_compare_cmd())
        .subcommand(new_gendata_cmd())
        .subcommand(new_requestsample_cmd());
    static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    let matches = CLIAPP.clone().get_matches();
    if let Some(c) = matches.get_one::<String>("config") {
        println!("config path is:{}", c);
        set_config_file_path(c.to_string());
    }
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
    if let Some(c) = matches.get_one::<String>("config") {
        set_config_file_path(c.to_string());
        set_config_from_file(&get_config_file_path());
    } else {
        set_config_from_file("");
    }

    if matches.get_flag("interact") {
        interact::run();
        return;
    }

    if let Some(ref compare) = matches.subcommand_matches("compare") {
        if let Some(sample) = compare.subcommand_matches("sample") {
            if let Some(single2single) = sample.subcommand_matches("single2single") {
                let mut file = "./compare_sample_single2single.yml".to_string();
                let file_arg = single2single.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut compare = Compare::default();
                compare.source[0].instance.urls[0] =
                    "redis://:password@127.0.0.1:6379/?timeout=1s".to_string();
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(single2cluster) = sample.subcommand_matches("single2cluster") {
                let mut file = "./compare_sample_single2cluster.yml".to_string();
                let file_arg = single2cluster.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut compare = Compare::default();
                let target_instance = RedisInstance {
                    urls: vec![
                        "redis://127.0.0.1:6379".to_string(),
                        "redis://127.0.0.1:6380".to_string(),
                        "redis://127.0.0.1:6381".to_string(),
                    ],
                    password: "".to_string(),
                    instance_type: InstanceType::Cluster,
                };
                compare.target = target_instance;
                compare.scenario = ScenarioType::Single2cluster;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(cluster2cluster) = sample.subcommand_matches("cluster2cluster") {
                let mut file = "./compare_sample_cluster2cluster.yml".to_string();
                let file_arg = cluster2cluster.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut compare = Compare::default();
                let mut dbmapper: HashMap<usize, usize> = HashMap::new();
                dbmapper.insert(0, 0);

                let source_instance = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://127.0.0.1:6379".to_string(),
                            "redis://127.0.0.1:6380".to_string(),
                            "redis://127.0.0.1:6381".to_string(),
                        ],
                        password: "xxx".to_string(),
                        instance_type: InstanceType::Cluster,
                    },
                    dbmapper,
                };
                let target_instance = RedisInstance {
                    urls: vec![
                        "redis://127.0.0.1:16379".to_string(),
                        "redis://127.0.0.1:16380".to_string(),
                        "redis://127.0.0.1:16381".to_string(),
                    ],
                    password: "xxx".to_string(),
                    instance_type: InstanceType::Cluster,
                };
                compare.source[0] = source_instance;
                compare.target = target_instance;
                compare.scenario = ScenarioType::Cluster2cluster;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(multisingle2single) = sample.subcommand_matches("multisingle2single") {
                let mut file = "./compare_sample_multisingle2single.yml".to_string();
                let file_arg = multisingle2single.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut dbmapper: HashMap<usize, usize> = HashMap::new();

                dbmapper.insert(0, 1);
                dbmapper.insert(3, 5);
                dbmapper.insert(4, 4);

                let source_instance1 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_1@127.0.0.1:6379/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 1);
                dbmapper.insert(2, 5);
                dbmapper.insert(4, 3);
                let source_instance2 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_2@127.0.0.1:6380/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(2, 1);
                dbmapper.insert(1, 5);
                dbmapper.insert(4, 7);
                let source_instance3 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_3@127.0.0.1:6381/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                let target_instance = RedisInstance {
                    urls: vec!["redis://:password_target@127.0.0.1:6382/?timeout=1s".to_string()],
                    password: "".to_string(),
                    instance_type: InstanceType::Single,
                };

                let mut compare = Compare::default();
                compare.source.clear();
                compare.source.push(source_instance1);
                compare.source.push(source_instance2);
                compare.source.push(source_instance3);
                compare.target = target_instance;
                compare.scenario = ScenarioType::Multisingle2single;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(multisingle2cluster) = sample.subcommand_matches("multisingle2cluster") {
                let mut file = "./compare_sample_multisingle2cluster.yml".to_string();
                let file_arg = multisingle2cluster.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut dbmapper: HashMap<usize, usize> = HashMap::new();

                dbmapper.insert(0, 1);
                dbmapper.insert(2, 3);
                dbmapper.insert(6, 6);

                let source_instance1 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_1@127.0.0.1:6379/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 0);
                dbmapper.insert(2, 5);
                dbmapper.insert(4, 3);
                let source_instance2 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_2@127.0.0.1:6380/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(2, 1);
                dbmapper.insert(1, 5);
                dbmapper.insert(4, 7);
                let source_instance3 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_3@127.0.0.1:6381/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                let target_instance = RedisInstance {
                    urls: vec![
                        "redis://127.0.0.1:16379".to_string(),
                        "redis://127.0.0.1:16380".to_string(),
                        "redis://127.0.0.1:16381".to_string(),
                    ],
                    password: "xxx".to_string(),
                    instance_type: InstanceType::Cluster,
                };

                let mut compare = Compare::default();

                compare.source.clear();
                compare.source.push(source_instance1);
                compare.source.push(source_instance2);
                compare.source.push(source_instance3);
                compare.target = target_instance;
                compare.scenario = ScenarioType::Multisingle2cluster;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(multisingle2single) = sample.subcommand_matches("multi2single") {
                let mut file = "./compare_sample_multi2single.yml".to_string();
                let file_arg = multisingle2single.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut dbmapper: HashMap<usize, usize> = HashMap::new();

                dbmapper.insert(0, 1);
                dbmapper.insert(3, 5);
                dbmapper.insert(4, 4);

                let source_instance1 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_1@127.0.0.1:6379/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 1);
                dbmapper.insert(2, 5);
                dbmapper.insert(4, 3);
                let source_instance2 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_2@127.0.0.1:6380/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 1);
                let source_instance3 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://127.0.0.1:26379".to_string(),
                            "redis://127.0.0.1:26380".to_string(),
                            "redis://127.0.0.1:26381".to_string(),
                        ],
                        password: "xxxx".to_string(),
                        instance_type: InstanceType::Cluster,
                    },
                    dbmapper: dbmapper.clone(),
                };

                let target_instance = RedisInstance {
                    urls: vec!["redis://:password_target@127.0.0.1:6382/?timeout=1s".to_string()],
                    password: "".to_string(),
                    instance_type: InstanceType::Single,
                };

                let mut compare = Compare::default();
                compare.source.clear();
                compare.source.push(source_instance1);
                compare.source.push(source_instance2);
                compare.source.push(source_instance3);
                compare.target = target_instance;
                compare.scenario = ScenarioType::Multisingle2single;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }

            if let Some(multisingle2cluster) = sample.subcommand_matches("multi2cluster") {
                let mut file = "./compare_sample_multi2cluster.yml".to_string();
                let file_arg = multisingle2cluster.get_one::<String>("filepath");
                if let Some(arg) = file_arg {
                    file = arg.to_string();
                }

                let mut dbmapper: HashMap<usize, usize> = HashMap::new();

                dbmapper.insert(1, 0);
                dbmapper.insert(2, 0);
                dbmapper.insert(6, 0);

                let source_instance1 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_1@127.0.0.1:6379/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 0);
                dbmapper.insert(5, 0);
                let source_instance2 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://:password_source_2@127.0.0.1:6380/?timeout=1s".to_string()
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Single,
                    },
                    dbmapper: dbmapper.clone(),
                };

                dbmapper.clear();
                dbmapper.insert(0, 0);

                let source_instance3 = SourceInstance {
                    instance: RedisInstance {
                        urls: vec![
                            "redis://127.0.0.1:26379".to_string(),
                            "redis://127.0.0.1:26380".to_string(),
                            "redis://127.0.0.1:26381".to_string(),
                        ],
                        password: "".to_string(),
                        instance_type: InstanceType::Cluster,
                    },
                    dbmapper: dbmapper.clone(),
                };

                let target_instance = RedisInstance {
                    urls: vec![
                        "redis://127.0.0.1:16379".to_string(),
                        "redis://127.0.0.1:16380".to_string(),
                        "redis://127.0.0.1:16381".to_string(),
                    ],
                    password: "xxx".to_string(),
                    instance_type: InstanceType::Cluster,
                };

                let mut compare = Compare::default();

                compare.source.clear();
                compare.source.push(source_instance1);
                compare.source.push(source_instance2);
                compare.source.push(source_instance3);
                compare.target = target_instance;
                compare.scenario = ScenarioType::Multisingle2cluster;
                match flash_struct_to_yaml_file(&compare, &file) {
                    Ok(_) => println!("Create file {} Ok", file),
                    Err(e) => eprintln!("{}", e),
                };
            }
        }

        if let Some(execute) = compare.subcommand_matches("exec") {
            let file = execute.get_one::<String>("file");
            if let Some(path) = file {
                let r = from_yaml_file_to_struct::<Compare>(path);
                match r {
                    Ok(compare) => {
                        compare.exec();
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
            if let Some(path) = gen_config.get_one::<String>("filepath") {
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
                if let Some(path) = template.get_one::<String>("filepath") {
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
                let file = from
                    .get_one::<String>("filepath")
                    .expect("flag filepath error");

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
                if let Some(path) = template.get_one::<String>("filepath") {
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
                if let Some(path) = from.get_one::<String>("filepath") {
                    println!("path is {}", path);
                    if let Ok(gbd) = from_yaml_file_to_struct::<GeneratorByDuration>(path) {
                        let r_exec = gbd.exec();
                        if let Err(e) = r_exec {
                            eprintln!("{}", e);
                        }
                    }
                }
            }
        }
    }
}
