extern crate core;

use logger::init_log;

mod cmd;
mod commons;
mod compare;
mod configure;
mod interact;
mod logger;
// mod request;
mod redisdatagen;
mod util;

fn main() {
    init_log();
    cmd::run_app();
}
