#[macro_use]
extern crate serde;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate thiserror;
#[macro_use]
pub extern crate nonblock_logger;

use nonblock_logger::{
    chrono::Local,
    current_thread_name,
    log::{LevelFilter, Record},
    BaseFilter, BaseFormater, FixedLevel, NonblockLogger,
};

use nakamoto_client::Network;
use nakamoto_node::{logger as nakamoto_logger, Domain};
use std::{net, thread};

pub fn format(base: &BaseFormater, record: &Record) -> String {
    let level = FixedLevel::with_color(record.level(), base.color_get()).length(base.level_get()).into_colored().into_coloredfg();

    current_thread_name(|ctn| {
        format!(
            "[{} {}#{}:{} {}] {}\n",
            Local::now().format("%Y-%m-%d %H:%M:%S.%3f"),
            level,
            record.module_path().unwrap_or("*"),
            // record.file().unwrap_or("*"),
            record.line().unwrap_or(0),
            ctn,
            record.args()
        )
    })
}

fn main() {
    use clap::Parser;

    let cli = Cli::parse();
    match cli.command {
        Command::Miner(config) => run_miner(config.fix_workers()),
        Command::Nakamoto(config) => run_nakamoto(config),
        Command::Ui(config) => run_ui(config),
    }
}

pub mod config;
pub mod miner;
pub mod reqs;
pub mod state;
pub mod util;

pub mod btc;
pub mod ckb;
pub mod eth;
pub mod nakamoto;
pub mod kas;
pub mod tui;

use crate::config::{Cli, Command, Config, Currency::*, NakamotoConfig};
use crate::{btc::BtcJob, ckb::CkbJob, eth::EthJob, kas::KasJob};

fn run_miner(config: Config) {
    let pkg = env!("CARGO_PKG_NAME");
    let log = config.log();
    println!("{}: {:?}, {:?}", pkg, log, config);

    let formater = BaseFormater::new().local(true).color(true).level(4).formater(format);
    let filter = BaseFilter::new()
        .max_level(log)
        .starts_with(true)
        .notfound(false)
        .chain(pkg, log)
        .chain("tokio", LevelFilter::Info)
        .chain("mio", LevelFilter::Info);

    let _handle = NonblockLogger::new()
        .formater(formater)
        .quiet()
        .filter(filter)
        .expect("add filiter failed")
        .log_to_stdout()
        .map_err(|e| eprintln!("failed to init nonblock_logger: {:?}", e))
        .unwrap();

    util::catch_ctrlc();

    match config.currency {
        Btc => miner::fun::<BtcJob>(config),
        Ckb => miner::fun::<CkbJob>(config),
        Eth => miner::fun::<EthJob>(config),
        Kas => miner::fun::<KasJob>(config),
    }
}

fn run_nakamoto(config: NakamotoConfig) {
    nakamoto_logger::init(config.log).expect("initializing logger for the first time");
    info!("starting in-process nakamoto miner: {:?}", config);

    let network = if config.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    let domains = if config.ipv4 && config.ipv6 {
        vec![Domain::IPV4, Domain::IPV6]
    } else if config.ipv4 {
        vec![Domain::IPV4]
    } else if config.ipv6 {
        vec![Domain::IPV6]
    } else {
        vec![Domain::IPV4, Domain::IPV6]
    };

    type Reactor = nakamoto_net_poll::Reactor<net::TcpStream>;

    let mut node_config = nakamoto_node::Config::new(network);
    node_config.connect = config.connect;
    node_config.listen = if config.listen.is_empty() {
       vec![([0, 0, 0, 0], 0).into()]
    } else {
       config.listen
    };
    node_config.domains = domains;
    node_config.root = config.root.unwrap_or(node_config.root);

    let client = nakamoto_node::Client::<Reactor>::new().expect("create nakamoto client");
    let handle = client.handle();
    let _node_thread = thread::spawn(move || {
       if let Err(e) = client.run(node_config) {
           eprintln!("node: Exiting: {}", e);
       }
    });

    let block = crate::nakamoto::build_candidate_block(&handle).expect("build candidate block");
    info!("candidate block txs: {}", block.txdata.len());

    info!("starting block solve with {} workers", num_cpus::get());
    let found = btc::pow::mine_block(block, num_cpus::get());
    if let Some(solved) = found {
       info!(
           "mined block {} with nonce {}",
           solved.block_hash(),
           solved.header.nonce
       );
    }

}

fn run_ui(config: NakamotoConfig) {
    tui::run(config).expect("run tui");
}
