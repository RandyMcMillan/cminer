use nonblock_logger::log::LevelFilter::{self, *};
use nonblock_logger::log::Level;

#[derive(Clone, Copy, Debug)]
pub enum Currency {
    Btc,
    Ckb,
    Eth,
    Kas,
}

impl clap::ArgEnum for Currency {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Eth, Self::Ckb, Self::Btc, Self::Kas]
    }
    fn to_possible_value<'a>(&self) -> Option<clap::PossibleValue<'a>> {
        Some(
            match self {
                Self::Eth => "eth",
                Self::Ckb => "ckb",
                Self::Btc => "btc",
                Self::Kas => "kas",
            }
            .into(),
        )
    }
}

impl Default for Currency {
    fn default() -> Self {
        Self::Ckb
    }
}

#[derive(clap::Parser, Debug, Clone)]
pub struct NakamotoNodeArgs {
    #[clap(long = "nakamoto-connect")]
    pub connect: Vec<SocketAddr>,
    #[clap(long = "nakamoto-listen")]
    pub listen: Vec<SocketAddr>,
    #[clap(long = "nakamoto-testnet")]
    pub nakamoto_testnet: bool,
    #[clap(short = '4', long = "nakamoto-ipv4")]
    pub ipv4: bool,
    #[clap(short = '6', long = "nakamoto-ipv6")]
    pub ipv6: bool,
    #[clap(long = "nakamoto-log", default_value = "info")]
    pub log: Level,
    #[clap(long = "nakamoto-root")]
    pub root: Option<PathBuf>,
}

impl Default for NakamotoNodeArgs {
    fn default() -> Self {
        Self {
            connect: Vec::new(),
            listen: Vec::new(),
            nakamoto_testnet: false,
            ipv4: false,
            ipv6: false,
            log: Level::Info,
            root: None,
        }
    }
}

use std::{
    fmt,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct PoolAddr {
    pub str: String,
    pub sa: SocketAddr,
}

impl fmt::Display for PoolAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.str, self.sa)
    }
}

impl std::str::FromStr for PoolAddr {
    type Err = String;
    fn from_str(pool: &str) -> Result<Self, Self::Err> {
        let mut iter = pool.to_socket_addrs().map_err(|e| format!("pool.to_socket_addrs failed: {:?}", e))?;
        iter.next().map(|sa| Self { str: pool.to_owned(), sa }).ok_or_else(|| "pool.to_socket_addrs is empty".into())
    }
}

#[derive(clap::Parser, Debug, Clone)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
pub struct Config {
    #[clap(short, long, help = "The address of pool: Host/IP:port")]
    pub pool: PoolAddr,
    #[clap(short, long, default_value = "128", help = "Default is NumCPUs, if arg bigger than it, will reset as it")]
    pub workers: usize,
    #[clap(arg_enum, default_value_t, ignore_case = true, short, long, default_value = "btc")]
    #[clap(help = "Currency")]
    pub currency: Currency,
    #[clap(short, long, help = "enable testnet(work for ckb testnet and etchash(ecip-1099))")]
    pub testnet: bool,
    #[clap(short, long, default_value = "user", help = "The name of User")]
    pub user: String,
    #[clap(short, long, default_value = "rig", help = "The name of Rig")]
    pub rig: String,
    #[clap(short, long, parse(from_occurrences), help = "Loglevel: -v(Info), -vv(Debug), -vvv+(Trace)")]
    pub verbose: u8,
    #[clap(short, long, default_value = "100", help = "program will reconnect if the job not updated for so many seconds")]
    pub expire: u64,
    #[clap(short, long, default_value = "0", help = "thread will sleep the secs after submit a solution")]
    pub sleep: u64,
    #[clap(short, long, help = "the domain for enable tls [An empty domain name means skipping the verify]")]
    pub domain: Option<String>,
    #[clap(flatten)]
    pub nakamoto: NakamotoNodeArgs,
}

impl Config {
    pub fn log(&self) -> LevelFilter {
        match self.verbose {
            0 => Warn,
            1 => Info,
            2 => Debug,
            _ => Trace,
        }
    }
    pub fn new2<C, P, U, R>(currency: C, testnet: bool, pool: P, workers: usize, user: U, rig: R, verbose: u8) -> Self
    where
        C: AsRef<str>,
        P: AsRef<str>,
        U: Into<String>,
        R: Into<String>,
    {
        use clap::ArgEnum;

        Self {
            testnet,
            workers,
            verbose,
            sleep: 0,
            expire: 100,
            domain: None,
            nakamoto: NakamotoNodeArgs::default(),
            pool: pool.as_ref().parse().expect("resolve name failed"),
            currency: Currency::from_str(currency.as_ref(), true).unwrap_or(Currency::Btc),
            user: user.into(),
            rig: rig.into(),
        }
    }
    pub fn fix_workers(mut self) -> Self {
        let ws = num_cpus::get();
        if self.workers > ws {
            self.workers = ws;
        }
        self
    }
    pub fn tls_config(&self) -> Option<(TlsConnector, String)> {
        Self::tls_config_for_proxy(self.domain.clone())
    }
    pub fn tls_config_for_proxy(domain: Option<String>) -> Option<(TlsConnector, String)> {
        domain.map(|mut d| {
            let mut config = ClientConfig::new();

            if d.is_empty() {
                config.dangerous().set_certificate_verifier(Arc::new(NoCertificateVerification));
                // "" will get InvalidDNSNameError
                d = "localhost".to_owned();
            } else {
                config.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            }
            (TlsConnector::from(Arc::new(config)), d)
        })
    }
}

use tokio_rustls::{rustls, rustls::ClientConfig, webpki, TlsConnector};

pub struct NoCertificateVerification;

impl rustls::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _roots: &rustls::RootCertStore,
        _presented_certs: &[rustls::Certificate],
        _dns_name: webpki::DNSNameRef<'_>,
        _ocsp: &[u8],
    ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
        Ok(rustls::ServerCertVerified::assertion())
    }
}

pub const TIMEOUT_SECS: u64 = 3;

use std::time::Duration;
pub const fn timeout() -> Duration {
    Duration::from_secs(TIMEOUT_SECS)
}
