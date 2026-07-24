use std::{
    collections::VecDeque,
    io,
    net,
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use bitcoin::Block;
use crossterm::{
    event::{self, Event as CEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use parking_lot::Mutex;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    prelude::{Frame, Line, Style},
    widgets::{Block as TBlock, Borders, Clear, List, ListItem, Paragraph, Tabs},
    Terminal,
};

use nonblock_logger::log::{self, Level, LevelFilter, Log, Metadata, Record};
use nakamoto_client::{handle::Handle as _, Domain, Network, Peer};
use nakamoto_common::bitcoin::network::constants::ServiceFlags;

use crate::{btc, config::NakamotoConfig, nakamoto::build_candidate_block, util::Result};

#[derive(Clone, Copy, PartialEq, Eq)]
enum MainTab {
    Node,
    Miner,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeTab {
    Peers,
    Logs,
}

#[derive(Default)]
struct App {
    main_tab: MainTab,
    node_tab: NodeTab,
    selected_peer: usize,
    log_scroll: u16,
    node: NodeState,
    miner: MinerState,
}

impl Default for MainTab {
    fn default() -> Self {
        Self::Node
    }
}

impl Default for NodeTab {
    fn default() -> Self {
        Self::Peers
    }
}

#[derive(Clone, Default)]
struct NodeState {
    tip: Option<String>,
    peers: Vec<Peer>,
}

#[derive(Clone, Default)]
struct MinerState {
    block: Option<Block>,
    tx_count: usize,
    workers: usize,
    current_nonce: u32,
    solved_nonce: Option<u32>,
    solved_hash: Option<String>,
    status: String,
}

#[derive(Clone)]
enum Update {
    Node(NodeState),
    Peers(Vec<Peer>),
    Miner(MinerState),
    Mine(btc::pow::MineUpdate),
}

struct BufferLogger {
    level: LevelFilter,
    logs: Arc<Mutex<VecDeque<String>>>,
}

impl BufferLogger {
    fn init(level: Level, logs: Arc<Mutex<VecDeque<String>>>) -> Result<()> {
        log::set_boxed_logger(Box::new(Self {
            level: level.to_level_filter(),
            logs,
        }))?;
        log::set_max_level(level.to_level_filter());
        Ok(())
    }
}

impl Log for BufferLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let mut logs = self.logs.lock();
        logs.push_back(sanitize_line(format!("{} {} {}", record.level(), record.target(), record.args())));
        while logs.len() > 400 {
            logs.pop_front();
        }
    }

    fn flush(&self) {}
}

struct UiGuard;

impl UiGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for UiGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

pub fn run(config: NakamotoConfig) -> Result<()> {
    let logs = Arc::new(Mutex::new(VecDeque::new()));
    BufferLogger::init(config.log, Arc::clone(&logs))?;

    let (update_tx, update_rx) = mpsc::channel::<Update>();
    let (mine_tx, mine_rx) = mpsc::channel::<btc::pow::MineUpdate>();

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

    let client = nakamoto_node::Client::<Reactor>::new()?;
    let handle = client.handle();
    let events = handle.events();

    thread::spawn(move || {
        if let Err(e) = client.run(node_config) {
            error!("nakamoto client stopped: {}", e);
        }
    });

    {
        let handle = handle.clone();
        let update_tx = update_tx.clone();
        thread::spawn(move || loop {
            let mut state = NodeState::default();
            if let Ok((height, header)) = handle.get_tip() {
                state.tip = Some(format!("{} @ {}", height, header.block_hash()));
            }
            if let Ok(peers) = handle.get_peers(ServiceFlags::NONE) {
                state.peers = peers;
            }
            let _ = update_tx.send(Update::Node(state));
            thread::sleep(Duration::from_secs(2));
        });
    }

    {
        let handle = handle.clone();
        let update_tx = update_tx.clone();
        let logs = Arc::clone(&logs);
        thread::spawn(move || {
            while let Ok(event) = events.recv() {
                let event_text = event.to_string();
                push_log(&logs, event_text.clone());

                match &event {
                    nakamoto_client::Event::PeerConnected { .. }
                    | nakamoto_client::Event::PeerDisconnected { .. }
                    | nakamoto_client::Event::PeerConnectionFailed { .. }
                    | nakamoto_client::Event::PeerNegotiated { .. } => {
                        push_log(&logs, format!("peer event: {}", event_text));
                        if let Ok(peers) = handle.get_peers(ServiceFlags::NONE) {
                            let _ = update_tx.send(Update::Peers(peers));
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    {
        let handle = handle.clone();
        let update_tx = update_tx.clone();
        thread::spawn(move || loop {
            match build_candidate_block(&handle) {
                Ok(block) => {
                    let state = MinerState {
                        tx_count: block.txdata.len(),
                        workers: num_cpus::get(),
                        status: "mining".to_owned(),
                        block: Some(block.clone()),
                        ..MinerState::default()
                    };
                    let _ = update_tx.send(Update::Miner(state));
                    let _ = btc::pow::mine_block_with_updates(block, num_cpus::get(), Some(mine_tx.clone()));
                    break;
                }
                Err(e) => {
                    warn!("candidate block not ready: {}", e);
                    thread::sleep(Duration::from_secs(2));
                }
            }
        });
    }

    {
        let update_tx = update_tx.clone();
        thread::spawn(move || {
            while let Ok(update) = mine_rx.recv() {
                let _ = update_tx.send(Update::Mine(update));
            }
        });
    }

    let _guard = UiGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::default();
    loop {
        while let Ok(update) = update_rx.try_recv() {
            match update {
                Update::Node(state) => app.node = state,
                Update::Peers(peers) => {
                    app.node.peers = peers;
                    if app.selected_peer >= app.node.peers.len() {
                        app.selected_peer = app.node.peers.len().saturating_sub(1);
                    }
                }
                Update::Miner(state) => app.miner = state,
                Update::Mine(update) => match update {
                    btc::pow::MineUpdate::Started { workers, .. } => app.miner.workers = workers,
                    btc::pow::MineUpdate::WorkerStarted { worker, stride } => {
                        app.miner.status = format!("worker {worker} stride {stride}");
                    }
                    btc::pow::MineUpdate::Progress { nonce, .. } => app.miner.current_nonce = nonce,
                    btc::pow::MineUpdate::Found { nonce, hash, .. } => {
                        app.miner.current_nonce = nonce;
                        app.miner.solved_nonce = Some(nonce);
                        app.miner.solved_hash = Some(hash);
                    }
                    btc::pow::MineUpdate::Finished { nonce, hash } => {
                        app.miner.current_nonce = nonce;
                        app.miner.solved_nonce = Some(nonce);
                        app.miner.solved_hash = Some(hash);
                        app.miner.status = "finished".to_owned();
                    }
                },
            }
        }

        if event::poll(Duration::from_millis(100))? {
            if let CEvent::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Tab => {
                        app.main_tab = match app.main_tab {
                            MainTab::Node => MainTab::Miner,
                            MainTab::Miner => MainTab::Node,
                        }
                    }
                    KeyCode::Left | KeyCode::Right if matches!(app.main_tab, MainTab::Node) => {
                        app.node_tab = match app.node_tab {
                            NodeTab::Peers => NodeTab::Logs,
                            NodeTab::Logs => NodeTab::Peers,
                        }
                    }
                    KeyCode::Up if matches!(app.main_tab, MainTab::Node) && matches!(app.node_tab, NodeTab::Peers) => {
                        app.selected_peer = app.selected_peer.saturating_sub(1);
                    }
                    KeyCode::Down if matches!(app.main_tab, MainTab::Node) && matches!(app.node_tab, NodeTab::Peers) => {
                        app.selected_peer = app.selected_peer.saturating_add(1).min(app.node.peers.len().saturating_sub(1));
                    }
                    KeyCode::Up if matches!(app.main_tab, MainTab::Node) && matches!(app.node_tab, NodeTab::Logs) => {
                        app.log_scroll = app.log_scroll.saturating_sub(1);
                    }
                    KeyCode::Down if matches!(app.main_tab, MainTab::Node) && matches!(app.node_tab, NodeTab::Logs) => {
                        app.log_scroll = app.log_scroll.saturating_add(1);
                    }
                    _ => {}
                }
            }
        }

        terminal.draw(|frame| draw(frame, &app, &logs))?;
    }

    let _ = handle.shutdown();
    Ok(())
}

fn draw(frame: &mut Frame<'_>, app: &App, logs: &Arc<Mutex<VecDeque<String>>>) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(frame.size());

    let tabs = Tabs::new(vec![Line::from("node"), Line::from("miner")])
        .block(TBlock::default().borders(Borders::ALL))
        .select(match app.main_tab {
            MainTab::Node => 0,
            MainTab::Miner => 1,
        })
        .highlight_style(Style::default().fg(ratatui::style::Color::Cyan));
    frame.render_widget(tabs, layout[0]);

    match app.main_tab {
        MainTab::Node => draw_node(frame, layout[1], app, logs),
        MainTab::Miner => draw_miner(frame, layout[1], app),
    }

    let footer = Paragraph::new(match app.main_tab {
        MainTab::Node => "tab switch | left/right peers/log | up/down select | q quit",
        MainTab::Miner => "tab switch | q quit",
    })
    .style(Style::default().fg(ratatui::style::Color::DarkGray));
    frame.render_widget(footer, layout[2]);
}

fn draw_node(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    logs: &Arc<Mutex<VecDeque<String>>>,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let tabs = Tabs::new(vec![Line::from("peers"), Line::from("log")])
        .block(TBlock::default().borders(Borders::ALL).title("node"))
        .select(match app.node_tab {
            NodeTab::Peers => 0,
            NodeTab::Logs => 1,
        });
    frame.render_widget(tabs, layout[0]);

    match app.node_tab {
        NodeTab::Peers => draw_peers(frame, layout[1], app),
        NodeTab::Logs => draw_logs(frame, layout[1], app, logs),
    }
}

fn draw_peers(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let items = if app.node.peers.is_empty() {
        vec![ListItem::new("no peers")]
    } else {
        app.node
            .peers
            .iter()
            .enumerate()
            .map(|(idx, peer)| {
                ListItem::new(format!(
                    "{} {}",
                    if peer.is_outbound() { "out" } else { "in " },
                    peer.addr
                ))
                .style(if idx == app.selected_peer {
                    Style::default().fg(ratatui::style::Color::Black).bg(ratatui::style::Color::White)
                } else {
                    Style::default()
                })
            })
            .collect::<Vec<_>>()
    };

    frame.render_widget(
        List::new(items).block(TBlock::default().borders(Borders::ALL).title("peers")),
        cols[0],
    );

    let text = if let Some(peer) = app.node.peers.get(app.selected_peer) {
        vec![
            format!("addr: {}", peer.addr),
            format!("local: {}", peer.local_addr),
            format!("link: {:?}", peer.link),
            format!("height: {}", peer.height),
            format!("services: {}", peer.services),
            format!("relay: {}", peer.relay),
            format!("agent: {}", peer.user_agent),
        ]
        .join("\n")
    } else {
        "select a peer".to_owned()
    };

    frame.render_widget(
        Paragraph::new(text).block(TBlock::default().borders(Borders::ALL).title("peer details")),
        cols[1],
    );
}

fn draw_logs(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    logs: &Arc<Mutex<VecDeque<String>>>,
) {
    frame.render_widget(Clear, area);
    let visible = logs
        .lock()
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .cloned()
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(visible.into_iter().rev().map(ListItem::new).collect::<Vec<_>>())
            .block(TBlock::default().borders(Borders::ALL).title("logs")),
        area,
    );
}

fn push_log(logs: &Arc<Mutex<VecDeque<String>>>, line: impl Into<String>) {
    let mut logs = logs.lock();
    logs.push_back(sanitize_line(line.into()));
    while logs.len() > 400 {
        logs.pop_front();
    }
}

fn sanitize_line(line: String) -> String {
    line.chars()
        .map(|c| {
            if c.is_ascii_graphic()
                || c == ' '
                || c == '\t'
                || c == ':'
                || c == '.'
                || c == ','
                || c == '-'
                || c == '_'
                || c == '/'
                || c == '('
                || c == ')'
                || c == '@'
                || c == '#'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '='
                || c == '<'
                || c == '>'
            {
                c
            } else {
                ' '
            }
        })
        .collect()
}

fn draw_miner(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(0)])
        .split(area);

    let mut lines = vec![
        format!("status: {}", app.miner.status),
        format!("workers: {}", app.miner.workers),
        format!("txs: {}", app.miner.tx_count),
        format!("nonce: {}", app.miner.current_nonce),
    ];
    if let Some(nonce) = app.miner.solved_nonce {
        lines.push(format!("solved nonce: {}", nonce));
    }
    if let Some(hash) = &app.miner.solved_hash {
        lines.push(format!("solved hash: {}", hash));
    }

    if let Some(block) = &app.miner.block {
        lines.push(format!("version: {}", block.header.version));
        lines.push(format!("prev: {}", block.header.prev_blockhash));
        lines.push(format!("merkle: {}", block.header.merkle_root));
        lines.push(format!("time: {}", block.header.time));
        lines.push(format!("bits: {}", block.header.bits));
    }

    frame.render_widget(
        Paragraph::new(lines.join("\n"))
            .block(TBlock::default().borders(Borders::ALL).title("block template")),
        layout[0],
    );

    let tx_items = app
        .miner
        .block
        .as_ref()
        .map(|block| {
            block
                .txdata
                .iter()
                .enumerate()
                .map(|(idx, tx)| ListItem::new(format!("{} {}", idx, tx.txid())))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![ListItem::new("waiting for template")]);

    frame.render_widget(
        List::new(tx_items).block(TBlock::default().borders(Borders::ALL).title("transactions")),
        layout[1],
    );
}
