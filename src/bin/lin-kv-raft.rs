use maelstrom_rust::{ErrorCode, Message, Stub};

use anyhow::Result;
use rand::Rng;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{self, Arc, Mutex},
    thread,
    time::Duration,
};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum MessageBody {
    /// (maelstrom) Initialize the node. Sent by the maelstrom tester.
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    /// (maelstrom) Response to [MessageBody::Init].
    InitOk { msg_id: u64, in_reply_to: u64 },
    /// (maelstrom) Error response to any request.
    Error {
        msg_id: u64,
        in_reply_to: u64,
        code: ErrorCode,
        text: String,
    },
    /// (lin-kv RPC request) Read a value for a given key.
    Read { msg_id: u64, key: u64 },
    /// (lin-kv RPC response) Successful response to [MessageBody::Read].
    ReadOk {
        msg_id: u64,
        in_reply_to: u64,
        value: i64,
    },
    /// (lin-kv RPC request) Write a value for a given key.
    Write { msg_id: u64, key: u64, value: i64 },
    /// (lin-kv RPC response) Successful response to [MessageBody::Write].
    WriteOk { msg_id: u64, in_reply_to: u64 },
    /// (lin-kv RPC request) Compare and swap a value for a given key.
    Cas {
        msg_id: u64,
        key: u64,
        from: i64,
        to: i64,
    },
    /// (lin-kv RPC response) Successful response to [MessageBody::Cas].
    CasOk { msg_id: u64, in_reply_to: u64 },
    RequestVote {
        msg_id: u64,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        msg_id: u64,
        in_reply_to: u64,
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        msg_id: u64,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        msg_id: u64,
        in_reply_to: u64,
        term: u64,
        success: bool,
        match_index: u64,
    },
}

impl MessageBody {
    fn msg_id(&self) -> u64 {
        match self {
            MessageBody::Init { msg_id, .. } => *msg_id,
            MessageBody::InitOk { msg_id, .. } => *msg_id,
            MessageBody::Error { msg_id, .. } => *msg_id,
            MessageBody::Read { msg_id, .. } => *msg_id,
            MessageBody::ReadOk { msg_id, .. } => *msg_id,
            MessageBody::Write { msg_id, .. } => *msg_id,
            MessageBody::WriteOk { msg_id, .. } => *msg_id,
            MessageBody::Cas { msg_id, .. } => *msg_id,
            MessageBody::CasOk { msg_id, .. } => *msg_id,
            MessageBody::RequestVote { msg_id, .. } => *msg_id,
            MessageBody::RequestVoteResponse { msg_id, .. } => *msg_id,
            MessageBody::AppendEntries { msg_id, .. } => *msg_id,
            MessageBody::AppendEntriesResponse { msg_id, .. } => *msg_id,
        }
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct LogEntry {
    term: u64,
    msg: Message<MessageBody>,
}

#[derive(Default)]
struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    fn get_since(&self, start: u64) -> &[LogEntry] {
        let start_idx = if start == 0 { 0 } else { (start - 1) as usize };
        &self.entries[start_idx..]
    }

    fn truncate(&mut self, index: u64) {
        self.entries.truncate((index - 1) as usize);
    }

    fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }
}

#[derive(PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct State {
    next_message_id: u64,
    leader: Option<String>,

    // Persistent state on all servers
    current_term: u64,
    voted_for: Option<String>,
    log: Log,

    // Volatile state on all servers
    role: Role,
    commit_index: u64,
    last_applied: u64,

    // Volatile state on candidates
    votes_granted: Vec<String>,

    // Volatile state on leaders
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    // State machine
    map: HashMap<u64, i64>,
}

impl Default for State {
    fn default() -> Self {
        State {
            next_message_id: 1,
            leader: None,
            current_term: 0,
            voted_for: None,
            log: Log::default(),
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            votes_granted: Vec::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            map: HashMap::new(),
        }
    }
}

impl State {
    fn acquire_message_id(&mut self) -> u64 {
        let msg_id = self.next_message_id;
        self.next_message_id += 1;
        msg_id
    }
}

struct Node {
    stub: Stub,
    id: String,
    member_ids: Vec<String>,
    state: Arc<Mutex<State>>,
    election_timeout_reset: Option<sync::mpsc::Sender<()>>,
    apply_committed_entries_notify: Option<sync::mpsc::Sender<()>>,
}

impl Node {
    fn new() -> Self {
        Node {
            stub: Stub::new(),
            id: String::new(),
            member_ids: Vec::new(),
            state: Arc::default(),
            election_timeout_reset: None,
            apply_committed_entries_notify: None,
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let msg = self.stub.get_message::<MessageBody>()?;
            match msg.body {
                MessageBody::Init {
                    msg_id,
                    node_id,
                    node_ids,
                } => self.handle_init(msg.src, msg.dest, msg_id, node_id, node_ids)?,
                MessageBody::Read { .. } => self.handle_command(msg)?,
                MessageBody::Write { .. } => self.handle_command(msg)?,
                MessageBody::Cas { .. } => self.handle_command(msg)?,
                MessageBody::RequestVote {
                    msg_id,
                    term,
                    last_log_index,
                    last_log_term,
                } => self.handle_request_vote(
                    msg.src,
                    msg.dest,
                    msg_id,
                    term,
                    last_log_index,
                    last_log_term,
                )?,
                MessageBody::RequestVoteResponse {
                    msg_id,
                    in_reply_to: _,
                    term,
                    vote_granted,
                } => self.handle_request_vote_response(
                    msg.src,
                    msg.dest,
                    msg_id,
                    term,
                    vote_granted,
                )?,
                MessageBody::AppendEntries {
                    msg_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => self.handle_append_entries(
                    msg.src,
                    msg.dest,
                    msg_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )?,
                MessageBody::AppendEntriesResponse {
                    msg_id,
                    in_reply_to: _,
                    term,
                    success,
                    match_index,
                } => self.handle_append_entries_response(
                    msg.src,
                    msg.dest,
                    msg_id,
                    term,
                    success,
                    match_index,
                )?,
                _ => unimplemented!(),
            }
        }
    }

    fn handle_init(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    ) -> Result<()> {
        self.id = node_id;
        self.member_ids = node_ids;
        eprintln!("Initialized node #{}", self.id);

        let state = &mut self.state.lock().unwrap();
        let msg_response_id = state.acquire_message_id();
        let msg_response = Message {
            src: dest,
            dest: src,
            body: MessageBody::InitOk {
                msg_id: msg_response_id,
                in_reply_to: msg_id,
            },
        };
        self.stub.send_message(&msg_response)?;

        self.election_timeout_reset = Some(self.run_election_timeout());
        self.apply_committed_entries_notify = Some(self.run_apply_committed_entries());
        Ok(())
    }

    fn handle_command(&mut self, msg: Message<MessageBody>) -> Result<()> {
        let state = &mut self.state.lock().unwrap();
        if state.role != Role::Leader {
            let msg_response = if let Some(leader) = &state.leader {
                Message {
                    src: msg.src,
                    dest: leader.clone(),
                    body: msg.body,
                }
            } else {
                let msg_response_id = state.acquire_message_id();
                let msg_response_body = MessageBody::Error {
                    msg_id: msg_response_id,
                    in_reply_to: msg.body.msg_id(),
                    code: ErrorCode::TemporarilyUnavailable, // TODO
                    text: "not a leader".to_string(),
                };
                Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: msg_response_body,
                }
            };
            return self.stub.send_message(&msg_response);
        }

        let entry = LogEntry {
            term: state.current_term,
            msg,
        };
        state.log.append(entry);

        Ok(())
    }

    fn handle_request_vote(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Result<()> {
        let state = self.state.clone();
        let mut state = state.lock().unwrap();
        self.update_term(&mut state, term);

        let last_log_index_self = state.log.last_index();
        let last_log_term_self = state.log.last_term();
        let log_ok = (last_log_term > last_log_term_self)
            || (last_log_term == last_log_term_self && last_log_index >= last_log_index_self);
        let grant = term == state.current_term
            && (state.voted_for.is_none() || state.voted_for.as_ref() == Some(&src))
            && log_ok;

        if grant {
            state.voted_for = Some(src.clone());
        }

        let msg_response = Message {
            src: dest,
            dest: src,
            body: MessageBody::RequestVoteResponse {
                msg_id: state.acquire_message_id(),
                in_reply_to: msg_id,
                term: state.current_term,
                vote_granted: grant,
            },
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_request_vote_response(
        &mut self,
        src: String,
        _dest: String,
        _msg_id: u64,
        term: u64,
        vote_granted: bool,
    ) -> Result<()> {
        let state = self.state.clone();
        let mut state = state.lock().unwrap();
        self.update_term(&mut state, term);

        if state.role != Role::Candidate || term != state.current_term {
            return Ok(());
        }

        if vote_granted {
            state.votes_granted.push(src);
            let majority = (self.member_ids.len() / 2) + 1;
            if state.votes_granted.len() >= majority {
                state.role = Role::Leader;
                state.leader = Some(self.id.clone());
                state.next_index = self
                    .member_ids
                    .iter()
                    .map(|id| (id.clone(), state.log.last_index() + 1))
                    .collect();
                state.match_index = self.member_ids.iter().map(|id| (id.clone(), 0)).collect();
                self.election_timeout_reset = None;
                self.run_append_entries(state.current_term);

                eprintln!("became leader. term={}", state.current_term);
            }
        }
        Ok(())
    }

    fn handle_append_entries(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<()> {
        let state = self.state.clone();
        let mut state = state.lock().unwrap();
        self.update_term(&mut state, term);

        let mut success = false;
        let mut match_index = 0;

        if term == state.current_term {
            if state.leader == None {
                state.leader = Some(src.clone());
            }
            if state.role == Role::Follower {
                self.election_timeout_reset.as_mut().map(|r| r.send(()));
            } else {
                state.role = Role::Follower;
                self.election_timeout_reset = Some(self.run_election_timeout());
                eprintln!(
                    "became follower, receiving AppendEntries from a leader. term={}, leader={}",
                    state.current_term, src,
                );
            }

            success = if prev_log_index == 0 {
                true
            } else if let Some(entry) = state.log.get(prev_log_index) {
                entry.term == prev_log_term
            } else {
                false
            };

            if success {
                let entries_len = entries.len();
                for (i, entry) in entries.into_iter().enumerate() {
                    let index = prev_log_index + 1 + (i as u64);
                    if let Some(existing_entry) = state.log.get(index) {
                        if existing_entry.term != entry.term {
                            state.log.truncate(index);
                            state.log.append(entry);
                        }
                    } else {
                        state.log.append(entry);
                    }
                }

                if leader_commit > state.commit_index {
                    state.commit_index = std::cmp::min(leader_commit, state.log.last_index());
                    self.apply_committed_entries_notify
                        .as_ref()
                        .map(|n| n.send(()));
                }
                match_index = prev_log_index + (entries_len as u64);
            }
        }

        let msg_response_id = state.acquire_message_id();
        let msg_response = Message {
            src: dest,
            dest: src,
            body: MessageBody::AppendEntriesResponse {
                msg_id: msg_response_id,
                in_reply_to: msg_id,
                term: state.current_term,
                success,
                match_index,
            },
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_append_entries_response(
        &mut self,
        src: String,
        _dest: String,
        _msg_id: u64,
        term: u64,
        success: bool,
        match_index: u64,
    ) -> Result<()> {
        let state = self.state.clone();
        let mut state = state.lock().unwrap();
        self.update_term(&mut state, term);

        if success {
            state.match_index.insert(src.clone(), match_index);
            state.next_index.insert(src, match_index + 1);
            self.advance_commit_index(&mut state);
        } else {
            let next_index = state.next_index.get_mut(&src).unwrap();
            if *next_index > 1 {
                *next_index -= 1;
            }
        }
        Ok(())
    }

    fn run_election_timeout(&self) -> sync::mpsc::Sender<()> {
        let node_id = self.id.clone();
        let member_ids = self.member_ids.clone();
        let state = self.state.clone();
        let mut stub = Stub::new();
        let (reset_tx, reset_rx) = sync::mpsc::channel::<()>();
        thread::spawn(move || {
            let mut rng = rand::rng();
            loop {
                let timeout_ms = rng.random_range(150..300);
                let reset = reset_rx.recv_timeout(Duration::from_millis(timeout_ms));

                match reset {
                    Ok(_) => continue,
                    Err(sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }

                let mut state = state.lock().unwrap();
                if state.role == Role::Follower || state.role == Role::Candidate {
                    state.current_term += 1;
                    state.role = Role::Candidate;
                    state.voted_for = Some(node_id.clone());
                    state.leader = None;
                    state.votes_granted = vec![node_id.clone()];

                    eprintln!(
                        "became candidate, starting a new election. term={}",
                        state.current_term
                    );

                    for member_id in &member_ids {
                        if member_id == &node_id {
                            continue;
                        }

                        let msg_id = state.acquire_message_id();
                        let msg = Message {
                            src: node_id.clone(),
                            dest: member_id.clone(),
                            body: MessageBody::RequestVote {
                                msg_id,
                                term: state.current_term,
                                last_log_index: state.log.last_index(),
                                last_log_term: state.log.last_term(),
                            },
                        };
                        stub.send_message(&msg).unwrap();
                    }
                }
            }
        });

        reset_tx
    }

    fn run_append_entries(&self, term: u64) {
        let node_id = self.id.clone();
        let member_ids = self.member_ids.clone();
        let state = self.state.clone();
        let mut stub = Stub::new();
        eprintln!("starting AppendEntries loop. term={}", term);
        thread::spawn(move || {
            loop {
                let mut state = state.lock().unwrap();
                if term != state.current_term || state.role != Role::Leader {
                    eprintln!(
                        "stopping AppendEntries loop. term={}, new_term={}",
                        term, state.current_term
                    );
                    break;
                }

                for member_id in &member_ids {
                    if member_id == &node_id {
                        continue;
                    }

                    let prev_log_index = state.next_index[member_id] - 1;
                    let prev_log_term = state.log.get(prev_log_index).map_or(0, |e| e.term);
                    let entries = state.log.get_since(state.next_index[member_id]).to_vec();

                    let msg_id = state.acquire_message_id();
                    let msg = Message {
                        src: node_id.clone(),
                        dest: member_id.clone(),
                        body: MessageBody::AppendEntries {
                            msg_id,
                            term: state.current_term,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit: state.commit_index,
                        },
                    };

                    stub.send_message(&msg).unwrap();
                }

                drop(state);
                thread::sleep(Duration::from_millis(50));
            }
        });
    }

    fn run_apply_committed_entries(&self) -> sync::mpsc::Sender<()> {
        let state = self.state.clone();
        let (notify_tx, notify_rx) = sync::mpsc::channel::<()>();
        thread::spawn(move || {
            let mut stub = Stub::new();
            loop {
                let noti = notify_rx.recv_timeout(Duration::from_millis(3000));
                if let Err(sync::mpsc::RecvTimeoutError::Disconnected) = noti {
                    break;
                }
                let mut state = state.lock().unwrap();

                while state.last_applied < state.commit_index {
                    state.last_applied += 1;
                    let entry = state.log.get(state.last_applied).unwrap().clone();
                    let map = &mut state.map;

                    let msg_response_body_gen = match entry.msg.body {
                        MessageBody::Read { msg_id, key } => Self::apply_read(map, msg_id, key),
                        MessageBody::Write { msg_id, key, value } => {
                            Self::apply_write(map, msg_id, key, value)
                        }
                        MessageBody::Cas {
                            msg_id,
                            key,
                            from,
                            to,
                        } => Self::apply_cas(map, msg_id, key, from, to),
                        _ => unreachable!(),
                    };

                    if state.role == Role::Leader {
                        let new_msg_id = state.acquire_message_id();
                        let msg_response_body = msg_response_body_gen(new_msg_id);
                        let msg_response = Message {
                            src: entry.msg.dest,
                            dest: entry.msg.src,
                            body: msg_response_body,
                        };
                        stub.send_message(&msg_response).unwrap();
                    }
                }
            }
        });
        notify_tx
    }

    fn apply_read(
        map: &mut HashMap<u64, i64>,
        msg_id: u64,
        key: u64,
    ) -> Box<dyn FnOnce(u64) -> MessageBody> {
        let value = map.get(&key).copied();

        Box::new(move |new_msg_id| {
            let msg_response_body = if let Some(value) = value {
                MessageBody::ReadOk {
                    msg_id: new_msg_id,
                    in_reply_to: msg_id,
                    value,
                }
            } else {
                MessageBody::Error {
                    msg_id: new_msg_id,
                    in_reply_to: msg_id,
                    code: ErrorCode::KeyDoesNotExist,
                    text: "key does not exist".to_string(),
                }
            };
            msg_response_body
        })
    }

    fn apply_write(
        map: &mut HashMap<u64, i64>,
        msg_id: u64,
        key: u64,
        value: i64,
    ) -> Box<dyn FnOnce(u64) -> MessageBody> {
        map.insert(key, value);

        Box::new(move |new_msg_id| MessageBody::WriteOk {
            msg_id: new_msg_id,
            in_reply_to: msg_id,
        })
    }

    fn apply_cas(
        map: &mut HashMap<u64, i64>,
        msg_id: u64,
        key: u64,
        from: i64,
        to: i64,
    ) -> Box<dyn FnOnce(u64) -> MessageBody> {
        let entry = map.entry(key);
        let result = if let Entry::Occupied(mut e) = entry {
            let v = e.get_mut();
            if *v == from {
                *v = to;
                Some(Ok(()))
            } else {
                Some(Err(()))
            }
        } else {
            None
        };

        Box::new(move |new_msg_id| match result {
            Some(Ok(())) => MessageBody::CasOk {
                msg_id: new_msg_id,
                in_reply_to: msg_id,
            },
            Some(Err(())) => MessageBody::Error {
                msg_id: new_msg_id,
                in_reply_to: msg_id,
                code: ErrorCode::PreconditionFailed,
                text: "precondition failed".to_string(),
            },
            None => MessageBody::Error {
                msg_id: new_msg_id,
                in_reply_to: msg_id,
                code: ErrorCode::KeyDoesNotExist,
                text: "key does not exist".to_string(),
            },
        })
    }

    fn update_term(&mut self, state: &mut State, term: u64) {
        if term > state.current_term {
            state.current_term = term;
            state.role = Role::Follower;
            state.voted_for = None;
            state.leader = None;
            self.election_timeout_reset = Some(self.run_election_timeout());

            eprintln!(
                "became follower, observing a newer term. term={}",
                state.current_term
            );
        }
    }

    fn advance_commit_index(&mut self, state: &mut State) {
        let mut match_index: Vec<u64> = state
            .match_index
            .iter()
            .map(|(node_id, i)| {
                if node_id == &self.id {
                    state.log.last_index()
                } else {
                    *i
                }
            })
            .collect();
        match_index.sort_unstable_by(|a, b| b.cmp(a));
        let majority_index = match_index[self.member_ids.len() / 2];

        if majority_index > state.commit_index {
            if let Some(entry) = state.log.get(majority_index) {
                if entry.term == state.current_term {
                    state.commit_index = majority_index;
                    self.apply_committed_entries_notify
                        .as_ref()
                        .map(|n| n.send(()));
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let node = Node::new();
    node.run()
}
