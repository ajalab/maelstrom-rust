use maelstrom_rust::{ErrorCode, Message, Stub};

use anyhow::Result;
use std::collections::HashMap;

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
    /// (maelstrom) MessageBody to [MessageBody::Init].
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

    Replicate {
        msg_id: u64,
        key: u64,
        value: i64,
        rpc_type: String,
        rpc_msg_id: u64,
        rpc_src: String,
    },
}

#[derive(Default)]
struct Chain {
    id: String,
    head: String,
    tail: String,
    next: Option<String>,
}

impl Chain {
    fn new(id: String, nodes: Vec<String>) -> Self {
        let head = nodes.first().cloned().unwrap_or_default();
        let tail = nodes.last().cloned().unwrap_or_default();
        let next = nodes
            .iter()
            .position(|n| *n == id)
            .and_then(|pos| nodes.get(pos + 1))
            .cloned();

        Chain {
            id,
            head,
            tail,
            next,
        }
    }

    fn is_head(&self) -> bool {
        self.id == self.head
    }

    fn is_tail(&self) -> bool {
        self.id == self.tail
    }
}

struct Node {
    stub: Stub,
    id: String,
    next_message_id: u64,

    chain: Chain,
    map: HashMap<u64, i64>,
}

impl Node {
    fn new() -> Self {
        Node {
            stub: Stub::new(),
            id: String::new(),
            next_message_id: 1,
            map: HashMap::new(),
            chain: Chain::default(),
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let Message { src, dest, body } = self.stub.get_message::<MessageBody>()?;
            match body {
                MessageBody::Init {
                    msg_id,
                    node_id,
                    node_ids,
                } => self.handle_init(src, dest, msg_id, node_id, node_ids)?,
                MessageBody::Read { msg_id, key } => self.handle_read(src, dest, msg_id, key)?,
                MessageBody::Write { msg_id, key, value } => {
                    self.handle_write(src, dest, msg_id, key, value)?
                }
                MessageBody::Cas {
                    msg_id,
                    key,
                    from,
                    to,
                } => self.handle_cas(src, dest, msg_id, key, from, to)?,
                MessageBody::Replicate {
                    msg_id: _,
                    key,
                    value,
                    rpc_type,
                    rpc_msg_id,
                    rpc_src,
                } => self.handle_replicate(key, value, rpc_type, rpc_msg_id, rpc_src)?,
                _ => unreachable!(),
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
        self.chain = Chain::new(self.id.clone(), node_ids);

        eprintln!("Initialized node #{}", self.id);

        let msg_response = Message {
            src: dest,
            dest: src,
            body: MessageBody::InitOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
            },
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_read(&mut self, src: String, dest: String, msg_id: u64, key: u64) -> Result<()> {
        if !self.chain.is_tail() {
            let msg_forward = Message {
                src,
                dest: self.chain.tail.clone(),
                body: MessageBody::Read { msg_id, key },
            };
            return self.stub.send_message(&msg_forward);
        }

        let value = self.map.get(&key).copied();
        let msg_response_body = if let Some(v) = value {
            MessageBody::ReadOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
                value: v,
            }
        } else {
            MessageBody::Error {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
                code: ErrorCode::KeyDoesNotExist,
                text: "key does not exist".to_string(),
            }
        };
        let msg_response = Message {
            src: dest.clone(),
            dest: src.clone(),
            body: msg_response_body,
        };

        self.stub.send_message(&msg_response)
    }

    fn handle_write(
        &mut self,
        src: String,
        _dest: String,
        msg_id: u64,
        key: u64,
        value: i64,
    ) -> Result<()> {
        if !self.chain.is_head() {
            let msg_forward = Message {
                src,
                dest: self.chain.head.clone(),
                body: MessageBody::Write { msg_id, key, value },
            };
            return self.stub.send_message(&msg_forward);
        }
        self.handle_replicate(key, value, "write".to_string(), msg_id, src)
    }

    fn handle_cas(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        key: u64,
        from: i64,
        to: i64,
    ) -> Result<()> {
        if !self.chain.is_head() {
            let msg_forward = Message {
                src,
                dest: self.chain.head.clone(),
                body: MessageBody::Cas {
                    msg_id,
                    key,
                    from,
                    to,
                },
            };
            return self.stub.send_message(&msg_forward);
        }

        let value = self.map.get(&key).copied();
        let msg_error_body = match value {
            Some(v) if v != from => {
                let error_text =
                    format!("current value {} does not match 'from' value {}", v, from);
                Some(MessageBody::Error {
                    msg_id: self.acquire_message_id(),
                    in_reply_to: msg_id,
                    code: ErrorCode::PreconditionFailed,
                    text: error_text,
                })
            }
            None => {
                let error_text = format!("key '{}' does not exist", key);
                Some(MessageBody::Error {
                    msg_id: self.acquire_message_id(),
                    in_reply_to: msg_id,
                    code: ErrorCode::KeyDoesNotExist,
                    text: error_text,
                })
            }
            _ => None,
        };

        if let Some(msg_response_body) = msg_error_body {
            let msg_response = Message {
                src: dest,
                dest: src,
                body: msg_response_body,
            };
            return self.stub.send_message(&msg_response);
        }

        self.handle_replicate(key, to, "cas".to_string(), msg_id, src)
    }

    fn handle_replicate(
        &mut self,
        key: u64,
        value: i64,
        rpc_type: String,
        rpc_msg_id: u64,
        rpc_src: String,
    ) -> Result<()> {
        self.map.insert(key, value);

        if let Some(next) = &self.chain.next {
            let msg_forward = Message {
                src: self.id.clone(),
                dest: next.clone(),
                body: MessageBody::Replicate {
                    msg_id: self.acquire_message_id(),
                    key,
                    value,
                    rpc_type,
                    rpc_msg_id,
                    rpc_src,
                },
            };
            self.stub.send_message(&msg_forward)
        } else {
            let msg_response_id = self.acquire_message_id();
            let mesg_response_body = match rpc_type.as_str() {
                "write" => MessageBody::WriteOk {
                    msg_id: msg_response_id,
                    in_reply_to: rpc_msg_id,
                },
                "cas" => MessageBody::CasOk {
                    msg_id: msg_response_id,
                    in_reply_to: rpc_msg_id,
                },
                _ => unreachable!(),
            };
            let msg_response = Message {
                src: self.id.clone(),
                dest: rpc_src,
                body: mesg_response_body,
            };
            self.stub.send_message(&msg_response)
        }
    }

    fn acquire_message_id(&mut self) -> u64 {
        let msg_id = self.next_message_id;
        self.next_message_id += 1;
        msg_id
    }
}

fn main() -> Result<()> {
    let node = Node::new();
    node.run()
}
