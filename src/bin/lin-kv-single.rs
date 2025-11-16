use maelstrom_rust::{ErrorCode, Message, Stub};

use anyhow::Result;
use std::{collections::HashMap, io};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    Init {
        msg_id: u64,
        node_id: String,
    },
    Read {
        msg_id: u64,
        key: u64,
    },
    Write {
        msg_id: u64,
        key: u64,
        value: i64,
    },
    Cas {
        msg_id: u64,
        key: u64,
        from: i64,
        to: i64,
    },
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Response {
    InitOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    ReadOk {
        msg_id: u64,
        in_reply_to: u64,
        value: i64,
    },
    WriteOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    CasOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    Error {
        msg_id: u64,
        in_reply_to: u64,
        code: ErrorCode,
        text: String,
    },
}

struct Node {
    stub: Stub,
    id: String,
    next_message_id: u64,

    map: HashMap<u64, i64>,
}

impl Node {
    fn new(stub: Stub) -> Self {
        Node {
            stub,
            id: String::new(),
            next_message_id: 1,
            map: HashMap::new(),
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let Message { src, dest, body } = self.stub.get_message::<Request>()?;
            match body {
                Request::Init { msg_id, node_id } => {
                    self.handle_init(src, dest, msg_id, node_id)?
                }
                Request::Read { msg_id, key } => self.handle_read(src, dest, msg_id, key)?,
                Request::Write { msg_id, key, value } => {
                    self.handle_write(src, dest, msg_id, key, value)?
                }
                Request::Cas {
                    msg_id,
                    key,
                    from,
                    to,
                } => self.handle_cas(src, dest, msg_id, key, from, to)?,
            }
        }
    }

    fn handle_init(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        node_id: String,
    ) -> Result<()> {
        self.id = node_id;
        eprintln!("Initialized node #{}", self.id);

        let msg_response = Message {
            src: dest,
            dest: src,
            body: Response::InitOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
            },
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_read(&mut self, src: String, dest: String, msg_id: u64, key: u64) -> Result<()> {
        let value = self.map.get(&key).copied();

        let msg_response_body = if let Some(v) = value {
            Response::ReadOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
                value: v,
            }
        } else {
            Response::Error {
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
        dest: String,
        msg_id: u64,
        key: u64,
        value: i64,
    ) -> Result<()> {
        self.map.insert(key, value);

        let msg_response = Message {
            src: dest,
            dest: src,
            body: Response::WriteOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
            },
        };
        self.stub.send_message(&msg_response)
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
        let value = self.map.get(&key).copied();
        let msg_response_body = match value {
            Some(v) => {
                if v != from {
                    let error_text =
                        format!("current value {} does not match 'from' value {}", v, from);
                    Response::Error {
                        msg_id: self.acquire_message_id(),
                        in_reply_to: msg_id,
                        code: ErrorCode::PreconditionFailed,
                        text: error_text,
                    }
                } else {
                    self.map.insert(key, to);
                    Response::CasOk {
                        msg_id: self.acquire_message_id(),
                        in_reply_to: msg_id,
                    }
                }
            }
            _ => {
                let error_text = format!("key '{}' does not exist", key);
                Response::Error {
                    msg_id: self.acquire_message_id(),
                    in_reply_to: msg_id,
                    code: ErrorCode::KeyDoesNotExist,
                    text: error_text,
                }
            }
        };

        let msg_response = Message {
            src: dest,
            dest: src,
            body: msg_response_body,
        };
        self.stub.send_message(&msg_response)
    }

    fn acquire_message_id(&mut self) -> u64 {
        let msg_id = self.next_message_id;
        self.next_message_id += 1;
        msg_id
    }
}

fn main() -> Result<()> {
    let stub = Stub::new(io::stdin().lock(), io::stdout().lock());
    let node = Node::new(stub);
    node.run()
}
