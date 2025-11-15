use maelstrom_rust::{ErrorCode, Message, MessageBody, Stub};

use anyhow::Result;
use std::{collections::HashMap, io};

struct LinKvSingleNode {
    stub: Stub,
    id: String,
    next_message_id: u64,

    map: HashMap<u64, i64>,
}

impl LinKvSingleNode {
    fn new(stub: Stub) -> Self {
        LinKvSingleNode {
            stub,
            id: String::new(),
            next_message_id: 1,
            map: HashMap::new(),
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let msg = self.stub.get_message()?;
            let typ = msg.body.typ();
            match typ {
                "init" => self.handle_init(&msg)?,
                "read" => self.handle_read(&msg)?,
                "write" => self.handle_write(&msg)?,
                "cas" => self.handle_cas(&msg)?,
                _ => return Err(anyhow::anyhow!("Unknown message type: {}", typ)),
            }
        }
    }

    fn handle_read(&mut self, msg: &Message) -> Result<()> {
        let key = msg.body.field_as_u64("key")?;
        let value = self.map.get(&key);

        let msg_response_body = if let Some(v) = value {
            let mut extra = HashMap::new();
            extra.insert("value".to_string(), serde_json::json!(v));
            MessageBody::new(
                "read_ok",
                self.acquire_message_id(),
                msg.body.msg_id(),
                extra,
            )
        } else {
            MessageBody::error(
                self.acquire_message_id(),
                msg.body.msg_id(),
                ErrorCode::KeyDoesNotExist,
                "key does not exist",
            )
        };
        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: msg_response_body,
        };

        self.stub.send_message(&msg_response)
    }

    fn handle_write(&mut self, msg: &Message) -> Result<()> {
        let key = msg.body.field_as_u64("key")?;
        let value = msg.body.field_as_i64("value")?;
        self.map.insert(key, value);

        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: MessageBody::new(
                "write_ok",
                self.acquire_message_id(),
                msg.body.msg_id(),
                HashMap::new(),
            ),
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_cas(&mut self, msg: &Message) -> Result<()> {
        let key = msg.body.field_as_u64("key")?;
        let from = msg.body.field_as_i64("from")?;
        let to = msg.body.field_as_i64("to")?;

        let msg_response_body = match self.map.get(&key) {
            Some(&v) => {
                if v != from {
                    let error_text =
                        format!("current value {} does not match 'from' value {}", v, from);
                    MessageBody::error(
                        self.acquire_message_id(),
                        msg.body.msg_id(),
                        ErrorCode::PreconditionFailed,
                        &error_text,
                    )
                } else {
                    self.map.insert(key, to);
                    MessageBody::new(
                        "cas_ok",
                        self.acquire_message_id(),
                        msg.body.msg_id(),
                        HashMap::new(),
                    )
                }
            }
            _ => {
                let error_text = format!("key '{}' does not exist", key);
                MessageBody::error(
                    self.acquire_message_id(),
                    msg.body.msg_id(),
                    ErrorCode::KeyDoesNotExist,
                    &error_text,
                )
            }
        };

        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: msg_response_body,
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_init(&mut self, msg: &Message) -> Result<()> {
        self.id = msg.body.field_as_str("node_id")?.to_string();
        eprintln!("Initialized node #{}", self.id);

        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: MessageBody::new(
                "init_ok",
                self.acquire_message_id(),
                msg.body.msg_id(),
                HashMap::new(),
            ),
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
    let node = LinKvSingleNode::new(stub);
    node.run()
}
