use maelstrom_rust::{Message, MessageBody, Stub};

use anyhow::Result;
use std::{collections::HashMap, io};

struct EchoNode {
    stub: Stub,
    id: String,
    next_message_id: u64,
}

impl EchoNode {
    fn new(stub: Stub) -> Self {
        EchoNode {
            stub,
            id: String::new(),
            next_message_id: 1,
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let msg = self.stub.get_message()?;
            match msg.body.typ.as_str() {
                "init" => self.handle_init(&msg)?,
                "echo" => self.handle_echo(&msg)?,
                _ => return Err(anyhow::anyhow!("Unknown message type: {}", msg.body.typ)),
            }
        }
    }

    fn handle_init(&mut self, msg: &Message) -> Result<()> {
        self.id = msg.body.extra["node_id"].as_str().unwrap().to_string();
        eprintln!("Initialized node #{}", self.id);

        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: MessageBody {
                typ: "init_ok".to_string(),
                msg_id: Some(self.acquire_message_id()),
                in_reply_to: msg.body.msg_id,
                extra: HashMap::new(),
            },
        };
        self.stub.send_message(&msg_response)
    }

    fn handle_echo(&mut self, msg: &Message) -> Result<()> {
        let mut extra = HashMap::new();
        extra.insert("echo".to_string(), msg.body.extra["echo"].clone());

        let msg_response = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: MessageBody {
                typ: "echo_ok".to_string(),
                msg_id: Some(self.acquire_message_id()),
                in_reply_to: msg.body.msg_id,
                extra,
            },
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
    let node = EchoNode::new(stub);
    node.run()
}
