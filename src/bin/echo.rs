use maelstrom_rust::{Message, Stub};

use anyhow::Result;
use std::io;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    Init {
        msg_id: u64,
        node_id: String,
    },
    Echo {
        msg_id: u64,
        echo: serde_json::Value,
    },
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Response {
    InitOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    EchoOk {
        msg_id: u64,
        in_reply_to: u64,
        echo: serde_json::Value,
    },
}

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
            let Message { src, dest, body } = self.stub.get_message::<Request>()?;
            match body {
                Request::Init { msg_id, node_id } => {
                    self.handle_init(src, dest, msg_id, node_id)?
                }
                Request::Echo { msg_id, echo } => self.handle_echo(src, dest, msg_id, echo)?,
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

    fn handle_echo(
        &mut self,
        src: String,
        dest: String,
        msg_id: u64,
        echo: serde_json::Value,
    ) -> Result<()> {
        let msg_response = Message {
            src: dest,
            dest: src,
            body: Response::EchoOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
                echo,
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
