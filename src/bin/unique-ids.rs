use maelstrom_rust::{Message, Stub};

use anyhow::Result;
use std::io;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    Init { msg_id: u64, node_id: String },
    Generate { msg_id: u64 },
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Response {
    InitOk {
        msg_id: u64,
        in_reply_to: u64,
    },
    GenerateOk {
        msg_id: u64,
        in_reply_to: u64,
        id: String,
    },
}

struct UniqueIdsNode {
    stub: Stub,
    id: String,
    next_message_id: u64,
    n: u64,
}

impl UniqueIdsNode {
    fn new(stub: Stub) -> Self {
        UniqueIdsNode {
            stub,
            id: String::new(),
            next_message_id: 1,
            n: 0,
        }
    }

    fn run(mut self) -> Result<()> {
        loop {
            let Message { src, dest, body } = self.stub.get_message::<Request>()?;
            match body {
                Request::Init { msg_id, node_id } => {
                    self.handle_init(src, dest, msg_id, node_id)?
                }
                Request::Generate { msg_id } => self.handle_generate(src, dest, msg_id)?,
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

    fn handle_generate(&mut self, src: String, dest: String, msg_id: u64) -> Result<()> {
        let id = format!("{}-{}", self.id, self.n);
        self.n += 1;

        let msg_response = Message {
            src: dest,
            dest: src,
            body: Response::GenerateOk {
                msg_id: self.acquire_message_id(),
                in_reply_to: msg_id,
                id,
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
    let node = UniqueIdsNode::new(stub);
    node.run()
}
