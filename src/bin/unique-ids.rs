use maelstrom_rust::{BaseNode, Message, Node, NodeRunner};
use std::{collections::HashMap, io};

#[derive(Default)]
struct UniqueIdsNode {
    n: u64,
    base: BaseNode,
}

impl Node for UniqueIdsNode {
    fn id(&self) -> &String {
        self.base.id()
    }

    fn handle_request(&mut self, req: &Message) -> Result<(), Box<dyn std::error::Error>> {
        match req.body.typ.as_str() {
            "generate" => {
                let id = format!("{}-{}", self.id(), self.n);
                self.n += 1;

                let mut extra = HashMap::new();
                extra.insert("id".to_string(), serde_json::json!(id));
                self.send_message(req.src.clone(), "generate_ok", req.body.msg_id, extra)
            }
            _ => self.base.handle_request(req),
        }
    }

    fn send_message(
        &mut self,
        dest: String,
        typ: &'static str,
        in_reply_to: Option<u64>,
        extra: HashMap<String, serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.base.send_message(dest, typ, in_reply_to, extra)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let handle = stdin.lock();

    let node = UniqueIdsNode::default();
    let node_runner = NodeRunner::new(node);
    node_runner.run(handle)
}
