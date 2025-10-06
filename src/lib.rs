use std::{collections::HashMap, io::BufRead};

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct MessageBody {
    #[serde(rename = "type")]
    pub typ: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

pub trait Node {
    fn id(&self) -> &String;
    fn handle_request(&mut self, req: &Message) -> Result<(), Box<dyn std::error::Error>>;
    fn send_message(
        &mut self,
        dest: String,
        typ: &'static str,
        in_reply_to: Option<u64>,
        extra: HashMap<String, serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

#[derive(Default)]
pub struct BaseNode {
    id: String,
    next_msg_id: u64,
}

impl Node for BaseNode {
    fn id(&self) -> &String {
        &self.id
    }

    fn handle_request(&mut self, req: &Message) -> Result<(), Box<dyn std::error::Error>> {
        match req.body.typ.as_str() {
            "init" => {
                // Initialize node
                self.id = req.body.extra["node_id"].as_str().unwrap().to_string();
                eprintln!("Initialized node #{}", self.id);
                self.send_message(req.src.clone(), "init_ok", req.body.msg_id, HashMap::new())?;
            }
            _ => {
                eprintln!("Unknown request type: {}", req.body.typ);
            }
        }
        Ok(())
    }

    fn send_message(
        &mut self,
        dest: String,
        typ: &'static str,
        in_reply_to: Option<u64>,
        extra: HashMap<String, serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = MessageBody {
            typ: typ.to_string(),
            msg_id: Some(self.next_msg_id),
            in_reply_to,
            extra,
        };
        let msg = Message {
            src: self.id.clone(),
            dest,
            body,
        };
        let msg_str = serde_json::to_string(&msg)?;

        println!("{}", msg_str);
        eprintln!("Sent {}", msg_str);
        self.next_msg_id += 1;

        Ok(())
    }
}

pub struct NodeRunner<N: Node> {
    node: N,
}

impl<N: Node> NodeRunner<N> {
    pub fn new(node: N) -> Self {
        Self { node }
    }

    pub fn run(mut self, buf_read: impl BufRead) -> Result<(), Box<dyn std::error::Error>> {
        for line in buf_read.lines() {
            let line = line?;
            eprintln!("Received {}", line);

            let req = serde_json::from_str::<Message>(&line)?;
            self.node.handle_request(&req)?;
        }
        Ok(())
    }
}
