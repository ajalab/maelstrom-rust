use anyhow::Result;
use std::collections::HashMap;
use std::io::{BufRead, StdinLock, StdoutLock, Write};

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

pub struct Stub {
    stdin: StdinLock<'static>,
    stdout: StdoutLock<'static>,
}

impl Stub {
    pub fn new(stdin: StdinLock<'static>, stdout: StdoutLock<'static>) -> Self {
        Stub { stdin, stdout }
    }

    pub fn get_message(&mut self) -> Result<Message> {
        let mut line = String::new();
        self.stdin.read_line(&mut line)?;
        let msg = serde_json::from_str::<Message>(&line)?;
        Ok(msg)
    }

    pub fn send_message(&mut self, msg: &Message) -> Result<()> {
        let msg_str = serde_json::to_string(msg)?;
        writeln!(self.stdout, "{}", msg_str)?;
        Ok(())
    }
}
