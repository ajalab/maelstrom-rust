use anyhow::Result;
use std::collections::HashMap;
use std::io::{BufRead, StdinLock, StdoutLock, Write};

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum ErrorCode {
    Crash = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct MessageBody {
    #[serde(rename = "type")]
    typ: String,
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,

    #[serde(flatten)]
    extra: HashMap<String, serde_json::Value>,
}

impl MessageBody {
    pub fn new(
        typ: &str,
        msg_id: u64,
        in_reply_to: Option<u64>,
        extra: HashMap<String, serde_json::Value>,
    ) -> Self {
        MessageBody {
            typ: typ.to_string(),
            msg_id: Some(msg_id),
            in_reply_to,
            extra,
        }
    }

    pub fn error(msg_id: u64, in_reply_to: Option<u64>, code: ErrorCode, text: &str) -> Self {
        let mut extra = HashMap::new();
        extra.insert("code".to_string(), serde_json::json!(code as u8));
        extra.insert("text".to_string(), serde_json::json!(text));
        MessageBody {
            typ: "error".to_string(),
            msg_id: Some(msg_id),
            in_reply_to,
            extra,
        }
    }

    pub fn typ(&self) -> &str {
        &self.typ
    }

    pub fn msg_id(&self) -> Option<u64> {
        self.msg_id
    }

    pub fn field(&self, key: &str) -> Result<&serde_json::Value> {
        self.extra
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("field '{}' is missing", key))
    }

    pub fn field_as_str(&self, key: &str) -> Result<&str> {
        self.field(key)?.as_str().ok_or_else(|| {
            anyhow::anyhow!("field '{}' is not a string: {:?}", key, self.extra.get(key))
        })
    }
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
