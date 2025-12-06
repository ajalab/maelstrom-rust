use anyhow::Result;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::io::{Stdin, Stdout, Write, stdin, stdout};

#[repr(u8)]
#[derive(Clone, Copy, Serialize_repr, Deserialize_repr)]
pub enum ErrorCode {
    Crash = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}

pub struct Stub {
    stdin: Stdin,
    stdout: Stdout,
}

impl Stub {
    pub fn new() -> Self {
        Stub {
            stdin: stdin(),
            stdout: stdout(),
        }
    }

    pub fn get_message<T>(&mut self) -> Result<Message<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut line = String::new();
        self.stdin.read_line(&mut line)?;
        let msg = serde_json::from_str::<Message<T>>(&line)?;
        Ok(msg)
    }

    pub fn send_message<T>(&mut self, msg: &Message<T>) -> Result<()>
    where
        T: serde::Serialize,
    {
        let msg_str = serde_json::to_string(msg)?;
        writeln!(self.stdout, "{}", msg_str)?;
        Ok(())
    }
}
