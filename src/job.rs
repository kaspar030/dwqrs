extern crate serde;
extern crate serde_json;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Result {
    pub status: i32,
    pub runtime: f64,
    pub worker: String,
    pub body: CmdBody,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct ResultBody {
    pub job_id: String,
    pub state: String,
    pub result: Result,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct CmdBody {
    pub repo: String,
    pub commit: String,
    pub command: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl CmdBody {
    pub fn new(
        repo: String,
        commit: String,
        command: String,
        control_queue: Option<&str>,
    ) -> CmdBody {
        let mut body = CmdBody {
            repo: repo,
            commit: commit,
            command: command,
            extra: HashMap::new(),
        };
        match control_queue {
            Some(control_queue) => {
                body.extra.insert(
                    "control_queues".to_string(),
                    serde_json::to_value(vec![control_queue]).unwrap(),
                );
                ()
            }
            None => (),
        };
        return body;
    }
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}
