mod dwq;

extern crate disque;
use disque::{Disque, QueueQueryBuilder};
use std::str::from_utf8;

pub fn get_queues(disque: &Disque) -> Vec<String> {
    let queues = QueueQueryBuilder::new()
        .busyloop(true)
        .iter(disque)
        .unwrap()
        .collect::<Vec<_>>();

    let queues = queues
        .iter()
        .map(|x| String::from(from_utf8(x).unwrap()))
        .collect::<Vec<_>>();

    let mut queues = queues;
    queues.sort();

    queues
}
