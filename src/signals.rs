extern crate signal_hook;
extern crate crossbeam_channel;


use std::io;
use std::thread;
use crossbeam_channel::{bounded, Receiver};
use signal_hook::iterator::Signals;
use signal_hook::SIGINT;

// Creates a channel that gets a message every time `SIGINT` is signalled.
pub fn sigint_notifier() -> io::Result<Receiver<()>> {
    let (s, r) = bounded(100);
    let signals = Signals::new(&[SIGINT])?;

    thread::spawn(move || {
        for _ in signals.forever() {
            if s.send(()).is_err() {
                break;
            }
        }
    });

    Ok(r)
}
