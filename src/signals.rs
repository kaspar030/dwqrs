extern crate crossbeam_channel;
extern crate signal_hook;

use crossbeam_channel::{bounded, Receiver};
use signal_hook::iterator::Signals;
use signal_hook::{SIGINT, SIGPIPE, SIGTERM};
use std::io;
use std::thread;

// Creates a channel that sends a message every time some signals arrive.
pub fn sigint_notifier() -> io::Result<Receiver<()>> {
    let (s, r) = bounded(100);
    let signals = Signals::new(&[SIGINT, SIGPIPE, SIGTERM])?;

    thread::spawn(move || {
        for _ in signals.forever() {
            if s.send(()).is_err() {
                break;
            }
        }
    });

    Ok(r)
}
