extern crate ansi_escapes;
extern crate atomic_counter;
extern crate clap;
extern crate collect_slice;
extern crate crossbeam_channel;
extern crate disque;
extern crate indicatif;
extern crate itertools;
extern crate rand;

mod job;
mod signals;

//use std::str::from_utf8;
//use std::time::Duration;
use std::collections::HashSet;
use std::io;
use std::io::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static GLOBAL_JOBS_COUNT: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_ABORT: AtomicBool = AtomicBool::new(false);

use clap::{crate_version, App, Arg};
use crossbeam_channel::{bounded, select, tick};
use disque::{AddJobBuilder, Disque};
use failure::{format_err, Error};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;

use job::{CmdBody, ResultBody};
use signals::sigint_notifier;

fn main() {
    let result = try_main();
    match result {
        Err(e) => {
            eprintln!("dwqc: error: {}", e);
            std::process::exit(1);
        }
        Ok(_) => {}
    };
}

fn try_main() -> Result<(), Error> {
    let matches = App::new("dwqc in rust")
        .version(crate_version!())
        .author("Kaspar Schleiser <kaspar@schleiser.de>")
        .about("Does awesome things")
        .arg(
            Arg::with_name("queue")
                .short('q')
                .long("queue")
                .help("Send job to specified queue")
                .required(false)
                .value_name("QUEUE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("disque_url")
                .short('u')
                .long("disque")
                .help("Connect to specified disque instance")
                .required(false)
                .value_name("URL")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("verbose")
                .short('v')
                .help("Enable status output"),
        )
        .arg(
            Arg::with_name("progress")
                .short('P')
                .help("Enable progress output"),
        )
        .arg(
            Arg::with_name("repo")
                .short('r')
                .value_name("URL")
                .help("Git repository to check out")
                .required(true)
                .env("DWQ_REPO"),
        )
        .arg(
            Arg::with_name("commit")
                .short('c')
                .value_name("COMMIT")
                .help("Git commit to check out")
                .required(true)
                .env("DWQ_COMMIT"),
        )
        .arg(
            Arg::with_name("command")
                .value_name("COMMAND")
                .help("Command to run")
                .index(1),
        )
        .get_matches();

    /* handle arguments */
    let disque_url = matches
        .value_of("disque_url")
        .unwrap_or("redis://localhost:7711")
        .to_string();

    let queue = matches.value_of("queue").unwrap_or("test").to_string();
    println!("queue: {}", queue);

    let verbose = matches.is_present("verbose");
    if verbose {
        println!("dwqc: status output enabled");
    }

    let control_queue = format!("control::{}", rand::random::<u64>());
    if verbose {
        println!("dwqc: control queue: {}", control_queue);
    }

    let progress = matches.is_present("progress");

    /* set up event loop */
    let ctrl_c = sigint_notifier().unwrap();
    let update = tick(Duration::from_secs(1));
    let (tx_cmds, rx_cmds) = bounded::<String>(1024);
    let (tx_jobid, rx_jobid) = bounded(1024);
    let (tx_result, rx_result) = bounded(1024);
    let (tx_reader, rx_reader) = bounded(1);

    // keep track of jobs
    let mut jobs = HashSet::new();
    let mut more_jobs_coming = true;
    let mut jobs_left = 0;

    let disque_url_sender = disque_url.clone();
    let job_sender = thread::spawn(move || {
        /* connect to disque */
        let disque_url: &str = &disque_url_sender;
        let disque = Disque::open(disque_url).unwrap();
        let tx = tx_jobid.clone();
        loop {
            let body_json = match rx_cmds.recv() {
                Ok(value) => value,
                Err(_) => break,
            };
            // send job
            let jobid = AddJobBuilder::new(queue.as_bytes(), body_json.as_bytes(), 300 * 1000)
                .ttl(24 * 60 * 60 * 1000)
                .run(&disque)
                .unwrap();

            tx.send(jobid).unwrap();
        }
        println!("dwqc: job sender done.");
    });

    let disque_url_receiver = disque_url.clone();
    let control_queue_receiver = control_queue.clone();
    thread::spawn(move || {
        fn get_result(disque: &Disque, control_queue: &str) -> Result<ResultBody, Error> {
            let result = disque.getjob(false, None, &[control_queue.as_bytes()])?;
            let (_res_q, _res_id, res_body_json) = match result {
                None => return Err(format_err!("timeout getting job result")),
                Some(t) => t,
            };

            let res_body: ResultBody = serde_json::from_slice(&res_body_json).unwrap();
            Ok(res_body)
        };
        /* connect to disque */
        let disque_url_receiver: &str = &disque_url_receiver;
        let disque = Disque::open(disque_url_receiver).unwrap();

        let tx = tx_result.clone();

        loop {
            let res_body = get_result(&disque, &control_queue_receiver);
            match tx.send(res_body) {
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        println!("dwqc: result_receiver done.");
    });

    fn handle_result(jobs: &HashSet<String>, res_body: &ResultBody) -> bool {
        if !jobs.contains(&res_body.job_id) {
            eprintln!("got unexpected job result (id={})", &res_body.job_id);
            return false;
        }

        let output = match res_body.result.extra.get("output") {
            Some(value) => match value.as_str() {
                Some(value) => value,
                None => "",
            },
            None => "",
        };
        print!("{}", output);
        return true;
    };

    if matches.is_present("command") && !matches.is_present("stdin") {
        // create json job body
        let body_json = CmdBody::new(
            matches.value_of("repo").unwrap().to_string(),
            matches.value_of("commit").unwrap().to_string(),
            matches.value_of("command").unwrap().to_string(),
            Some(&control_queue),
        )
        .to_json();

        GLOBAL_JOBS_COUNT.fetch_add(1, Ordering::SeqCst);
        tx_cmds.send(body_json.clone())?;
        more_jobs_coming = false;
    } else {
        let tx_cmds = tx_cmds.clone();
        let tx_reader = tx_reader.clone();
        thread::spawn(move || {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let line = line.unwrap().to_string();
                //println!("job: {}", line);
                // create json job body
                let body_json = CmdBody::new(
                    matches.value_of("repo").unwrap().to_string(),
                    matches.value_of("commit").unwrap().to_string(),
                    line,
                    Some(&control_queue),
                )
                .to_json();

                GLOBAL_JOBS_COUNT.fetch_add(1, Ordering::SeqCst);
                tx_cmds.send(body_json.clone()).unwrap();
            }
            //println!("job reader done");
            tx_reader.send(true).unwrap();
        });
    }

    let mut result = 0i32;
    let mut jobs_total = GLOBAL_JOBS_COUNT.load(Ordering::SeqCst);
    let mut jobs_done = 0;
    //let bar = ProgressBar::new(jobs_total as u64);
    let bar = match progress {
        true => ProgressBar::new(0u64),
        false => ProgressBar::hidden(),
    };

    if !more_jobs_coming {
        bar.set_style(ProgressStyle::default_bar())
    } else {
        bar.set_style(ProgressStyle::default_spinner());
        bar.set_message("collecting jobs");
    }

    while more_jobs_coming || (jobs_total - jobs_done) > 0 {
        select! {
            recv(rx_reader) -> _ => {
                more_jobs_coming = false;
                jobs_total = GLOBAL_JOBS_COUNT.load(Ordering::SeqCst);
                bar.set_style(ProgressStyle::default_bar().template("{msg} {spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} eta: {eta}").progress_chars("=>"));
                bar.set_length(jobs_total as u64);
                bar.set_message("");
                bar.println("dwqc: all jobs collected");
            }
            recv(update) -> _ => {
                bar.tick();
            }
            recv(rx_jobid) -> jobid => {
                let jobid = match jobid {
                    Err(_) => {
                        continue;
                    },
                    Ok(value) => value,
                };
                jobs.insert(jobid);
                bar.tick();
            }
            recv(rx_result) -> res_body => {
                if progress {
                    print!("{}", ansi_escapes::EraseLines(2));
                }
                let res_body = res_body.unwrap().unwrap();
                if handle_result(&jobs, &res_body) == false {
                    continue;
                }
                if res_body.result.status != 0 {
                    result = 1;
                }

                jobs_done += 1;
                jobs_total = GLOBAL_JOBS_COUNT.load(Ordering::SeqCst);
                if progress {
                    println!();
                    bar.set_length(jobs_total as u64);
                    bar.set_position(jobs_done as u64);
                    bar.println("");
                }
            }
            recv(ctrl_c) -> _ => {
                println!();
                println!("dwqc: aborted.");
                GLOBAL_ABORT.store(true, Ordering::Relaxed);
                result = 1;
                break;
            }
        }
    }

    bar.finish_and_clear();
    drop(tx_cmds);
    drop(rx_result);
    drop(rx_jobid);

    if GLOBAL_ABORT.load(Ordering::Relaxed) {
        if !jobs.is_empty() {
            println!("dwqc: cancelling jobs...");
            let disque = Disque::open(&disque_url as &str).unwrap();
            for chunk in &jobs.drain().chunks(4096) {
                let job_ids = chunk.collect::<Vec<String>>();
                let job_ids: Vec<&[u8]> = job_ids.iter().map(String::as_bytes).collect();
                disque.deljobs(&job_ids[..])?;
            }
        }
    }

    job_sender.join().unwrap();
    std::process::exit(result);
    //result_receiver.join().unwrap();
}
