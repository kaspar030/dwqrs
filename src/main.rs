extern crate clap;
extern crate disque;

mod job;

use std::str::from_utf8;
use std::time::Duration;

use clap::{App, Arg};
use disque::{AddJobBuilder, Disque};
use failure::{format_err, Error};
use job::{CmdBody, ResultBody};

extern crate rand;

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
        .version("0.1")
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
            Arg::with_name("repo")
                .short('r')
                .value_name("URL")
                .help("Git repository to check out")
                .required(true)
                .env("DWQ_REPO")
        )
        .arg(
            Arg::with_name("commit")
                .short('c')
                .value_name("COMMIT")
                .help("Git commit to check out")
                .required(true)
                .env("DWQ_COMMIT")
        )
        .arg(
            Arg::with_name("command")
                .value_name("COMMAND")
                .help("Command to run")
                .required(true)
                .index(1),
        )
        .get_matches();

    /* handle arguments */
    let disque_url = matches
        .value_of("disque_url")
        .unwrap_or("redis://localhost:7711");

    let queue = matches.value_of("queue").unwrap_or("test");
    println!("queue: {}", queue);

    let verbose = matches.is_present("verbose");
    if verbose {
        println!("dwqc: status output enabled");
    }

    /* connect to disque */
    let disque = Disque::open(disque_url).unwrap();

    let control_queue = format!("control::{}", rand::random::<u64>());
    println!("dwqc: control queue: {}", control_queue);

    let mut body = CmdBody::new(
        matches.value_of("repo").unwrap().to_string(),
        matches.value_of("repo").unwrap().to_string(),
        matches.value_of("command").unwrap().to_string(),
    );
    body.extra.insert(
        "control_queues".to_string(),
        serde_json::to_value(vec![&control_queue]).unwrap(),
    );
    let body_json = serde_json::to_string(&body).unwrap();
    println!("body: {}", body_json);

    let jobid = AddJobBuilder::new(queue.as_bytes(), body_json.as_bytes(), 300 * 1000)
        .run(&disque)
        .unwrap();

    println!("dwqc: job id: {}", jobid);
    let result = disque.getjob(false, None, &[control_queue.as_bytes()])?;
    let (_res_q, _res_id, res_body_json) = match result {
        //None => bail!("dwq: timeout"),
        None => return Err(format_err!("timeout getting job result")),
        Some(t) => t,
    };

    let res_body = String::from_utf8(res_body_json).unwrap();
    println!("-- result: {}", res_body);
    let res_body: ResultBody = serde_json::from_str(&res_body).unwrap();

    if jobid != res_body.job_id {
        return Err(format_err!(
            "got unexpected job result: {} vs {}",
            &jobid,
            &res_body.job_id
        ));
    }

    let output = match res_body.result.extra.get("output") {
        Some(value) => match value.as_str() {
            Some(value) => value,
            None => "",
        },
        None => "",
    };

    print!("{}", output);

    std::process::exit(res_body.result.status);
}
