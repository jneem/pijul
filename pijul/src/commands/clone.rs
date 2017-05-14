use clap::{SubCommand, ArgMatches, Arg};

use commands::{init, StaticSubcommand};
use error::Error;
use commands::remote::{Remote, parse_remote};
use regex::Regex;
use libpijul::DEFAULT_BRANCH;
use std::io::{Write, stderr};
use std::process::exit;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("clone")
        .about("clone a remote branch")
        .arg(Arg::with_name("from")
            .help("Repository to clone.")
            .required(true))
        .arg(Arg::with_name("from_branch")
            .long("from-branch")
            .help("The branch to pull from")
            .takes_value(true))
        .arg(Arg::with_name("to_branch")
            .long("to-branch")
            .help("The branch to pull into")
            .takes_value(true))
        .arg(Arg::with_name("to").help("Target."))
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .help("Port of the remote ssh server.")
            .takes_value(true)
            .validator(|val| {
                let x: Result<u16, _> = val.parse();
                match x {
                    Ok(_) => Ok(()),
                    Err(_) => Err(val),
                }
            }));
}
#[derive(Debug)]
pub struct Params<'a> {
    pub from: Remote<'a>,
    pub from_branch: &'a str,
    pub to: Remote<'a>,
    pub to_branch: &'a str,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    // At least one must not use its "port" argument
    let from = parse_remote(args.value_of("from").unwrap(),
                            args.value_of("port").and_then(|x| Some(x.parse().unwrap())),
                            None);
    let to = if let Some(to) = args.value_of("to") {
        parse_remote(to,
                     args.value_of("port").and_then(|x| Some(x.parse().unwrap())),
                     None)
    } else {
        let basename = Regex::new(r"([^/:]*)").unwrap();
        let from = args.value_of("from").unwrap();
        if let Some(to) = basename.captures_iter(from).last().and_then(|to| to.get(1)) {
            parse_remote(to.as_str(),
                         args.value_of("port").and_then(|x| Some(x.parse().unwrap())),
                         None)
        } else {
            panic!("Could not parse target")
        }
    };
    let from_branch = args.value_of("from_branch").unwrap_or(DEFAULT_BRANCH);
    let to_branch = args.value_of("to_branch").unwrap_or(from_branch);
    Params {
        from: from,
        from_branch: from_branch,
        to: to,
        to_branch: to_branch,
    }
}



pub fn run(args: &Params) -> Result<(), Error> {
    debug!("{:?}", args);
    match args.from {
        Remote::Local { ref path } => {
            let mut to_session = try!(args.to.session());
            debug!("remote init");
            try!(to_session.remote_init());
            debug!("pushable?");
            let pushable = to_session.pushable_patches(args.from_branch, args.to_branch, path)?;
            debug!("pushable = {:?}", pushable);
            let pushable = pushable.into_iter().map(|(h, _)| h).collect();
            to_session.push(path, args.to_branch, &pushable)
        }
        _ => {
            match args.to {
                Remote::Local { ref path } => {
                    // This is "darcs get"
                    try!(init::run(&init::Params {
                        location: Some(path),
                        allow_nested: false,
                    }));
                    let mut session = try!(args.from.session());
                    let pullable:Vec<_> = try!(session.pullable_patches(
                        args.from_branch,
                        args.to_branch,
                        path
                    )).iter().collect();
                    session.pull(path, args.to_branch, &pullable)
                }
                _ =>
                    // Clone between remote repositories.
                    unimplemented!(),
            }
        }
    }
}

pub fn explain(res: Result<(), Error>) {
    match res {
        Ok(()) => (),
        Err(Error::InARepository(p)) => {
            write!(stderr(), "error: Cannot clone onto / into existing repository {}", p.display()).unwrap();
            exit(1)
        },
        Err(e) => {
            write!(stderr(), "error: {}", e).unwrap();
            exit(1)
        }
    }
}
