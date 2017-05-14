use clap::{SubCommand, ArgMatches, Arg};

use error::Error;
use commands::{StaticSubcommand, remote, get_wd, default_explain};
use std::path::Path;
use libpijul::fs_representation::{find_repo_root, read_patch};
use libpijul::DEFAULT_BRANCH;
use meta::{Meta, Repository};
use super::ask;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("push")
        .about("push to a remote repository")
        .arg(Arg::with_name("remote").help("Repository to push to."))
        .arg(Arg::with_name("repository").help("Local repository."))
        .arg(Arg::with_name("local_branch")
            .long("from-branch")
            .help("The branch to push from")
            .takes_value(true))
        .arg(Arg::with_name("remote_branch")
            .long("to-branch")
            .help("The branch to push into")
            .takes_value(true))
        .arg(Arg::with_name("all")
            .short("a")
            .long("all")
            .help("Answer 'y' to all questions")
            .takes_value(false))
        .arg(Arg::with_name("set-default").long("set-default"))
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
    pub repository: Option<&'a Path>,
    pub remote_id: Option<&'a str>,
    pub yes_to_all: bool,
    pub set_default: bool,
    pub port: Option<u16>,
    pub local_branch: &'a str,
    pub remote_branch: &'a str,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = args.value_of("repository").and_then(|x| Some(Path::new(x)));
    let remote_id = args.value_of("remote");
    Params {
        repository: repository,
        remote_id: remote_id,
        yes_to_all: args.is_present("all"),
        set_default: args.is_present("set-default"),
        port: args.value_of("port").and_then(|x| Some(x.parse().unwrap())),
        local_branch: args.value_of("local_branch").unwrap_or(DEFAULT_BRANCH),
        remote_branch: args.value_of("remote_branch").unwrap_or(DEFAULT_BRANCH),
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref r) => {
            let meta = match Meta::load(r) {
                Ok(m) => m,
                Err(_) => Meta::new(),
            };
            let mut savable = false;
            let remote = {
                if let Some(remote_id) = args.remote_id {
                    savable = true;
                    remote::parse_remote(remote_id, args.port, None)
                } else {
                    match meta.pull {
                        Some(Repository::SSH { ref address, ref port }) => {
                            remote::parse_remote(address, Some(*port), Some(r))
                        }
                        Some(Repository::String(ref host)) => {
                            remote::parse_remote(host, None, Some(r))
                        }
                        None => return Err(Error::MissingRemoteRepository),
                    }
                }
            };
            debug!("remote: {:?}", remote);
            let mut session = try!(remote.session());
            let pushable =
                try!(session.pushable_patches(args.local_branch, args.remote_branch, r));
            let pushable = if !args.yes_to_all {
                let mut patches = Vec::new();
                // let patch_dir = patches_dir(r);
                let mut pushable:Vec<_> = pushable.into_iter().collect();
                pushable.sort_by(|&(_, a), &(_, b)| a.cmp(&b));
                for &(ref i, _) in pushable.iter() {
                    patches.push((i.clone(), read_patch(r, i.as_ref())?))
                }
                try!(ask::ask_patches(ask::Command::Push, &patches))
            } else {
                pushable.into_iter().map(|(h, _)| h).collect()
            };

            try!(session.push(r, args.remote_branch, &pushable));
            if args.set_default && savable {
                let mut meta = match Meta::load(r) {
                    Ok(m) => m,
                    Err(_) => Meta::new(),
                };
                if let Some(remote_id) = args.remote_id {
                    if let Some(p) = args.port {
                        meta.push = Some(Repository::SSH {
                            address: remote_id.to_string(),
                            port: p,
                        });
                    } else {
                        meta.push = Some(Repository::String(remote_id.to_string()))
                    }
                }
                try!(meta.save(r));
            }
            Ok(())
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
