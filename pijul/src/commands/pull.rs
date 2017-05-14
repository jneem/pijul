use clap::{SubCommand, ArgMatches, Arg};

use commands::{StaticSubcommand, default_explain};
use error::Error;
use std::path::Path;
use std::fs::File;

use libpijul::fs_representation::find_repo_root;
use libpijul::patch::Patch;
use libpijul::{Hash, DEFAULT_BRANCH, ApplyTimestamp};
use commands::remote;
use commands::ask::{ask_patches, Command};
use commands::get_wd;
use std::io::BufReader;

use super::super::meta::{Meta, Repository};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("pull")
        .about("pull from a remote repository")
        .arg(Arg::with_name("remote").help("Repository from which to pull."))
        .arg(Arg::with_name("repository").help("Local repository."))
        .arg(Arg::with_name("remote_branch")
            .long("from-branch")
            .help("The branch to pull from")
            .takes_value(true))
        .arg(Arg::with_name("local_branch")
            .long("to-branch")
            .help("The branch to pull into")
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
    let repository = args.value_of("repository").map(Path::new);
    let remote_id = args.value_of("remote");
    // let remote=remote::parse_remote(&remote_id,args);
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

fn get_remote<'a>(args: &Params<'a>,
                  meta: &'a Meta,
                  repo_root: &'a Path)
                  -> Result<(bool, remote::Remote<'a>), Error> {
    match args.remote_id {
        Some(remote_id) => Ok((true, remote::parse_remote(remote_id, args.port, None))),
        None => {
            match meta.pull {
                Some(Repository::SSH { ref address, ref port }) => {
                    Ok((false, remote::parse_remote(address, Some(*port), Some(repo_root))))
                }
                Some(Repository::String(ref host)) => {
                    Ok((false, remote::parse_remote(host, None, Some(repo_root))))
                }
                None => Err(Error::MissingRemoteRepository),
            }
        }
    }
}

fn fetch_pullable_patches(session: &mut remote::Session,
                          pullable: &[(Hash, ApplyTimestamp)],
                          r: &Path)
                          -> Result<Vec<(Hash, Patch)>, Error> {
    let mut patches = Vec::new();
    for &(ref i, _) in pullable {
        let (hash, _, patch) = {
            let filename = try!(session.download_patch(r, i));
            debug!("filename {:?}", filename);
            let file = try!(File::open(&filename));
            let mut file = BufReader::new(file);
            Patch::from_reader_compressed(&mut file)?
        };
        assert_eq!(&hash, i);
        patches.push((hash, patch));
    }
    Ok(patches)
}

pub fn select_patches(interactive: bool,
                      session: &mut remote::Session,
                      remote_branch: &str,
                      local_branch: &str,
                      r: &Path)
                      -> Result<Vec<(Hash, ApplyTimestamp)>, Error> {
    let pullable = try!(session.pullable_patches(remote_branch, local_branch, r));
    let mut pullable:Vec<_> = pullable.iter().collect();
    pullable.sort_by(|&(_, a), &(_, b)| a.cmp(&b));
    if interactive {
        let selected = {
            let patches = try!(fetch_pullable_patches(session, &pullable, r));
            try!(ask_patches(Command::Pull, &patches[..]))
        };
        Ok(pullable.into_iter()
           .filter(|&(ref h, _)| selected.contains(h))
           .collect())
    } else {
        Ok(pullable)
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    debug!("pull args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref r) => {
            let meta = Meta::load(r);
            let (savable, remote) = try!(get_remote(&args, &meta, r));
            let mut session = try!(remote.session());
            let pullable = try!(select_patches(!args.yes_to_all,
                                               &mut session,
                                               args.remote_branch,
                                               args.local_branch,
                                               r));

            // Pulling and applying
            info!("Pulling patch {:?}", pullable);
            try!(session.pull(r, args.local_branch, &pullable));
            info!("Saving meta");
            if args.set_default && savable {
                if let Some(remote_id) = args.remote_id {
                    let pull = if let Some(p) = args.port {
                        Repository::SSH { address: remote_id.to_string(), port: p }
                    } else {
                        Repository::String(remote_id.to_string())
                    };
                    Meta::save_pull(r, pull)?;
                }
            }
            Ok(())
        }
    }
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
