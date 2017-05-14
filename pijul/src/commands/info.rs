use std::path::Path;
use clap::{SubCommand, Arg, ArgMatches};

use commands;
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use libpijul::{Repository, Inode};
use error::Error;
use commands::get_wd;
use std::fs::File;
use super::{get_current_branch};

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub debug: bool,
    pub inode: Option<Inode>
}

pub fn invocation() -> commands::StaticSubcommand {
    return SubCommand::with_name("info")
        .about("Get information about the current repository, if any")
        .arg(Arg::with_name("dir")
             .help("Pijul info will be given about this directory.")
             .required(false))
        .arg(Arg::with_name("debug")
             .long("--debug")
             .help("Pijul info will be given about this directory.")
             .required(false))
        .arg(Arg::with_name("inode")
             .long("--from-inode")
             .help("Inode to start the graph from.")
             .takes_value(true)
             .required(false));
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("dir").map(|x| Path::new(x)),
        debug: args.is_present("debug"),
        inode: args.value_of("inode").and_then(|x| Inode::from_hex(x).ok())
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        Some(ref r) => {
            println!("Current repository root: {:?}", r);
            println!("Current branch: {:?}", get_current_branch(r)?);
            if args.debug {
                let pristine_dir = pristine_dir(r);
                let repo = try!(Repository::open(&pristine_dir, None).map_err(Error::Repository));
                let txn = repo.txn_begin()?;
                txn.dump("dump");
                if let Some(ref inode) = args.inode {
                    // Output just the graph under `inode`.
                    let node = &txn.get_inodes(inode).unwrap().key;
                    for branch in txn.iter_branches(None) {
                        let ret = txn.retrieve(&branch, node);
                        let mut f = try!(File::create(format!("debug_{}", branch.name.as_str())));
                        ret.debug(&mut f)?
                    }
                } else {
                    // Output everything.
                    for branch in txn.iter_branches(None) {
                        let mut f = try!(File::create(format!("debug_{}", branch.name.as_str())));
                        txn.debug(branch.name.as_str(), &mut f);
                    }
                }
            }
            Ok(())
        }
        None => Err(Error::NotInARepository),
    }
}

pub fn explain(r: Result<(), Error>) {
    commands::default_explain(r)
}
