use clap::{SubCommand, ArgMatches, Arg};

use super::{StaticSubcommand, set_current_branch, get_wd, default_explain};
use error::Error;
use std::path::Path;

use libpijul::{Repository, DEFAULT_BRANCH};
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use rand;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("fork")
        .about("Create a new branch")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Local repository.")
             .takes_value(true)
        )
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("Branch.")
             .takes_value(true)
        )
        .arg(Arg::with_name("to")
             .help("Name of the new branch.")
             .takes_value(true)
             .required(true)
        )
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: &'a str,
    pub to: &'a str,
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch").unwrap_or(DEFAULT_BRANCH),
        to: args.value_of("to").unwrap()
    }
}

pub fn run<'a>(args: &Params<'a>) -> Result<(), Error> {
    debug!("args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref repo_root) => {

            let pristine_dir = pristine_dir(repo_root);
            let repo = try!(Repository::open(&pristine_dir, None).map_err(Error::Repository));
            let mut txn = repo.mut_txn_begin(rand::thread_rng())?;

            if !txn.has_branch(args.to) {
                let branch = txn.open_branch(&args.branch)?;
                let new_branch = txn.fork(&branch, args.to)?;
                try!(txn.commit_branch(branch));
                try!(txn.commit_branch(new_branch));
                try!(txn.commit());
                set_current_branch(&repo_root, args.to)?;
                Ok(())
            } else {
                Err(Error::BranchAlreadyExists)
            }
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
