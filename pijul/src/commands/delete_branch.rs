use clap::{SubCommand, ArgMatches, Arg};

use super::{StaticSubcommand, get_current_branch, get_wd, default_explain};
use error::Error;
use std::path::Path;

use libpijul::Repository;
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use rand;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("delete-branch")
        .about("Delete a branch in the local repository")
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Local repository.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .help("Branch.")
            .takes_value(true)
            .required(true));
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: &'a str,
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch").unwrap(),
    }
}

pub fn run<'a>(args: &Params<'a>) -> Result<(), Error> {
    debug!("args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref repo_root) => {

            let pristine_dir = pristine_dir(repo_root);
            let current_branch = get_current_branch(repo_root)?;
            if current_branch == args.branch {
                return Err(Error::CannotDeleteCurrentBranch)
            }
            let repo = Repository::open(&pristine_dir, None).map_err(Error::Repository)?;
            let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
            let at_least_two_branches = {
                let mut it = txn.iter_branches(None);
                it.next();
                it.next().is_some()
            };
            if at_least_two_branches {
                if ! txn.drop_branch(&args.branch)? {
                    return Err(Error::NoSuchBranch)
                };
                txn.commit()?;
                Ok(())
            } else {
                if txn.get_branch(&args.branch).is_none() {
                    Err(Error::NoSuchBranch)
                } else {
                    Err(Error::CannotDeleteCurrentBranch)
                }
            }
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
