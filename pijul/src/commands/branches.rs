use clap::{SubCommand, ArgMatches, Arg};

use super::{StaticSubcommand, get_wd, get_current_branch, default_explain};
use error::Error;
use std::path::Path;

use libpijul::{Repository};
use libpijul::fs_representation::{pristine_dir, find_repo_root};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("branches")
        .about("List all branches")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Local repository.")
             .takes_value(true)
        )
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    debug!("args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref repo_root) => {

            let repo_dir = pristine_dir(repo_root);
            let repo = try!(Repository::open(&repo_dir, None).map_err(Error::Repository));
            let txn = repo.txn_begin()?;
            let current_branch = get_current_branch(repo_root)?;
            for branch in txn.iter_branches(None) {
                if branch.name.as_str() == current_branch {
                    println!("* {}", branch.name.as_str())
                } else {
                    println!("  {}", branch.name.as_str())
                }
            }
            Ok(())
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
