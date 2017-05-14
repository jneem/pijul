use clap::{SubCommand, ArgMatches, Arg};

use super::{StaticSubcommand, set_current_branch, get_current_branch, get_wd, default_explain};
use rand;
use error::Error;
use std::path::Path;

use libpijul::{Repository, Patch};
use libpijul::fs_representation::{pristine_dir, find_repo_root};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("checkout")
        .about("Change the current branch")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Local repository.")
             .takes_value(true)
        )
        .arg(Arg::with_name("branch")
             .help("Branch to switch to.")
             .takes_value(true)
        )
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch")
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    debug!("args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref repo_root) => {

            if let Some(branch) = args.branch {
                let pristine_dir = pristine_dir(repo_root);

                let repo = try!(Repository::open(&pristine_dir, None));
                let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));

                if txn.get_branch(branch).is_some() {

                    txn.output_repository(&branch, &repo_root, &Patch::empty(), &Vec::new())?;
                    txn.commit()?;

                    set_current_branch(repo_root, branch)?;

                    Ok(())

                } else {
                    Err(Error::NoSuchBranch)
                }
            } else {
                println!("Current branch: {:?}", get_current_branch(repo_root)?);
                Ok(())
            }
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
