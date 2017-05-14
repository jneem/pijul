use clap::{SubCommand, ArgMatches, Arg};
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, DEFAULT_BRANCH};
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use error::Error;

use std::path::Path;
use commands::get_wd;
use super::get_current_branch;
use rand;
pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("diff")
        .about("show what would be recorded if record were called")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository to show, defaults to the current directory.")
             .required(false))
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("The branch to show, defaults to the current branch.")
             .takes_value(true)
             .required(false))
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch"),
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => Err(Error::NotInARepository),
        Some(ref r) => {
            let pristine_dir = pristine_dir(r);
            let branch_name = if let Some(b) = args.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(r) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };

            // Increase by 100 pages. The most things record can
            // write is one write in the branches table, affecting
            // at most O(log n) blocks.
            let repo = Repository::open(&pristine_dir, Some(409600))?;
            let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
            let (changes, _) = txn.record(&branch_name, &r)?;
            try!(super::ask::print_status(&txn, &changes));
            Ok(())
        }
    }
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
