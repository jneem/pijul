use clap::{SubCommand, ArgMatches, Arg};
use rand;

use super::{BasicOptions, StaticSubcommand, get_current_branch, default_explain};
use error::Error;


pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("delete-branch")
        .about("Delete a branch in the local repository")
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Local repository.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .help("Branch to delete.")
            .takes_value(true)
            .required(true));
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    debug!("args {:?}", args);
    let opts = BasicOptions::from_args(args)?;
    let branch = args.value_of("branch").unwrap();
    let current_branch = get_current_branch(&opts.repo_root)?;
    if current_branch == branch {
        return Err(Error::CannotDeleteCurrentBranch)
    }
    let repo = opts.open_repo()?;
    let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
    let at_least_two_branches = {
        let mut it = txn.iter_branches(None);
        it.next();
        it.next().is_some()
    };
    if at_least_two_branches {
        if !txn.drop_branch(&branch)? {
            return Err(Error::NoSuchBranch)
        };
        txn.commit()?;
        Ok(())
    } else {
        if txn.get_branch(&branch).is_none() {
            Err(Error::NoSuchBranch)
        } else {
            Err(Error::CannotDeleteCurrentBranch)
        }
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
