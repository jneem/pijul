use clap::{SubCommand, ArgMatches, Arg};
use rand;

use error::Error;
use super::{BasicOptions, StaticSubcommand, default_explain, set_current_branch};

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

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    let to = args.value_of("to").unwrap();
    let repo = opts.open_repo()?;
    let mut txn = repo.mut_txn_begin(rand::thread_rng())?;

    if !txn.has_branch(to) {
        let branch = txn.open_branch(&opts.branch())?;
        let new_branch = txn.fork(&branch, to)?;
        try!(txn.commit_branch(branch));
        try!(txn.commit_branch(new_branch));
        try!(txn.commit());
        set_current_branch(&opts.repo_root, to)?;
        Ok(())
    } else {
        Err(Error::BranchAlreadyExists)
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
