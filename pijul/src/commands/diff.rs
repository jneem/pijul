use clap::{SubCommand, ArgMatches, Arg};
use rand;

use commands::{BasicOptions, StaticSubcommand, default_explain};
use error::Error;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("diff")
        .about("show what would be recorded if record were called")
        .arg(Arg::with_name("repository")
             .takes_value(true)
             .long("repository")
             .help("The repository to show, defaults to the current directory.")
             .required(false))
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("The branch to show, defaults to the current branch.")
             .takes_value(true)
             .required(false))
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;

    // Increase by 100 pages. The most things record can
    // write is one write in the branches table, affecting
    // at most O(log n) blocks.
    let repo = opts.open_and_grow_repo(409600)?;
    let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
    let (changes, _) = txn.record(&opts.branch(), &opts.repo_root)?;
    try!(super::ask::print_status(&txn, &changes));
    Ok(())
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
