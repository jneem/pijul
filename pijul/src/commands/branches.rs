use clap::{SubCommand, ArgMatches, Arg};

use super::{BasicOptions, StaticSubcommand, get_current_branch, default_explain};
use error::Error;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("branches")
        .about("List all branches")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Path to a pijul repository. Defaults to the repository containing the \
                    current directory.")
             .takes_value(true)
        )
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    let repo = opts.open_repo()?;
    let txn = repo.txn_begin()?;
    let current_branch = get_current_branch(&opts.repo_root)?;
    for branch in txn.iter_branches(None) {
        if branch.name.as_str() == current_branch {
            println!("* {}", branch.name.as_str())
        } else {
            println!("  {}", branch.name.as_str())
        }
    }
    Ok(())
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
