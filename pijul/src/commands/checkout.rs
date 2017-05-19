use clap::{SubCommand, ArgMatches, Arg};

use super::{BasicOptions, StaticSubcommand, set_current_branch, get_current_branch, default_explain};
use rand;
use error::Error;

use libpijul::Patch;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("checkout")
        .about("Change the current branch")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Path to a pijul repository. Defaults to the repository containing the \
                    current directory.")
             .takes_value(true)
        )
        .arg(Arg::with_name("branch")
             .help("Branch to switch to.")
             .takes_value(true)
        )
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    if let Some(branch) = args.value_of("branch") {
        let repo = opts.open_repo()?;
        let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));

        if txn.get_branch(branch).is_some() {

            txn.output_repository(&branch, &opts.repo_root, &Patch::empty(), &Vec::new())?;
            txn.commit()?;

            set_current_branch(&opts.repo_root, branch)?;

            Ok(())

        } else {
            Err(Error::NoSuchBranch)
        }
    } else {
        println!("Current branch: {:?}", get_current_branch(&opts.repo_root)?);
        Ok(())
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
