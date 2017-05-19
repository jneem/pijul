use commands::{BasicOptions, StaticSubcommand, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;
use rand;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("ls")
        .about("list tracked files")
        .arg(Arg::with_name("dir")
            .multiple(true)
            .help("Prefix of the list"))
        .arg(Arg::with_name("repository")
            .takes_value(true)
            .long("repository")
            .help("Repository to list."));
}

pub fn run(args: &ArgMatches) -> Result<(), error::Error> {
    let opts = BasicOptions::from_args(args)?;
    let repo = opts.open_repo()?;
    let txn = repo.mut_txn_begin(rand::thread_rng())?;
    let files = txn.list_files()?;
    for f in files {
        println!("{}", f.display())
    }
    Ok(())
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
