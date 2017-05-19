use commands::fs_operation;
use commands::fs_operation::Operation;
use commands::{StaticSubcommand, default_explain};
use error::Error;
use clap::{SubCommand, ArgMatches, Arg};


pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("add")
        .about("add a file to the repository")
        .arg(Arg::with_name("files")
            .multiple(true)
            .help("Files to add to the repository.")
            .required(true))
        .arg(Arg::with_name("repository")
            .takes_value(true)
            .long("repository")
            .help("Add the files to this repository. Defaults to the repository containing \
                   the current directory."));
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    fs_operation::run(args, Operation::Add)
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
