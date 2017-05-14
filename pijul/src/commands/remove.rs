use commands::fs_operation;
use commands::fs_operation::Operation;
use commands::{StaticSubcommand, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("remove")
        .about("remove file from the repository")
        .arg(Arg::with_name("files")
            .multiple(true)
            .help("Files to remove from the repository.")
            .required(true))
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Repository to remove files from."));
}

pub type Params<'a> = fs_operation::Params<'a>;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    return fs_operation::parse_args(args);
}


pub fn run(args: &Params) -> Result<(), error::Error> {
    fs_operation::run(args, Operation::Remove)
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
