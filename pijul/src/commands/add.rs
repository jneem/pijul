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
            .long("repository")
            .help("Repository where to add files.")
            .takes_value(true));
}

pub type Params<'a> = fs_operation::Params<'a>;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    return fs_operation::parse_args(args);
}


pub fn run(args: &Params) -> Result<(), Error> {
    fs_operation::run(args, Operation::Add)
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
