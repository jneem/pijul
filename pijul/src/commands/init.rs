use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;
use error::Error;
use commands::{StaticSubcommand, create_repo, default_explain};
use std::process::exit;
use libpijul::fs_representation::find_repo_root;
use std::env::current_dir;
use std::io::{Write,stderr};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("init")
        .about("Create a new repository")
        .arg(Arg::with_name("directory")
            .index(1)
            .help("Where to create the repository, defaults to the current repository.")
            .required(false));
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    // TODO: make this configurable.
    let allow_nested = false;
    // Since the location may not exist, we can't always canonicalize,
    // which doesn't really matter since we're going to explore the
    // whole path in `find_repo_root`.
    let wd = match args.value_of("directory").map(Path::new) {
        Some(r) if r.is_relative() => current_dir()?.join(r),
        Some(r) => r.to_path_buf(),
        None => current_dir()?
    };
    match find_repo_root(&wd) {
        Some(_) if allow_nested => create_repo(&wd),
        Some(r) => Err(Error::InARepository(r)),
        None => create_repo(&wd),
    }
}

pub fn explain(r: Result<(), Error>) {
    match r {
        Err(Error::InARepository(p)) => {
            writeln!(stderr(), "Repository {} already exists", p.display()).unwrap();
            exit(1)
        },
        _ => {
            default_explain(r)
        }
    }
}
