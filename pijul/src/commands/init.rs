use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;
use error::Error;
use commands::StaticSubcommand;
use std::io::{Write,stderr};
use std::process::exit;
use rand;
use libpijul::Repository;
use libpijul::fs_representation::{find_repo_root, pristine_dir, create};
use std::env::current_dir;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("init")
        .about("Create a new repository")
        .arg(Arg::with_name("directory")
            .index(1)
            .help("Where to create the repository, defaults to the current directory.")
            .required(false));
}

pub struct Params<'a> {
    pub location: Option<&'a Path>,
    pub allow_nested: bool,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        location: args.value_of("directory").map(|x| Path::new(x)),
        allow_nested: false,
    }
}

pub fn run(p: &Params) -> Result<(), Error> {
    // Since the location may not exist, we can't always canonicalize,
    // which doesn't really matter since we're going to explore the
    // whole path in `find_repo_root`.
    let wd = match p.location {
        Some(r) if r.is_relative() => current_dir()?.join(r),
        Some(r) => r.to_path_buf(),
        None => current_dir()?
    };
    match find_repo_root(&wd) {
        Some(_) if p.allow_nested => create(&wd, rand::thread_rng())?,
        Some(r) => return Err(Error::InARepository(r)),
        None => create(&wd, rand::thread_rng())?,
    }
    let repo_dir = pristine_dir(wd);
    let repo = Repository::open(&repo_dir, None)?;
    let txn = repo.mut_txn_begin(rand::thread_rng())?;
    txn.commit().map_err(Error::Repository)
}

pub fn explain(r: Result<(), Error>) {
    match r {
        Ok(_) => (),
        Err(Error::InARepository(p)) => {
            write!(stderr(), "Repository {} already exists", p.display()).unwrap();
            exit(1)
        },
        Err(e) => {
            write!(stderr(), "error: {}", e).unwrap();
            exit(1)
        }
    }
}
