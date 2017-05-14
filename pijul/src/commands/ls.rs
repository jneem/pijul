use commands::{StaticSubcommand, get_wd, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;
use libpijul::Repository;
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use std::path::Path;
use rand;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("ls")
        .about("list tracked files")
        .arg(Arg::with_name("dir")
            .multiple(true)
            .help("Prefix of the list"))
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Repository to list."));
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params { repository: args.value_of("repository").and_then(|x| Some(Path::new(x))) }
}

pub fn run(args: &Params) -> Result<(), error::Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => Err(error::Error::NotInARepository),
        Some(ref r) => {
            let repo_dir = pristine_dir(r);
            let repo = Repository::open(&repo_dir, None).map_err(error::Error::Repository)?;
            let txn = repo.mut_txn_begin(rand::thread_rng())?;
            let files = txn.list_files()?;
            for f in files {
                println!("{}", f.display())
            }
            Ok(())
        }
    }
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
