use clap::{SubCommand, ArgMatches, Arg};
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, DEFAULT_BRANCH};
use libpijul::fs_representation::{pristine_dir, find_repo_root, read_patch_nochanges, id_file};
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str;

use super::{ask, get_current_branch, get_wd};

pub fn invocation() -> StaticSubcommand {
    SubCommand::with_name("changes")
        .about("List the patches applied to the given branch")
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Path to the repository to list.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .long("branch")
            .help("The branch to list.")
             .takes_value(true))
        .arg(Arg::with_name("hash-only")
            .long("hash-only")
            .help("Only display the hash of each path."))
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>,
    pub hash_only: bool,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch"),
        hash_only: args.is_present("hash-only"),
    }
}

pub fn run(params: &mut Params) -> Result<(), Error> {
    let wd = try!(get_wd(params.repository));
    match find_repo_root(&wd) {
        None => Err(Error::NotInARepository),
        Some(ref target) => {

            if params.hash_only {
                // If in binary form, start with this repository's id.
                let id_file = id_file(target);
                let mut f = File::open(&id_file)?;
                let mut s = String::new();
                f.read_to_string(&mut s)?;
                println!("{}", s.trim());
            }

            let repo_dir = pristine_dir(target);
            let repo = try!(Repository::open(&repo_dir, None));
            let txn = try!(repo.txn_begin());
            let branch_name = if let Some(b) = params.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(target) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };

            if let Some(branch) = txn.get_branch(&branch_name) {
                if params.hash_only {
                    for (s, hash) in txn.iter_applied(&branch, None) {
                        let hash_ext = txn.get_external(&hash).unwrap();
                        println!("{}:{}", hash_ext.to_base64(URL_SAFE), s)
                    }
                } else {
                    for (_, hash) in txn.rev_iter_applied(&branch, None) {
                        let hash_ext = txn.get_external(&hash).unwrap();
                        let patch = read_patch_nochanges(&target, hash_ext)?;
                        ask::print_patch_descr(&hash_ext.to_owned(), &patch)
                    }
                }
            }
            Ok(())
        }
    }
}

pub fn explain(r: Result<(), Error>) {
    default_explain(r)
}
