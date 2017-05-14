use clap::{SubCommand, ArgMatches, Arg};
use chrono;
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, DEFAULT_BRANCH, InodeUpdate};
use libpijul::patch::{Record, Patch};
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use std::path::Path;
use rand;
use error;
use commands::get_wd;
use super::get_current_branch;
use super::ask::{ChangesDirection, ask_changes};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("revert")
        .about("Rewrite the working copy from the pristine")
        .arg(Arg::with_name("repository")
             .long("repository")
             .takes_value(true)
             .help("Local repository."))
        .arg(Arg::with_name("all")
             .short("a")
             .long("all")
             .help("Answer 'y' to all questions")
             .takes_value(false))
        .arg(Arg::with_name("branch")
             .help("Branch to revert to.")
             .long("branch")
             .takes_value(true)
        );
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>,
    pub yes_to_all: bool,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(Path::new),
        branch: args.value_of("branch"),
        yes_to_all: args.is_present("all"),
    }
}

pub fn run(args: &Params) -> Result<(), error::Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(error::Error::NotInARepository),
        Some(ref r) => {
            let pristine_dir = pristine_dir(r);
            let branch = if let Some(b) = args.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(r) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };


            // Generate the pending patch.
            let (pending, pending_syncs):(_,Vec<_>) =
                if !args.yes_to_all {
                    let repo = Repository::open(&pristine_dir, Some(409600))?;
                    let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
                    let (changes, syncs):(Vec<Record>, _) = {
                        let (changes, syncs) = txn.record(&branch, &r)?;
                        let c = try!(ask_changes(&txn, &changes, ChangesDirection::Revert));
                        let selected = changes.into_iter()
                            .enumerate()
                            .filter(|&(i, _)| *(c.get(&i).unwrap_or(&false)))
                            .map(|(_, x)| x)
                            .collect();
                        (selected, syncs)
                    };
                    let branch = txn.get_branch(&branch).unwrap();
                    let changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
                    let patch = txn.new_patch(&branch, Vec::new(), String::new(), None, chrono::UTC::now(), changes);
                    txn.commit()?;
                    (patch, syncs)
                } else {
                    (Patch::empty(), Vec::new())
                };

            let mut size_increase = None;
            loop {
                match output_repository(r, &pristine_dir, &branch, size_increase, &pending, &pending_syncs) {
                    Err(ref e) if e.lacks_space() => {
                        size_increase = Some(Repository::repository_size(&pristine_dir).unwrap())
                    },
                    e => return e
                }
            }
        }
    }
}

fn output_repository(r: &Path, pristine_dir: &Path, branch: &str, size_increase: Option<u64>, pending: &Patch, pending_syncs: &[InodeUpdate]) -> Result<(), error::Error> {
    let repo = try!(Repository::open(&pristine_dir, size_increase));
    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    try!(txn.output_repository(&branch, &r, pending, pending_syncs));
    txn.commit()?;
    Ok(())
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
