use clap::{SubCommand, ArgMatches, Arg};
use chrono;
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, InodeUpdate};
use libpijul::patch::{Record, Patch};
use libpijul::fs_representation::pristine_dir;
use std::path::Path;
use rand;
use error;
use super::BasicOptions;
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

pub fn run(args: &ArgMatches) -> Result<(), error::Error> {
    let opts = BasicOptions::from_args(args)?;
    let yes_to_all = args.is_present("all");
    let branch_name = opts.branch();

    // Generate the pending patch.
    let (pending, pending_syncs):(_,Vec<_>) =
        if !yes_to_all {
            let repo = opts.open_and_grow_repo(409600)?;
            let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
            let (changes, syncs):(Vec<Record>, _) = {
                let (changes, syncs) = txn.record(&branch_name, &opts.repo_root)?;
                let c = try!(ask_changes(&txn, &changes, ChangesDirection::Revert));
                let selected = changes.into_iter()
                    .enumerate()
                    .filter(|&(i, _)| *(c.get(&i).unwrap_or(&false)))
                    .map(|(_, x)| x)
                    .collect();
                (selected, syncs)
            };
            let branch = txn.get_branch(&branch_name).unwrap();
            let changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
            let patch = txn.new_patch(&branch, Vec::new(), String::new(), None, chrono::UTC::now(), changes);
            txn.commit()?;
            (patch, syncs)
        } else {
            (Patch::empty(), Vec::new())
        };

    let mut size_increase = None;
    let pristine = pristine_dir(&opts.repo_root);
    loop {
        match output_repository(&opts.repo_root, &pristine, &branch_name, size_increase, &pending, &pending_syncs) {
            Err(ref e) if e.lacks_space() => {
                size_increase = Some(Repository::repository_size(&pristine).unwrap())
            },
            e => return e
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
