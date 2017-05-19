use clap::{SubCommand, ArgMatches, Arg};
use chrono;
use commands::{BasicOptions, StaticSubcommand, default_explain};
use libpijul::{Repository, Hash, InodeUpdate, Patch};
use libpijul::fs_representation::pristine_dir;
use std::mem::drop;
use error::Error;

use std::path::Path;
use meta::{GlobalMeta, Meta};
use commands::ask;
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use super::ask::{ChangesDirection, ask_changes};
use rand;
pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("record")
        .about("record changes in the repository")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("The branch where to record, defaults to the current branch.")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("all")
             .short("a")
             .long("all")
             .help("Answer 'y' to all questions")
             .takes_value(false))
        .arg(Arg::with_name("message")
             .short("m")
             .long("name")
             .help("The name of the patch to record")
             .takes_value(true))
        .arg(Arg::with_name("author")
             .short("A")
             .long("author")
             .help("Author of this patch (multiple occurrences allowed)")
             .multiple(true)
             .takes_value(true));
}

pub fn run(args: &ArgMatches) -> Result<Option<Hash>, Error> {
    let opts = BasicOptions::from_args(args)?;
    let yes_to_all = args.is_present("all");
    let patch_name_arg = args.value_of("message");
    let authors_arg = args.values_of("author").map(|x| x.collect::<Vec<_>>());
    let branch_name = opts.branch();

    let (changes, syncs) = {
        // Increase by 100 pages. The most things record can
        // write is one write in the branches table, affecting
        // at most O(log n) blocks.
        let repo = opts.open_and_grow_repo(409600)?;
        let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
        let (changes, syncs) = txn.record(&branch_name, &opts.repo_root)?;
        if !yes_to_all {
            let c = try!(ask_changes(&txn, &changes, ChangesDirection::Record));
            let selected = changes.into_iter()
                .enumerate()
                .filter(|&(i, _)| *(c.get(&i).unwrap_or(&false)))
                .map(|(_, x)| x)
                .collect();
            txn.commit()?;
            (selected, syncs)
        } else {
            txn.commit()?;
            (changes, syncs)
        }
    };
    if changes.is_empty() {
        println!("Nothing to record");
        Ok(None)
    } else {
        // println!("patch: {:?}",changes);
        let repo = opts.open_repo()?;
        let patch = {
            let txn = repo.txn_begin()?;
            let meta = Meta::load(&opts.repo_root);
            debug!("meta:{:?}", meta);

            let authors: Vec<String> = if let Some(ref authors) = authors_arg {
                authors.iter().map(|x| x.to_string()).collect()
            } else if meta.default_authors.len() > 0 {
                    meta.default_authors.clone()
            } else {
                ask::ask_authors()?
            };
            debug!("authors:{:?}", authors);

            let patch_name = if let Some(ref m) = patch_name_arg {
                m.to_string()
            } else {
                try!(ask::ask_patch_name())
            };
            debug!("patch_name:{:?}", patch_name);

            if meta.default_authors.is_empty() {
                println!("From now on, the author you just entered will be used by default.");
                println!("To change the default value, edit one of pijul's configuration files.");
                if let Err(global_err) = GlobalMeta::save_default_authors(&authors) {
                    println!(
                        "Warning: failed to save default authors in system-wide configuration: {}",
                        global_err);
                    if let Err(local_err) = Meta::save_default_authors(&opts.repo_root, &authors) {
                        println!(
                            "Warning: failed to save default authors in repo-wide configuration: {}",
                            local_err);
                    }
                }
                Meta::print_meta_info(&opts.repo_root);
            }

            debug!("new");
            let changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
            let branch = txn.get_branch(&branch_name).unwrap();
            txn.new_patch(&branch, authors, patch_name, None, chrono::UTC::now(), changes)
        };
        drop(repo);

        let mut increase = 409600;
        let pristine = pristine_dir(&opts.repo_root);
        loop {
            match record_no_resize(&pristine, &opts.repo_root, &branch_name, &patch, &syncs, increase) {
                Err(ref e) if e.lacks_space() => { increase *= 2 },
                e => return e
            }
        }
    }
}

fn record_no_resize(pristine_dir: &Path, r: &Path, branch_name: &str, patch: &Patch, syncs: &[InodeUpdate], increase: u64) -> Result<Option<Hash>, Error> {

    let size_increase = increase + patch.size_upper_bound() as u64;
    let repo = try!(Repository::open(&pristine_dir, Some(size_increase)).map_err(Error::Repository));
    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    // save patch
    debug!("syncs: {:?}", syncs);
    let (hash, _) = txn.apply_local_patch(&branch_name, r, &patch, &syncs, false)?;
    txn.commit()?;
    println!("Recorded patch {}", hash.to_base64(URL_SAFE));
    Ok(Some(hash))
}

pub fn explain(res: Result<Option<Hash>, Error>) {
    default_explain(res)
}
