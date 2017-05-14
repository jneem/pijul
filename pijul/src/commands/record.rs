use clap::{SubCommand, ArgMatches, Arg};
use chrono;
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, DEFAULT_BRANCH, Hash, InodeUpdate, Patch};
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use std::mem::drop;
use error::Error;

use std::path::Path;
use meta::Meta;
use commands::{ask, get_wd};
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use super::get_current_branch;
use super::ask::{ChangesDirection, ask_changes};
use rand;
pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("record")
        .about("record changes in the repository")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .required(false)
             .takes_value(true))
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

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>,
    pub patch_name: Option<&'a str>,
    pub authors: Option<Vec<&'a str>>,
    pub yes_to_all: bool,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        branch: args.value_of("branch"),
        yes_to_all: args.is_present("all"),
        authors: args.values_of("author").map(|x| x.collect()),
        patch_name: args.value_of("message"),
    }
}

pub fn run(args: &Params) -> Result<Option<Hash>, Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref r) => {
            let pristine_dir = pristine_dir(r);
            let branch_name = if let Some(b) = args.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(r) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };
            let (changes, syncs) = {
                // Increase by 100 pages. The most things record can
                // write is one write in the branches table, affecting
                // at most O(log n) blocks.
                let repo = Repository::open(&pristine_dir, Some(409600))?;
                let mut txn = repo.mut_txn_begin(rand::thread_rng())?;
                let (changes, syncs) = txn.record(&branch_name, &r)?;
                if !args.yes_to_all {
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
                let repo = Repository::open(&pristine_dir, None).map_err(Error::Repository)?;
                let patch = {
                    let txn = repo.txn_begin()?;
                    let mut save_meta = false;
                    let mut meta = match Meta::load(r) {
                        Ok(m) => m,
                        Err(_) => {
                            save_meta = true;
                            Meta::new()
                        }
                    };
                    debug!("meta:{:?}", meta);
                    let authors: Vec<String> = if let Some(ref authors) = args.authors {
                        let authors: Vec<String> = authors.iter().map(|x| x.to_string()).collect();
                        {
                            if meta.default_authors.len() == 0 {
                                meta.default_authors = authors.clone();
                                save_meta = true
                            }
                        }
                        authors
                    } else {
                        if meta.default_authors.len() > 0 {
                            meta.default_authors.clone()
                        } else {
                            save_meta = true;
                            let authors = try!(ask::ask_authors());
                            meta.default_authors = authors.clone();
                            authors
                        }
                    };
                    debug!("authors:{:?}", authors);
                    let patch_name = if let Some(ref m) = args.patch_name {
                        m.to_string()
                    } else {
                        try!(ask::ask_patch_name())
                    };
                    debug!("patch_name:{:?}", patch_name);
                    if save_meta {
                        try!(meta.save(r))
                    }
                    debug!("new");
                    let changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
                    let branch = txn.get_branch(&branch_name).unwrap();
                    txn.new_patch(&branch, authors, patch_name, None, chrono::UTC::now(), changes)
                };
                drop(repo);


                let mut increase = 409600;
                loop {
                    match record_no_resize(&pristine_dir, r, &branch_name, &patch, &syncs, increase) {
                        Err(ref e) if e.lacks_space() => { increase *= 2 },
                        e => return e
                    }
                }
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
