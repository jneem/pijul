use clap::{SubCommand, ArgMatches, Arg};

use super::{StaticSubcommand, default_explain};
use error::Error;
use std::path::Path;
use std::collections::HashSet;

use libpijul::{Repository, DEFAULT_BRANCH, Hash, HashRef, unrecord_no_resize};
use libpijul::patch::{Patch};
use libpijul::fs_representation::{pristine_dir, find_repo_root, patches_dir, patch_file_name};
use rand;
use super::{get_wd, ask, get_current_branch};
use std::collections::{HashMap};
use std::fs::File;
use std::io::{BufReader};
use std::mem::drop;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("unrecord")
        .about("Unrecord some patches (remove them without reverting them)")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Local repository.")
             .takes_value(true)
        )
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("Branch.")
             .takes_value(true)
        )
        .arg(Arg::with_name("patch")
             .long("patch")
             .help("Patch to unrecord.")
             .takes_value(true)
             .multiple(true)
        )
        .arg(Arg::with_name("all")
             .short("a")
             .long("all")
             .help("Answer 'y' to all questions")
             .takes_value(false)
        );
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub branch: Option<&'a str>,
    pub yes_to_all: bool,
    pub patches: Option<HashSet<Hash>>
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(Path::new),
        branch: args.value_of("branch"),
        yes_to_all: args.is_present("all"),
        patches: if let Some(patches) = args.values_of("patch") {
            Some(patches.map(|x| Hash::from_base64(x).unwrap()).collect())
        } else {
            None
        }
    }
}

pub fn run(args: &Params) -> Result<(), Error> {
    debug!("args {:?}", args);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref repo_root) => {

            let repo_dir = pristine_dir(repo_root);
            let mut increase = 409600;
            let repo = try!(Repository::open(&repo_dir, Some(increase)).map_err(Error::Repository));
            let branch_name = if let Some(b) = args.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(repo_root) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };

            let mut patches:HashMap<_, _> = if let Some(ref patches) = args.patches {
                let txn = try!(repo.txn_begin());
                if let Some(branch) = txn.get_branch(&branch_name) {
                    let mut patches_ = HashMap::new();
                    for h in patches.iter() {

                        let patch = load_patch(repo_root, h.as_ref());
                        patches_.insert(h.to_owned(), patch);

                        if let Some(internal) = txn.get_internal(h.as_ref()) {
                            for (_, revdep) in txn.iter_revdep(Some((&internal, None))).take_while(|&(q, _)| q == internal) {
                                // If the branch has patch revdep.
                                if txn.get_patch(&branch.patches, &revdep).is_some() {
                                    let ext = txn.get_external(&revdep).unwrap();
                                    let patch = load_patch(repo_root, ext);
                                    patches_.insert(ext.to_owned(), patch);
                                }
                            }
                        }
                    }
                    patches_
                } else {
                    HashMap::new()
                }
            } else {
                let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
                let branch = txn.open_branch(&branch_name)?;
                let mut patches:Vec<_> = txn.rev_iter_applied(&branch, None)
                    .map(|(t, h)| {

                        let ext = txn.get_external(&h).unwrap();
                        let patch = load_patch(repo_root, ext);
                        (ext.to_owned(), patch, t)

                    })
                    .collect();
                txn.commit_branch(branch)?;
                txn.commit()?;
                patches.sort_by(|&(_, _, a), &(_, _, b)| b.cmp(&a));
                let patches:Vec<(Hash, Patch)> = patches.into_iter().map(|(a, b, _)| (a, b)).collect();
                // debug!("patches: {:?}", patches);
                let to_unrecord = ask::ask_patches(ask::Command::Unrecord, &patches).unwrap();
                debug!("to_unrecord: {:?}", to_unrecord);
                let patches: HashMap<_,_> =
                    patches
                    .into_iter()
                    .filter(|&(ref k,_)| to_unrecord.contains(&k))
                    .collect();
                patches
            };

            let mut selected = Vec::new();
            loop {
                let hash = if let Some((hash, patch)) = patches.iter().next() {
                    increase += patch.size_upper_bound() as u64;
                    hash.to_owned()
                } else {
                    break
                };
                deps_dfs(&mut selected, &mut patches, &hash)
            }
            drop(repo);

            loop {
                match unrecord_no_resize(&repo_dir, &repo_root, &branch_name, &mut selected, increase) {
                    Err(ref e) if e.lacks_space() => { increase *= 2 },
                    e => return e.map_err(Error::Repository)
                }
            }
        }
    }
}

fn load_patch(repo_root: &Path, ext: HashRef) -> Patch {
    let base = patch_file_name(ext);
    let filename = patches_dir(repo_root).join(&base);
    debug!("filename: {:?}", filename);
    let file = File::open(&filename).unwrap();
    let mut file = BufReader::new(file);
    let (_, _, patch) = Patch::from_reader_compressed(&mut file).unwrap();
    patch
}

fn deps_dfs(selected: &mut Vec<(Hash, Patch)>,
            patches: &mut HashMap<Hash, Patch>,
            current: &Hash) {

    if let Some(patch) = patches.remove(current) {

        for dep in patch.dependencies.iter() {
            deps_dfs(selected, patches, dep)
        }

        selected.push((current.to_owned(), patch))
    }
}


pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
