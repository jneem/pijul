extern crate bincode;
#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate chrono;
extern crate flate2;
extern crate libc;
#[macro_use]
extern crate log;
extern crate rand;
extern crate ring;
extern crate rustc_serialize;
extern crate sanakirja;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use std::path::Path;
use std::collections::{HashMap, HashSet};
use rustc_serialize::base64::{URL_SAFE, ToBase64};
use std::io::Write;

pub use sanakirja::Transaction;

pub mod error;
use self::error::*;

pub trait RepositoryEnv<'env, R>: Sized {
    fn open<P: AsRef<Path>>(&self, path: P) -> Result<Self, Error>;
    fn mut_txn_begin(&'env self) -> Result<R, Error>;
}


#[macro_use]
mod backend;

mod apply;
mod optimal_diff;
mod output;
mod record;
mod unrecord;
pub mod conflict;
pub mod file_operations;
pub mod fs_representation;
pub mod graph;
pub mod patch;

pub use backend::{
    DEFAULT_BRANCH, Repository, MutTxn, LineId, PatchId, FOLDER_EDGE, PARENT_EDGE, DELETED_EDGE,
    Hash, HashRef,
    Key, Edge,
    Txn, Branch, Inode,
    ROOT_INODE, ROOT_KEY,
    SmallString,
    ApplyTimestamp
};

pub use record::InodeUpdate;
pub use patch::Patch;
pub use sanakirja::value::Value;
use fs_representation::ID_LENGTH;
use std::io::Read;
use rand::Rng;

impl<'env, T: rand::Rng> backend::MutTxn<'env, T> {

    pub fn output_changes_file<P: AsRef<Path>>(&mut self, branch: &Branch, path: P) -> Result<(), Error> {
        let changes_file = fs_representation::branch_changes_file(path.as_ref(), branch.name.as_str());
        let mut branch_id:Vec<u8> = vec![b'\n'; ID_LENGTH + 1];
        {
            if let Ok(mut file) = std::fs::File::open(&changes_file) {
                file.read_exact(&mut branch_id)?;
            }
        }
        let mut branch_id =
            if let Ok(s) = String::from_utf8(branch_id) {
                s
            } else {
                "\n".to_string()
            };
        if branch_id.as_bytes()[0] == b'\n' {
            branch_id.truncate(0);
            let mut rng = rand::thread_rng();
            branch_id.extend(rng.gen_ascii_chars().take(ID_LENGTH));
            branch_id.push('\n');
        }

        let mut file = std::fs::File::create(&changes_file)?;
        file.write_all(&branch_id.as_bytes())?;
        for (s, patch_id) in self.iter_applied(&branch, None) {
            let hash_ext = self.get_external(&patch_id).unwrap();
            writeln!(file, "{}:{}", hash_ext.to_base64(URL_SAFE), s)?
        }
        Ok(())
    }

    pub fn branch_patches(&mut self, branch: &Branch) -> HashSet<(backend::Hash, ApplyTimestamp)> {
        self.iter_patches(branch, None)
            .map(|(patch_id, time)| (self.external_hash(&patch_id).to_owned(), time))
            .collect()
    }

    pub fn fork(&mut self, branch: &Branch, new_name:&str) -> Result<Branch, Error> {
        if branch.name.as_str() == new_name {
            Err(Error::BranchNameAlreadyExists)
        } else {
            Ok(Branch {
                db: self.txn.fork(&mut self.rng, &branch.db)?,
                patches: self.txn.fork(&mut self.rng, &branch.patches)?,
                revpatches: self.txn.fork(&mut self.rng, &branch.revpatches)?,
                name: SmallString::from_str(new_name),
                apply_counter: 0
            })
        }
    }
}

impl<'env, T: rand::Rng> backend::MutTxn<'env, T> {
    pub fn add_file<P: AsRef<Path>>(&mut self, path: P, is_dir: bool) -> Result<(), Error> {
        self.add_inode(None, path.as_ref(), is_dir)
    }

    /// Tells whether a `key` is alive in `branch`, i.e. is either the
    /// root, or has at least one alive edge pointing to it.
    fn is_alive(&self, branch: &Branch, key: &Key<PatchId>) -> bool {
        *key == ROOT_KEY ||
            self.has_edge(branch, &key, PARENT_EDGE, false) ||
            self.has_edge(branch, &key, PARENT_EDGE | FOLDER_EDGE, false)
    }
}

fn make_remote<'a, I:Iterator<Item = &'a Hash>>(target: &Path, remote: I) -> Result<(HashMap<Hash, Patch>, usize), Error> {
    use fs_representation::*;
    use std::io::BufReader;
    use std::fs::File;
    let mut patches = HashMap::new();
    let mut patches_dir = patches_dir(target).to_path_buf();;
    let mut size_increase = 0;

    for h in remote {

        patches_dir.push(&patch_file_name(h.as_ref()));

        debug!("opening {:?}", patches_dir);
        let file = try!(File::open(&patches_dir));
        let mut file = BufReader::new(file);
        let (h, _, patch) = Patch::from_reader_compressed(&mut file)?;

        size_increase += patch.size_upper_bound();
        patches.insert(h.clone(), patch);

        patches_dir.pop();

    }
    Ok((patches, size_increase))
}

/// Apply a number of patches, guessing the new repository size.  If
/// this fails, the repository size is guaranteed to have been
/// increased by at least some pages, and it is safe to call this
/// function again.
///
/// Also, this function takes a file lock on the repository.
pub fn apply_resize<'a, I:Iterator<Item = &'a Hash>>(target: &Path, branch_name: &str, remote: I) -> Result<(), Error> {
    use fs_representation::*;
    let (patches, size_increase) = make_remote(target, remote)?;
    info!("applying patches with size_increase {:?}", size_increase);
    let pristine_dir = pristine_dir(target).to_path_buf();;
    let repo = try!(Repository::open(pristine_dir, Some(size_increase as u64)));
    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    try!(txn.apply_patches(branch_name, target, &patches));
    try!(txn.commit());
    Ok(())
}

/// Apply a number of patches, guessing the new repository size.  If
/// this fails, the repository size is guaranteed to have been
/// increased by at least some pages, and it is safe to call this
/// function again.
///
/// Also, this function takes a file lock on the repository.
pub fn apply_resize_no_output<'a, I:Iterator<Item = &'a Hash>>(target: &Path, branch_name: &str, remote: I) -> Result<(), Error> {
    use fs_representation::*;
    let (patches, size_increase) = make_remote(target, remote)?;
    let pristine_dir = pristine_dir(target).to_path_buf();;
    let repo = try!(Repository::open(pristine_dir, Some(size_increase as u64)));
    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    let mut branch = txn.open_branch(branch_name)?;
    let mut new_patches_count = 0;
    for (p, patch) in patches.iter() {
        debug!("apply_patches: {:?}", p);
        txn.apply_patches_rec(&mut branch, &patches,
                              p, patch, &mut new_patches_count)?
    }
    txn.commit_branch(branch)?;
    txn.commit()?;
    Ok(())
}

pub fn unrecord_no_resize(repo_dir: &Path, repo_root: &Path, branch_name: &str, selected: &mut Vec<(Hash, Patch)>, increase: u64) -> Result<(), Error> {
    let repo = try!(Repository::open(repo_dir, Some(increase)));

    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    let mut branch = txn.open_branch(branch_name)?;
    let mut timestamps = Vec::new();
    while let Some((hash, patch)) = selected.pop() {
        let internal = txn.get_internal(hash.as_ref()).unwrap().to_owned();
        debug!("Unrecording {:?}", hash);
        if let Some(ts) = txn.get_patch(&branch.patches, &internal) {
            timestamps.push(ts);
        }
        try!(txn.unrecord(&mut branch, &internal, &patch));
        debug!("Done unrecording {:?}", hash);
    }


    if let Err(e) = txn.output_changes_file(&branch, repo_root) {
        error!("no changes file: {:?}", e)
    }
    try!(txn.commit_branch(branch));
    try!(txn.commit());
    Ok(())
}
