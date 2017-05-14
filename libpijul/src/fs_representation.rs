//! Layout of a repository (files in `.pijul`) on the disk. This
//! module exports both high-level functions that require no knowledge
//! of the repository, and lower-level constants documented on
//! [pijul.org/documentation/repository](https://pijul.org/documentation/repository),
//! used for instance for downloading files from remote repositories.

use std::path::{Path, PathBuf};
use std::fs::{metadata, create_dir_all, File};
use std::io::{Write, BufReader};
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use std;
use backend::HashRef;
use patch::{Patch, PatchHeader};
use error::Error;
use flate2;
use rand::Rng;

pub const PIJUL_DIR_NAME: &'static str = ".pijul";

pub fn repo_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    p.as_ref().join(PIJUL_DIR_NAME)
}

pub fn pristine_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join("pristine");
}

pub const PATCHES_DIR_NAME: &'static str = "patches";

pub fn patches_dir<P: AsRef<Path>>(p: P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join(PATCHES_DIR_NAME);
}

pub fn branch_changes_base_path(b: &str) -> String {
    "changes.".to_string() + &b.as_bytes().to_base64(URL_SAFE)
}

pub fn branch_changes_file(p: &Path, b: &str) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join(branch_changes_base_path(b))
}

pub fn id_file(p: &Path) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join("id")
}

pub fn find_repo_root<'a>(dir: &'a Path) -> Option<PathBuf> {
    let mut p = dir.to_path_buf();
    loop {
        p.push(PIJUL_DIR_NAME);
        match metadata(&p) {
            Ok(ref attr) if attr.is_dir() => {
                p.pop();
                return Some(p);
            }
            _ => {}
        }
        p.pop();

        if !p.pop() {
            return None
        }
    }
}

pub const ID_LENGTH: usize = 100;

/// Create a repository.
pub fn create<R:Rng>(dir: &Path, mut rng: R) -> std::io::Result<()> {
    debug!("create: {:?}", dir);
    let mut repo_dir = repo_dir(dir);
    try!(create_dir_all(&repo_dir));

    repo_dir.push("pristine");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();

    repo_dir.push("patches");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();

    repo_dir.push("id");
    let mut f = std::fs::File::create(&repo_dir)?;
    let mut x = String::new();
    x.extend(rng.gen_ascii_chars().take(ID_LENGTH));
    f.write_all(x.as_bytes())?;
    repo_dir.pop();

    repo_dir.push("version");
    let mut f = std::fs::File::create(&repo_dir)?;
    writeln!(f, "{}", env!("CARGO_PKG_VERSION"))?;
    repo_dir.pop();

    Ok(())
}


pub fn patch_file_name(hash: HashRef) -> String {
    hash.to_base64(URL_SAFE) + ".gz"
}

/// Read a complete patch.
pub fn read_patch(repo: &Path, hash: HashRef) -> Result<Patch, Error> {
    let patch_dir = patches_dir(repo);
    let path = patch_dir.join(&patch_file_name(hash));
    let f = File::open(path)?;
    let mut f = BufReader::new(f);
    let (_, _, patch) = Patch::from_reader_compressed(&mut f)?;
    Ok(patch)
}

/// Read a patch, but without the "changes" part, i.e. the actual
/// contents of the patch.
pub fn read_patch_nochanges(repo: &Path, hash: HashRef) -> Result<PatchHeader, Error> {
    let patch_dir = patches_dir(repo);
    let path = patch_dir.join(&patch_file_name(hash));
    let f = File::open(path)?;
    let mut f = flate2::bufread::GzDecoder::new(BufReader::new(f)).unwrap();
    Ok(PatchHeader::from_reader_nochanges(&mut f)?)
}
