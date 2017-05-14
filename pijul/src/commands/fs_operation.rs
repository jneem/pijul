use clap::ArgMatches;
use libpijul::Repository;
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use std::path::Path;
use std::fs::{metadata, canonicalize};
use error;
use commands::get_wd;
use rand;
#[derive(Debug)]
pub struct Params<'a> {
    pub touched_files: Vec<&'a Path>,
    pub repository: Option<&'a Path>,
}
use error::Error;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let paths = match args.values_of("files") {
        Some(l) => l.map(|p| Path::new(p)).collect(),
        None => vec![],
    };
    let repository = args.value_of("repository").and_then(|x| Some(Path::new(x)));
    Params {
        repository: repository,
        touched_files: paths,
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Add,
    Remove,
}

pub fn run(args: &Params, op: Operation) -> Result<(), error::Error> {
    debug!("fs_operation {:?}", op);
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(error::Error::NotInARepository),
        Some(ref r) => {
            debug!("repo {:?}", r);
            let repo_dir = pristine_dir(r);
            let mut extra_space = 409600;
            loop {
                match really_run(&repo_dir, &wd, r, args, op, extra_space) {
                    Err(ref e) if e.lacks_space() => extra_space *= 2,
                    e => return e
                }
            }
        }
    }
}

fn really_run(repo_dir: &Path, wd: &Path, r: &Path, args: &Params, op: Operation, extra_space: u64) -> Result<(), Error> {
    let files = &args.touched_files;
    let mut rng = rand::thread_rng();
    let repo = Repository::open(&repo_dir, Some(extra_space))?;
    let mut txn = repo.mut_txn_begin(&mut rng)?;
    match op {
        Operation::Add => {
            for file in &files[..] {
                let p = canonicalize(wd.join(*file))?;
                let m = metadata(&p)?;
                if let Ok(file) = p.strip_prefix(r) {
                    txn.add_file(file, m.is_dir())?
                } else {
                    return Err(Error::InvalidPath(file.to_string_lossy().into_owned()));
                }
            }
        }
        Operation::Remove => {
            for file in &files[..] {
                let p = canonicalize(wd.join(*file))?;
                if let Ok(file) = p.strip_prefix(r) {
                    txn.remove_file(file)?
                } else {
                    return Err(Error::InvalidPath(file.to_string_lossy().into_owned()));
                }
            }
        }
    }
    try!(txn.commit());
    Ok(())

}
