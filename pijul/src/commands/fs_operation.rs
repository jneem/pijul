use clap::ArgMatches;
use libpijul::Repository;
use std::path::Path;
use std::fs::{metadata, canonicalize};
use error;
use commands::BasicOptions;
use rand;
use error::Error;

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Add,
    Remove,
}

pub fn run(args: &ArgMatches, op: Operation) -> Result<(), error::Error> {
    debug!("fs_operation {:?}", op);
    let opts = BasicOptions::from_args(args)?;
    let touched_files = match args.values_of("files") {
        Some(l) => l.map(|p| Path::new(p)).collect(),
        None => vec![],
    };

    debug!("repo {:?}", opts.repo_root);
    let mut extra_space = 409600;
    loop {
        match really_run(&opts.pristine_dir(), &opts.cwd, &opts.repo_root, &touched_files, op, extra_space) {
            Err(ref e) if e.lacks_space() => extra_space *= 2,
            e => return e
        }
    }
}

fn really_run(repo_dir: &Path, wd: &Path, r: &Path, files: &[&Path], op: Operation, extra_space: u64) -> Result<(), Error> {
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
