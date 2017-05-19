use clap;
use clap::ArgMatches;
pub type StaticSubcommand = clap::App<'static, 'static>;

mod fs_operation;
mod remote;
mod ask;

pub mod info;
pub mod init;
pub mod record;
pub mod add;
pub mod pull;
pub mod push;
pub mod apply;
pub mod clone;
pub mod remove;
pub mod mv;
pub mod ls;
pub mod revert;
pub mod unrecord;
pub mod changes;
pub mod patch;
pub mod fork;
pub mod branches;
pub mod delete_branch;
pub mod checkout;
pub mod diff;
pub mod blame;
pub mod dist;

#[cfg(test)]
mod test;

use rand;
use std::fs::{File, canonicalize, metadata};
use std::path::{Path, PathBuf};
use std::env::current_dir;
use libpijul::{DEFAULT_BRANCH, Repository, fs_representation};
use error::Error;
use std::io::{Read, Write, stderr};
use std::process::exit;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![changes::invocation(),
                info::invocation(),
                init::invocation(),
                record::invocation(),
                unrecord::invocation(),
                add::invocation(),
                pull::invocation(),
                push::invocation(),
                apply::invocation(),
                clone::invocation(),
                remove::invocation(),
                mv::invocation(),
                ls::invocation(),
                revert::invocation(),
                patch::invocation(),
                fork::invocation(),
                branches::invocation(),
                delete_branch::invocation(),
                checkout::invocation(),
                diff::invocation(),
                blame::invocation(),
                dist::invocation(),
    ];
}

pub fn get_wd(repository_path: Option<&Path>) -> Result<PathBuf, Error> {
    debug!("get_wd: {:?}", repository_path);
    match repository_path {
        None => Ok(canonicalize(current_dir()?)?),
        Some(a) if a.is_relative() => Ok(canonicalize(current_dir()?.join(a))?),
        Some(a) => Ok(canonicalize(a)?)
    }
}

pub fn get_current_branch(root: &Path) -> Result<String, Error> {
    debug!("path: {:?}", root);
    let mut path = fs_representation::repo_dir(root);
    path.push("current_branch");
    if let Ok(mut f) = File::open(&path) {
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        Ok(s.trim().to_string())
    } else {
        Ok(DEFAULT_BRANCH.to_string())
    }
}

pub fn set_current_branch(root: &Path, branch: &str) -> Result<(), Error> {
    debug!("set current branch: root={:?}, branch={:?}", root, branch);
    let mut path = fs_representation::repo_dir(root);
    path.push("current_branch");
    let mut f = File::create(&path)?;
    f.write_all(branch.trim().as_ref())?;
    f.write_all(b"\n")?;
    Ok(())
}

/// Returns an error if the `dir` is contained in a repository.
pub fn assert_no_containing_repo(dir: &Path) -> Result<(), Error> {
    if fs_representation::find_repo_root(dir).is_some() {
        Err(Error::InARepository(dir.to_owned()))
    } else {
        Ok(())
    }
}

/// Creates an empty pijul repository in the given directory.
pub fn create_repo(dir: &Path) -> Result<(), Error> {
    // Check that a repository does not already exist.
    let repo_dir = fs_representation::repo_dir(dir);
    if let Ok(attrs) = metadata(&repo_dir) {
        if attrs.is_dir() {
            return Err(Error::InARepository(dir.to_owned()));
        }
    }

    fs_representation::create(&dir, rand::thread_rng())?;
    let pristine_dir = fs_representation::pristine_dir(&dir);
    let repo = Repository::open(&pristine_dir, None)?;
    repo.mut_txn_begin(rand::thread_rng())?
        .commit()?;
    Ok(())
}

fn default_explain<R>(command_result: Result<R, Error>) {
    match command_result {
        Ok(_) => (),
        Err(e) => {
            writeln!(stderr(), "error: {}", e).unwrap();
            exit(1)
        }
    }
}

/// Almost all commands want to know the current directory and the repository root.  This struct
/// fills that need, and also provides methods for other commonly-used tasks.
pub struct BasicOptions<'a> {
    /// This isn't 100% the same as the actual current working directory, so pay attention: this
    /// will be the current directory, unless the user specifies `--repository`, in which case
    /// `cwd` will actually be the path of the repository root. In other words, specifying
    /// `--repository` has the same effect as changing directory to the repository root before
    /// running `pijul`.
    pub cwd: PathBuf,
    pub repo_root: PathBuf,
    args: &'a ArgMatches<'a>,
}

impl<'a> BasicOptions<'a> {
    /// Reads the options from command line arguments.
    pub fn from_args(args: &'a ArgMatches<'a>) -> Result<BasicOptions<'a>, Error> {
        let wd = get_wd(args.value_of("repository").map(Path::new))?;
        let repo_root = fs_representation::find_repo_root(&wd).ok_or(Error::NotInARepository)?;
        Ok(BasicOptions {
            cwd: wd,
            repo_root: repo_root,
            args: args,
        })
    }

    /// Gets the name of the desired branch.
    pub fn branch(&self) -> String {
        if let Some(b) = self.args.value_of("branch") {
            b.to_string()
        } else if let Ok(b) = get_current_branch(&self.repo_root) {
            b
        } else {
            DEFAULT_BRANCH.to_string()
        }
    }

    pub fn repo_dir(&self) -> PathBuf {
        fs_representation::repo_dir(&self.repo_root)
    }

    pub fn open_repo(&self) -> Result<Repository, Error> {
        Repository::open(self.pristine_dir(), None).map_err(|e| e.into())
    }

    pub fn open_and_grow_repo(&self, increase: u64) -> Result<Repository, Error> {
        Repository::open(self.pristine_dir(), Some(increase)).map_err(|e| e.into())
    }

    pub fn pristine_dir(&self) -> PathBuf {
        fs_representation::pristine_dir(&self.repo_root)
    }
}

