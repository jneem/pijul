use clap;
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
use std::fs::{File, canonicalize};
use std::path::{Path, PathBuf};
use std::env::current_dir;
use libpijul::fs_representation::repo_dir;
use error::Error;
use std::io::{Read, Write, stderr};
use std::process::exit;
use libpijul::DEFAULT_BRANCH;

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
    let mut path = repo_dir(root);
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
    let mut path = repo_dir(root);
    path.push("current_branch");
    let mut f = File::create(&path)?;
    f.write_all(branch.trim().as_ref())?;
    f.write_all(b"\n")?;
    Ok(())
}

fn default_explain<R>(command_result: Result<R, Error>) {
    match command_result {
        Ok(_) => (),
        Err(e) => {
            write!(stderr(), "error: {}", e).unwrap();
            exit(1)
        }
    }
}
