use clap::{SubCommand, ArgMatches, Arg};
use commands::{StaticSubcommand, default_explain};
use libpijul::{Repository, DEFAULT_BRANCH, PatchId, Key, Value, Txn};
use libpijul::graph::LineBuffer;
use libpijul::fs_representation::{pristine_dir, find_repo_root, read_patch_nochanges};
use std::path::Path;
use error;
use commands::get_wd;
use std::fs::{canonicalize};
use super::get_current_branch;
use std::io::{stdout, Stdout};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("blame")
        .about("Show what patch introduced each line of a file.")
        .arg(Arg::with_name("repository")
             .long("repository")
             .takes_value(true)
             .help("Local repository."))
        .arg(Arg::with_name("file")
             .help("File to annotate.")
             .required(true)
             .takes_value(true)
        );
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub file: &'a Path
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(Path::new),
        file: Path::new(args.value_of("file").unwrap())
    }
}

pub fn run(args: &Params) -> Result<(), error::Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(error::Error::NotInARepository),
        Some(ref r) => {
            let pristine_dir = pristine_dir(r);
            let branch = if let Ok(b) = get_current_branch(r) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };
            let p = canonicalize(wd.join(args.file))?;
            if let Ok(file) = p.strip_prefix(r) {
                let repo = Repository::open(&pristine_dir, None)?;
                let txn = repo.txn_begin()?;
                if let Some(branch) = txn.get_branch(&branch) {
                    let inode = txn.find_inode(&file)?;
                    if txn.is_directory(&inode) {
                        return Err(error::Error::IsDirectory)
                    }
                    let node = txn.get_inodes(&inode).unwrap();
                    let mut graph = txn.retrieve(&branch, &node.key);
                    let mut buf = OutBuffer {
                        stdout: stdout(),
                        txn: &txn,
                        target: r
                    };
                    txn.output_file(&mut buf, &mut graph, &mut Vec::new())?
                }
            }
            Ok(())
        }
    }
}

struct OutBuffer<'a> { stdout: Stdout, txn: &'a Txn<'a>, target: &'a Path }

use libpijul::Transaction;
use libpijul;
use std::io::Write;
use rustc_serialize::base64::{URL_SAFE, ToBase64};

impl<'a, T: 'a + Transaction> LineBuffer<'a, T> for OutBuffer<'a> {

    fn output_line(&mut self, key: &Key<PatchId>, contents: Value<'a, T>) -> Result<(), libpijul::error::Error> {
        let ext = self.txn.get_external(&key.patch).unwrap();
        let patch = read_patch_nochanges(self.target, ext)?;
        write!(self.stdout, "{} {} {} > ", patch.authors[0], patch.timestamp.format("%F %R %Z"), key.patch.to_base64(URL_SAFE))?;
        let mut ends_with_eol = false;
        for chunk in contents {
            self.stdout.write_all(chunk)?;
            if let Some(&c) = chunk.last() {
                ends_with_eol = c == b'\n'
            }
        }
        if !ends_with_eol {
            writeln!(self.stdout, "")?;
        }
        Ok(())
    }

    fn output_conflict_marker(&mut self, s: &'a str) -> Result<(), libpijul::error::Error> {
        write!(self.stdout, "{}", s)?;
        Ok(())
    }
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
