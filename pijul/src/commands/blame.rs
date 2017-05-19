use clap::{SubCommand, ArgMatches, Arg};
use commands::{BasicOptions, StaticSubcommand, default_explain};
use libpijul::{PatchId, Key, Value, Txn};
use libpijul::graph::LineBuffer;
use libpijul::fs_representation::read_patch_nochanges;
use std::path::Path;
use error;
use std::fs::{canonicalize};
use std::io::{stdout, Stdout};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("blame")
        .about("Show what patch introduced each line of a file.")
        .arg(Arg::with_name("repository")
            .long("repository")
            .takes_value(true)
            .help("Path to a pijul repository. Defaults to the repository containing the \
                   current directory."))
        .arg(Arg::with_name("branch")
            .long("branch")
            .help("The branch to get the history from. Defaults to the current branch.")
            .takes_value(true))
        .arg(Arg::with_name("file")
            .help("File to annotate.")
            .required(true)
            .takes_value(true)
        );
}

pub fn run(args: &ArgMatches) -> Result<(), error::Error> {
    let opts = BasicOptions::from_args(args)?;
    let file = Path::new(args.value_of("file").unwrap());
    let p = canonicalize(opts.cwd.join(file))?;
    if let Ok(file) = p.strip_prefix(&opts.repo_root) {
        let repo = opts.open_repo()?;
        let txn = repo.txn_begin()?;
        if let Some(branch) = txn.get_branch(&opts.branch()) {
            let inode = txn.find_inode(&file)?;
            if txn.is_directory(&inode) {
                return Err(error::Error::IsDirectory)
            }
            let node = txn.get_inodes(&inode).unwrap();
            let mut graph = txn.retrieve(&branch, &node.key);
            let mut buf = OutBuffer {
                stdout: stdout(),
                txn: &txn,
                target: &opts.repo_root,
            };
            txn.output_file(&mut buf, &mut graph, &mut Vec::new())?
        }
    }
    Ok(())
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
