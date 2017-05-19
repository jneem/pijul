use clap::{SubCommand, ArgMatches, Arg};
use commands::{BasicOptions, StaticSubcommand, ask, default_explain};
use libpijul::fs_representation::{read_patch_nochanges, id_file};
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use error::Error;
use std::fs::File;
use std::io::Read;
use std::str;

pub fn invocation() -> StaticSubcommand {
    SubCommand::with_name("changes")
        .about("List the patches applied to the given branch")
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Path to a pijul repository. Defaults to the repository containing the \
                   current directory.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .long("branch")
            .help("The branch to list.")
             .takes_value(true))
        .arg(Arg::with_name("hash-only")
            .long("hash-only")
            .help("Only display the hash of each path."))
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    let hash_only = args.is_present("hash-only");
    if hash_only {
        // If in binary form, start with this repository's id.
        let id_file = id_file(&opts.repo_root);
        let mut f = File::open(&id_file)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        println!("{}", s.trim());
    }

    let repo = opts.open_repo()?;
    let txn = try!(repo.txn_begin());
    if let Some(branch) = txn.get_branch(&opts.branch()) {
        if hash_only {
            for (s, hash) in txn.iter_applied(&branch, None) {
                let hash_ext = txn.get_external(&hash).unwrap();
                println!("{}:{}", hash_ext.to_base64(URL_SAFE), s)
            }
        } else {
            for (_, hash) in txn.rev_iter_applied(&branch, None) {
                let hash_ext = txn.get_external(&hash).unwrap();
                let patch = read_patch_nochanges(&opts.repo_root, hash_ext)?;
                ask::print_patch_descr(&hash_ext.to_owned(), &patch)
            }
        }
    }
    Ok(())
}

pub fn explain(r: Result<(), Error>) {
    default_explain(r)
}
