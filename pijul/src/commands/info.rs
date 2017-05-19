use clap::{SubCommand, Arg, ArgMatches};
use libpijul::Inode;
use std::fs::File;

use error::Error;
use super::{BasicOptions, StaticSubcommand, default_explain, get_current_branch};

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("info")
        .about("Get information about the current repository, if any")
        .arg(Arg::with_name("dir")
             .help("Pijul info will be given about this directory.")
             .required(false))
        .arg(Arg::with_name("debug")
             .long("--debug")
             .help("Pijul info will be given about this directory.")
             .required(false))
        .arg(Arg::with_name("inode")
             .long("--from-inode")
             .help("Inode to start the graph from.")
             .takes_value(true)
             .required(false));
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    println!("Current repository root: {:?}", opts.repo_root);
    println!("Current branch: {:?}", get_current_branch(&opts.repo_root)?);

    if args.is_present("debug") {
        let repo = opts.open_repo()?;
        let txn = repo.txn_begin()?;
        txn.dump("dump");
        if let Some(ref inode) = args.value_of("inode") {
            // Output just the graph under `inode`.
            let inode = Inode::from_hex(inode)?;
            let node = &txn.get_inodes(&inode).unwrap().key;
            for branch in txn.iter_branches(None) {
                let ret = txn.retrieve(&branch, node);
                let mut f = try!(File::create(format!("debug_{}", branch.name.as_str())));
                ret.debug(&mut f)?
            }
        } else {
            // Output everything.
            for branch in txn.iter_branches(None) {
                let mut f = try!(File::create(format!("debug_{}", branch.name.as_str())));
                txn.debug(branch.name.as_str(), &mut f);
            }
        }
    }
    Ok(())
}

pub fn explain(r: Result<(), Error>) {
    default_explain(r)
}
