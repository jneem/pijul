use clap::{SubCommand, ArgMatches, Arg};
use commands::{StaticSubcommand, default_explain};
use libpijul::{Hash, apply_resize};
use libpijul::patch::Patch;

use error::Error;
use std::collections::HashSet;

use std::io::{BufReader, stdin};
use super::BasicOptions;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("apply")
        .about("apply a patch")
        .arg(Arg::with_name("patch")
            .help("Hash of the patch to apply, in base64. If no patch is given, patches are \
                   read from the standard input.")
            .takes_value(true)
            .multiple(true))
        .arg(Arg::with_name("repository")
            .long("repository")
            .help("Path to the repository where the patches will be applied. Defaults to the \
                   repository containing the current directory.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .long("branch")
            .help("The branch to which the patches will be applied. Defaults to the current \
                   branch.")
            .takes_value(true));
}

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    debug!("applying");
    let remote: HashSet<Hash> = if let Some(hashes) = args.values_of("hash") {
        hashes.map(|h| Hash::from_base64(&h).unwrap()).collect()
    } else {
        // Read patches in gz format from stdin.
        let mut hashes = HashSet::new();
        let mut buffered = BufReader::new(stdin());
        while let Ok((h, _, patch)) = Patch::from_reader_compressed(&mut buffered) {
            debug!("{:?}", patch);
            hashes.insert(h);
            break;
        }
        hashes
    };

    debug!("remote={:?}", remote);
    loop {
        match apply_resize(&opts.repo_root, &opts.branch(), remote.iter()) {
            Err(ref e) if e.lacks_space() => {}
            Ok(()) => return Ok(()),
            Err(e) => return Err(From::from(e)),
        }
    }
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
