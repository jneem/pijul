use clap::{SubCommand, ArgMatches, Arg, Values};
use commands::{StaticSubcommand, default_explain};
use libpijul::{DEFAULT_BRANCH, Hash, apply_resize};
use libpijul::patch::Patch;
use libpijul::fs_representation::find_repo_root;

use error::Error;
use std::collections::HashSet;

use std::path::Path;
use std::io::{BufReader, stdin};
use super::{get_wd, get_current_branch};

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
            .help("Path to the repository where the patches will be applied.")
            .takes_value(true))
        .arg(Arg::with_name("branch")
            .long("branch")
            .help("The branch to which the patches will be applied.")
            .takes_value(true));
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub hashes: Option<Values<'a>>,
    pub branch: Option<&'a str>,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(|x| Path::new(x)),
        hashes: args.values_of("patch"),
        branch: args.value_of("branch"),
    }
}

pub fn run(params: &mut Params) -> Result<(), Error> {
    let wd = try!(get_wd(params.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref target) => {
            debug!("applying");
            let branch_name = if let Some(b) = params.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(target) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };
            let remote: HashSet<Hash> = if let Some(hashes) = params.hashes.take() {
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
                match apply_resize(&target, &branch_name, remote.iter()) {
                    Err(ref e) if e.lacks_space() => {}
                    Ok(()) => return Ok(()),
                    Err(e) => return Err(From::from(e)),
                }
            }
        }
    }
}

pub fn explain(res: Result<(), Error>) {
    default_explain(res)
}
