use commands::{StaticSubcommand, get_wd, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;
use libpijul::{Repository, ROOT_KEY, Branch, Txn, DEFAULT_BRANCH, Key, PatchId, Edge};
use libpijul::fs_representation::{pristine_dir, find_repo_root};
use std::path::{PathBuf, Path};
use super::get_current_branch;
use tar::{Header, Builder};
use std::fs::File;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("dist")
        .about("Produces a tar.gz archive of the repository")
        .arg(Arg::with_name("archive")
             .short("d")
             .takes_value(true)
             .required(true)
             .help("File name of the output archive."))
        .arg(Arg::with_name("branch")
             .long("branch")
             .help("The branch where to record, defaults to the current branch.")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Repository to list."));
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub archive_path: PathBuf,
    pub branch: Option<&'a str>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").and_then(|x| Some(Path::new(x))),
        archive_path: {
            let mut path = Path::new(args.value_of("archive").unwrap()).to_path_buf();
            path.set_extension("tar.gz");
            path
        },
        branch: args.value_of("branch")
    }
}

pub fn run(args: &Params) -> Result<(), error::Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => Err(error::Error::NotInARepository),
        Some(ref r) => {
            let repo_dir = pristine_dir(r);
            let repo = Repository::open(&repo_dir, None).map_err(error::Error::Repository)?;

            let branch_name = if let Some(b) = args.branch {
                b.to_string()
            } else if let Ok(b) = get_current_branch(r) {
                b
            } else {
                DEFAULT_BRANCH.to_string()
            };

            let txn = repo.txn_begin()?;
            if let Some(branch) = txn.get_branch(&branch_name) {
                let encoder = GzEncoder::new(File::create(&args.archive_path)?, Compression::Best);
                let mut archive = Builder::new(encoder);
                let mut buffer = Vec::new();
                let mut forward = Vec::new();
                let mut current_path = PathBuf::new();
                archive_rec(&txn, &branch, ROOT_KEY, &mut archive, &mut buffer, &mut forward,
                            &mut current_path)?;
                archive.into_inner()?.finish()?.flush()?;
            }
            Ok(())
        }
    }
}

pub fn archive_rec<W:Write>(
    txn: &Txn, branch: &Branch, key: Key<PatchId>,
    builder: &mut Builder<W>, buffer: &mut Vec<u8>,
    forward: &mut Vec<(Key<PatchId>, Edge)>, current_path: &mut PathBuf
) -> Result<(), error::Error> {

    let files = txn.list_files_under_node(branch, &key);
    for (key, names) in files {
        debug!("archive_rec: {:?} {:?}", key, names);
        if names.len() > 1 {
            error!("file has several names: {:?}", names);
        }
        current_path.push(names[0].1);
        if names[0].0.is_dir() {
            archive_rec(txn, branch, key, builder, buffer, forward, current_path)?;
        } else {
            buffer.clear();
            let mut graph = txn.retrieve(&branch, &key);
            txn.output_file(buffer, &mut graph, forward)?;
            let mut header = Header::new_gnu();
            header.set_path(&current_path)?;
            header.set_size(buffer.len() as u64);
            header.set_mode(names[0].0.permissions() as u32);
            header.set_cksum();
            builder.append(&header, &buffer[..])?;
        }
        current_path.pop();
    }
    Ok(())
}


pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
