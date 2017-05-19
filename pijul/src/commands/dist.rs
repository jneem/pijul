use commands::{BasicOptions, StaticSubcommand, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;
use libpijul::{ROOT_KEY, Branch, Txn, Key, PatchId, Edge};
use std::path::{PathBuf, Path};
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
             .takes_value(true)
             .long("repository")
             .help("Repository to list."));
}

pub fn run(args: &ArgMatches) -> Result<(), error::Error> {
    let opts = BasicOptions::from_args(args)?;
    let archive_path = {
        let mut path = Path::new(args.value_of("archive").unwrap()).to_path_buf();
        path.set_extension("tar.gz");
        path
    };
    let repo = opts.open_repo()?;
    let txn = repo.txn_begin()?;
    if let Some(branch) = txn.get_branch(&opts.branch()) {
        let encoder = GzEncoder::new(File::create(&archive_path)?, Compression::Best);
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
