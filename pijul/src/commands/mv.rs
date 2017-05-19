use commands::{BasicOptions, StaticSubcommand, default_explain};
use clap::{SubCommand, ArgMatches, Arg};
use error;
use std::path::PathBuf;
use std::fs::{rename, metadata};

use std;
use rand;
pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("mv")
        .about("Change file names")
        .arg(Arg::with_name("files")
            .multiple(true)
            .help("Files to move.")
            .required(true)
            .min_values(2))
        .arg(Arg::with_name("repository")
            .takes_value(true)
            .long("repository")
            .help("Repository where the files are."));
}

#[derive(Debug)]
pub enum Movement {
    IntoDir { from: Vec<PathBuf>, to: PathBuf },
    FileToFile { from: PathBuf, to: PathBuf },
}

fn get_movement(opts: &BasicOptions, args: &ArgMatches) -> Movement {
    debug!("wd = {:?}", opts.cwd);
    debug!("repo_root = {:?}", opts.repo_root);
    let mut repo_paths = Vec::new();
    for fname in args.values_of("files").unwrap() {
        debug!("fname: {:?}", fname);
        // if fname is absolute, erases current_dir.
        let mut path = std::env::current_dir().unwrap();
        path.push(fname);
        debug!("path = {:?}", path);
        let path = if let Ok(f) = std::fs::canonicalize(&path) {
            f
        } else {
            std::fs::canonicalize(&path.parent().unwrap()).unwrap().join(&path.file_name().unwrap())
        };
        debug!("path = {:?}", path);
        let path = path.strip_prefix(&opts.repo_root).unwrap();
        debug!("path = {:?}", path);

        repo_paths.push(path.to_path_buf());
    }
    debug!("parse_args: done");
    let repo_paths = repo_paths;
    let (dest, origs) = repo_paths.split_last().unwrap();
    let target_path = opts.repo_root.join(&dest);
    let to_dir = target_path.exists() && target_path.is_dir();

    if to_dir {
        Movement::IntoDir {
            from: Vec::from(origs),
            to: dest.clone(),
        }
    } else {
        if origs.len() == 1 {
            Movement::FileToFile {
                from: origs[0].clone(),
                to: dest.clone(),
            }
        } else {
            panic!("Cannot move files into {}: it is not a valid directory",
                   dest.to_string_lossy());
        }
    }
}


pub fn run(args: &ArgMatches) -> Result<(), error::Error> {
    let opts = BasicOptions::from_args(args)?;
    let movement = get_movement(&opts, args);
    let repo = opts.open_repo()?;
    let mut txn = try!(repo.mut_txn_begin(rand::thread_rng()));
    match movement {
        Movement::FileToFile { from: ref orig_path, to: ref dest_path } => {
            try!(txn.move_file(orig_path.as_path(), dest_path.as_path(), false));
            debug!("1 renaming {:?} into {:?}",
                   opts.repo_root.join(orig_path.as_path()),
                   opts.repo_root.join(dest_path.as_path()));
            try!(rename(opts.repo_root.join(orig_path.as_path()),
                        opts.repo_root.join(dest_path.as_path())));
            try!(txn.commit());
            Ok(())
        }
        Movement::IntoDir { from: ref orig_paths, to: ref dest_dir } => {
            for file in orig_paths {
                let f = &file.as_path();
                let repo_target_name = {
                    let target_basename = try!(f.file_name()
                                               .ok_or(error::Error::InvalidPath(f.to_string_lossy().into_owned())));
                    dest_dir.as_path().join(&target_basename)
                };
                let is_dir = try!(metadata(&opts.repo_root.join(f))).is_dir();
                try!(txn.move_file(f, &repo_target_name.as_path(), is_dir));
            }
            for file in orig_paths {
                let f = &file.as_path();
                let full_target_name = {
                    let target_basename = try!(f.file_name()
                                               .ok_or(error::Error::InvalidPath(f.to_string_lossy().into_owned())));
                    dest_dir.as_path().join(&target_basename)
                };
                debug!("2 renaming {:?} into {:?}",
                       opts.repo_root.join(f),
                       opts.repo_root.join(full_target_name.as_path()));
                try!(rename(&opts.repo_root.join(f),
                            opts.repo_root.join(full_target_name.as_path())));
            }
            try!(txn.commit());
            Ok(())
        }
    }
}

pub fn explain(res: Result<(), error::Error>) {
    default_explain(res)
}
