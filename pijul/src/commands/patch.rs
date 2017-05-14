use clap::{SubCommand, ArgMatches, Arg};
use commands::{StaticSubcommand, default_explain};
use libpijul::{Hash};
use libpijul::fs_representation::{find_repo_root, patches_dir};

use error::Error;
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use std::path::Path;
use std::io::{stdout, copy};
use std::fs::File;
use super::get_wd;

pub fn invocation() -> StaticSubcommand {
    return SubCommand::with_name("patch")
        .about("Output a patch (in binary)")

        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Path to the repository where the patches will be applied.")
             .takes_value(true))

        .arg(Arg::with_name("patch")
             .help("The hash of the patch to be printed.")
             .takes_value(true)
             .required(true)
             .validator(|x| {
                 if Hash::from_base64(&x).is_some() {
                     Ok(())
                 } else {
                     Err(String::new())
                 }
             }));
}

pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub patch: Hash
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {
        repository: args.value_of("repository").map(Path::new),
        patch: Hash::from_base64(args.value_of("patch").unwrap()).unwrap()
    }
}

pub fn run(params: &mut Params) -> Result<(), Error> {
    let wd = try!(get_wd(params.repository));
    match find_repo_root(&wd) {
        None => return Err(Error::NotInARepository),
        Some(ref target) => {
            let mut patch_path = patches_dir(target).join(&params.patch.to_base64(URL_SAFE));
            patch_path.set_extension("gz");
            let mut f = try!(File::open(&patch_path));
            let mut stdout = stdout();
            try!(copy(&mut f, &mut stdout));
            Ok(())
        }
    }
}

pub fn explain(r: Result<(), Error>) {
    default_explain(r)
}
