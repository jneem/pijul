use clap::{SubCommand, ArgMatches, Arg};
use commands::{BasicOptions, StaticSubcommand, default_explain};
use libpijul::{Hash};
use libpijul::fs_representation::patches_dir;

use error::Error;
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use std::io::{stdout, copy};
use std::fs::File;

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

pub fn run(args: &ArgMatches) -> Result<(), Error> {
    let opts = BasicOptions::from_args(args)?;
    // FIXME: the second panic could unwrap
    let patch = Hash::from_base64(args.value_of("patch").unwrap()).unwrap();
    let mut patch_path = patches_dir(opts.repo_root).join(&patch.to_base64(URL_SAFE));
    patch_path.set_extension("gz");
    let mut f = try!(File::open(&patch_path));
    let mut stdout = stdout();
    try!(copy(&mut f, &mut stdout));
    Ok(())
}

pub fn explain(r: Result<(), Error>) {
    default_explain(r)
}
