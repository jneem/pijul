extern crate app_dirs;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate flate2;
extern crate futures;
extern crate getch;
extern crate hyper;
extern crate hyper_rustls;
extern crate libpijul;
#[macro_use]
extern crate log;
extern crate rand;
extern crate regex;
extern crate rustc_serialize;
extern crate rustyline;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate shell_escape;
extern crate tar;
extern crate term;
extern crate thrussh;
extern crate tokio_core;
extern crate toml;
extern crate user;

mod error;
mod commands;
mod meta;

macro_rules! pijul_subcommand_dispatch {
    ($default:expr, $p:expr => $($subcommand_name:expr => $subcommand:ident),*) => {{
        match $p {
            $(($subcommand_name, Some(args)) =>
             {
                 let res = commands::$subcommand::run(&args);
                 commands::$subcommand::explain(res)
             }
              ),*
                ("", None) => { $default; println!(""); },
            _ => panic!("Incorrect subcommand name")
        }
    }}
}

fn main() {
    env_logger::init().unwrap();
    let time0 = chrono::Local::now();
    let version = crate_version!();
    let app = clap::App::new("pijul")
        .version(&version[..])
        .author("Pierre-Étienne Meunier and Florent Becker")
        .about("Version Control: fast, distributed, easy to use; pick any three");
    let app = app.subcommands(commands::all_command_invocations());
    let mut app_help = app.clone();

    let args = app.get_matches();
    pijul_subcommand_dispatch!(app_help.print_help().unwrap(), args.subcommand() =>
                               "info" => info,
                               "changes" => changes,
                               "patch" => patch,
                               "init" => init,
                               "add" => add,
                               "record" => record,
                               "pull" => pull,
                               "push" => push,
                               "apply" => apply,
                               "clone" => clone,
                               "remove" => remove,
                               "mv" => mv,
                               "ls" => ls,
                               "revert" => revert,
                               "unrecord" => unrecord,
                               "fork" => fork,
                               "branches" => branches,
                               "delete-branch" => delete_branch,
                               "checkout" => checkout,
                               "diff" => diff,
                               "blame" => blame,
                               "dist" => dist
                               );
    let time1 = chrono::Local::now();
    info!("The command took: {:?}", time1.signed_duration_since(time0));
}
