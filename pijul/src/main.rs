#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate libpijul;
extern crate getch;
extern crate toml;
extern crate regex;
extern crate hyper;
extern crate hyper_rustls;
extern crate chrono;
extern crate thrussh;
extern crate rustc_serialize;
extern crate term;
extern crate rand;
extern crate tokio_core;
extern crate futures;
extern crate user;
extern crate shell_escape;
extern crate rustyline;
extern crate tar;
extern crate flate2;
// #[macro_use]
// extern crate serde_derive;
// extern crate serde;

mod error;
mod commands;
mod meta;

macro_rules! pijul_subcommand_dispatch {
    ($default:expr, $p:expr => $($subcommand_name:expr => $subcommand:ident),*) => {{
        match $p {
            $(($subcommand_name, Some(args)) =>
             {
                 let mut params = commands::$subcommand::parse_args(args);
                 let res = commands::$subcommand::run(&mut params);
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
