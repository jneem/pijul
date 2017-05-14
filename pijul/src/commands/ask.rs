use std::io::prelude::*;
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use getch;
use libpijul::patch::{Change, Patch, Record, EdgeOp, PatchHeader};

use std::io::stdout;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::path::PathBuf;

use error::Error;
use libpijul::{MutTxn, LineId, FOLDER_EDGE, PARENT_EDGE, DELETED_EDGE, Hash};
use std::char::from_u32;
use std::str;
use std;
use rand;
use rustyline;
use term::{StdoutTerminal, Attr};
use term;

const BINARY_CONTENTS: &'static str = "<binary contents>";
#[derive(Clone,Copy)]
pub enum Command {
    Pull,
    Push,
    Unrecord,
}

pub fn print_patch_descr(hash: &Hash, patch: &PatchHeader) {
    println!("Hash: {}", hash.to_base64(URL_SAFE));
    println!("Authors: {:?}", patch.authors);
    println!("Timestamp {}", patch.timestamp);
    println!("  * {}", patch.name);
    match patch.description {
        Some(ref d) => println!("  {}", d),
        None => {}
    };
}


fn check_forced_decision(command: Command,
                         choices: &HashMap<&Hash, bool>,
                         rev_dependencies: &HashMap<&Hash, Vec<&Hash>>,
                         a: &Hash,
                         b: &Patch)
                         -> Option<bool> {

    let covariant = match command {
        Command::Pull | Command::Push => true,
        Command::Unrecord => false,
    };
    // If we've selected patches that depend on a, and this is a pull
    // or a push, select a.
    if let Some(x) = rev_dependencies.get(a) {
        for y in x {
            // Here, y depends on a.
            //
            // If this command is covariant, and we've selected y, select a.
            // If this command is covariant, and we've unselected y, don't do anything.
            //
            // If this command is contravariant, and we've selected y, don't do anything.
            // If this command is contravariant, and we've unselected y, unselect a.
            if let Some(&choice) = choices.get(y) {
                if choice == covariant {
                    return Some(covariant);
                }
            }
        }
    };

    // If we've unselected dependencies of a, unselect a.
    for y in b.dependencies.iter() {
        // Here, a depends on y.
        //
        // If this command is covariant, and we've selected y, don't do anything.
        // If this command is covariant, and we've unselected y, unselect a.
        //
        // If this command is contravariant, and we've selected y, select a.
        // If this command is contravariant, and we've unselected y, don't do anything.

        if let Some(&choice) = choices.get(&y) {
            if choice != covariant {
                return Some(!covariant);
            }
        }
    }

    None
}

fn interactive_ask(getch: &getch::Getch,
                   a: &Hash,
                   b: &Patch,
                   command_name: Command)
                   -> Result<(char, Option<bool>), Error> {
    print_patch_descr(a, b);
    print!("{} [ynkad] ",
           match command_name {
               Command::Push => "Shall I push this patch?",
               Command::Pull => "Shall I pull this patch?",
               Command::Unrecord => "Shall I unrecord this patch?",
           });
    try!(stdout().flush());
    match getch.getch().ok().and_then(|x| from_u32(x as u32)) {
        Some(e) => {
            println!("{}", e);
            let e = e.to_uppercase().next().unwrap_or('\0');
            match e {
                'A' => Ok(('Y', Some(true))),
                'D' => Ok(('N', Some(false))),
                e => Ok((e, None)),
            }
        }
        _ => Ok(('\0', None)),
    }
}



/// Patches might have a dummy "changes" field here.
pub fn ask_patches(command: Command, patches: &[(Hash, Patch)]) -> Result<HashSet<Hash>, Error> {

    let getch = try!(getch::Getch::new());
    let mut i = 0;

    // Record of the user's choices.
    let mut choices: HashMap<&Hash, bool> = HashMap::new();

    // For each patch, the list of patches that depend on it.
    let mut rev_dependencies: HashMap<&Hash, Vec<&Hash>> = HashMap::new();

    // Decision for the remaining patches ('a' or 'd'), if any.
    let mut final_decision = None;


    while i < patches.len() {
        let (ref a, ref b) = patches[i];
        let forced_decision = check_forced_decision(command, &choices, &rev_dependencies, a, b);

        // Is the decision already forced by a previous choice?
        let e = match forced_decision.or(final_decision) {
            Some(true) => 'Y',
            Some(false) => 'N',
            None => {
                debug!("decision not forced");
                let (current, remaining) = try!(interactive_ask(&getch, a, b, command));
                final_decision = remaining;
                current
            }
        };
        debug!("decision: {:?}", e);
        match e {
            'Y' => {
                choices.insert(a, true);
                match command {
                    Command::Pull | Command::Push => {
                        for ref dep in b.dependencies.iter() {
                            let d = rev_dependencies.entry(dep).or_insert(vec![]);
                            d.push(a)
                        }
                    }
                    Command::Unrecord => {}
                }
                i += 1
            }
            'N' => {
                choices.insert(a, false);
                match command {
                    Command::Unrecord => {
                        for ref dep in b.dependencies.iter() {
                            let d = rev_dependencies.entry(dep).or_insert(vec![]);
                            d.push(a)
                        }
                    }
                    Command::Pull | Command::Push => {}
                }
                i += 1
            }
            'K' if i > 0 => {
                let (ref a, _) = patches[i];
                choices.remove(a);
                i -= 1
            }
            _ => {}
        }
    }
    Ok(choices.into_iter()
        .filter(|&(_, selected)| selected)
        .map(|(x, _)| x.to_owned())
        .collect())
}


fn change_deps(id: usize, c: &Record, provided_by: &mut HashMap<LineId, usize>) -> HashSet<LineId> {
    let mut s = HashSet::new();
    for c in c.iter() {
        match *c {
            Change::NewNodes { ref up_context, ref down_context, ref line_num, ref nodes, .. } => {
                for cont in up_context.iter().chain(down_context) {

                    if cont.patch.is_none() && !cont.line.is_root() {
                        s.insert(cont.line.clone());
                    }
                }
                for i in 0..nodes.len() {
                    provided_by.insert(*line_num + i, id);
                }
            }
            Change::NewEdges { ref edges, .. } => {
                for e in edges {
                    if e.from.patch.is_none() && !e.from.line.is_root() {
                        s.insert(e.from.line.clone());
                    }
                    if e.to.patch.is_none() && !e.from.line.is_root() {
                        s.insert(e.to.line.clone());
                    }
                }
            }
        }
    }
    s
}

fn print_change<T: rand::Rng>(term: &mut Box<StdoutTerminal>,
                              repo: &MutTxn<T>,
                              current_file: &mut Option<Rc<PathBuf>>,
                              c: &Record)
                              -> Result<(), Error> {
    match *c {

        Record::FileAdd { ref name, .. } => {
            term.fg(term::color::CYAN).unwrap_or(());
            print!("added file ");
            term.reset().unwrap_or(());
            println!("{}", name);
            Ok(())
        }
        Record::FileDel { ref name, .. } => {
            term.fg(term::color::MAGENTA).unwrap_or(());
            print!("deleted file: ");
            term.reset().unwrap_or(());
            println!("{}", name);
            Ok(())
        }
        Record::FileMove { ref new_name, .. } => {
            term.fg(term::color::YELLOW).unwrap_or(());
            print!("file moved to: ");
            term.reset().unwrap_or(());
            println!("{}", new_name);
            Ok(())
        }
        Record::Replace { ref adds, ref dels, ref file, .. } => {
            let r = Record::Change { change: dels.clone(), file: file.clone(), conflict_reordering: Vec::new() };
            print_change(term, repo, current_file, &r)?;
            let r = Record::Change { change: adds.clone(), file: file.clone(), conflict_reordering: Vec::new() };
            print_change(term, repo, current_file, &r)
        }
        Record::Change { ref change, ref file, .. } => {
            match *change {
                Change::NewNodes { // ref up_context,ref down_context,ref line_num,
                                   ref flag,
                                   ref nodes,
                                   .. } => {
                    for n in nodes {
                        if flag.contains(FOLDER_EDGE) {
                            if n.len() >= 2 {
                                term.fg(term::color::CYAN).unwrap_or(());
                                print!("new file ");
                                term.reset().unwrap_or(());
                                println!("{}", str::from_utf8(&n[2..]).unwrap_or(""));
                            }
                        } else {
                            let s = str::from_utf8(n).unwrap_or(BINARY_CONTENTS);
                            let mut file_changed = true;
                            if let Some(ref cur_file) = *current_file {
                                if file == cur_file {
                                    file_changed = false;
                                }
                            }
                            if file_changed {
                                term.attr(Attr::Bold).unwrap_or(());
                                term.attr(Attr::Underline(true)).unwrap_or(());
                                println!("In file {:?}\n", file);
                                term.reset().unwrap_or(());
                                *current_file = Some(file.clone())
                            }
                            term.fg(term::color::GREEN).unwrap_or(());
                            print!("+ ");
                            term.reset().unwrap_or(());
                            if s.ends_with("\n") {
                                print!("{}", s);
                            } else {
                                println!("{}", s);
                            }
                        }
                    }
                    Ok(())
                }
                Change::NewEdges { ref edges, ref op, .. } => {
                    let mut h_targets = HashSet::with_capacity(edges.len());
                    for e in edges {
                        let (target, flag) = match *op {
                            EdgeOp::Map { flag, .. } |
                            EdgeOp::New { flag, .. } |
                            EdgeOp::Forget { previous: flag } => {
                                if !flag.contains(PARENT_EDGE) {
                                    if h_targets.insert(&e.to) {
                                        (Some(&e.to), flag)
                                    } else {
                                        (None, flag)
                                    }
                                } else {
                                    if h_targets.insert(&e.from) {
                                        (Some(&e.from), flag)
                                    } else {
                                        (None, flag)
                                    }
                                }
                            }
                        };
                        if let Some(target) = target {
                            let internal = repo.internal_key_unwrap(target);
                            let l = repo.get_contents(&internal).unwrap();
                            let l = l.into_cow();
                            let s = str::from_utf8(&l).unwrap_or(BINARY_CONTENTS);

                            let mut file_changed = true;
                            if let Some(ref cur_file) = *current_file {
                                if file == cur_file {
                                    file_changed = false;
                                }
                            }
                            if file_changed {
                                term.attr(Attr::Bold).unwrap_or(());
                                term.attr(Attr::Underline(true)).unwrap_or(());
                                println!("In file {:?}\n", file);
                                term.reset().unwrap_or(());
                                *current_file = Some(file.clone())
                            }


                            if flag.contains(DELETED_EDGE) {
                                term.fg(term::color::RED).unwrap_or(());
                                print!("- ");
                            } else {
                                term.fg(term::color::GREEN).unwrap_or(());
                                print!("+ ");
                            }
                            term.reset().unwrap_or(());
                            if s.ends_with("\n") {
                                print!("{}", s)
                            } else {
                                println!("{}", s)
                            }
                        }
                    }
                    Ok(())
                }
            }
        }
    }
}

pub enum ChangesDirection {
    Record,
    Revert
}

impl ChangesDirection {
    fn is_record(&self) -> bool {
        match *self {
            ChangesDirection::Record => true,
            _ => false
        }
    }
    fn verb(&self) -> &str {
        match *self {
            ChangesDirection::Record => "record",
            ChangesDirection::Revert => "revert",
        }
    }
}

pub fn ask_changes<T: rand::Rng>(repository: &MutTxn<T>,
                                 changes: &[Record],
                                 direction: ChangesDirection)
                                 -> Result<HashMap<usize, bool>, Error> {
    debug!("changes: {:?}", changes);
    let mut terminal = term::stdout().unwrap();
    let getch = try!(getch::Getch::new());
    let mut i = 0;
    let mut choices: HashMap<usize, bool> = HashMap::new();
    let mut final_decision = None;
    let mut provided_by = HashMap::new();
    let mut line_deps = Vec::with_capacity(changes.len());
    for i in 0..changes.len() {
        line_deps.push(change_deps(i, &changes[i], &mut provided_by));
    }
    let mut deps: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut rev_deps: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..changes.len() {
        for dep in line_deps[i].iter() {
            debug!("provided: i {}, dep {:?}", i, dep);
            let p = provided_by.get(dep).unwrap();
            debug!("provided: p= {}", p);

            let e = deps.entry(i).or_insert(Vec::new());
            e.push(*p);

            let e = rev_deps.entry(*p).or_insert(Vec::new());
            e.push(i);
        }
    }
    let empty_deps = Vec::new();
    let mut current_file = None;
    while i < changes.len() {
        let decision=
            // If one of our dependencies has been unselected (with "n")
            if deps.get(&i)
            .unwrap_or(&empty_deps)
            .iter()
            .any(|x| { ! *(choices.get(x).unwrap_or(&true)) }) {
                Some(false)
            } else if rev_deps.get(&i).unwrap_or(&empty_deps)
            .iter().any(|x| { *(choices.get(x).unwrap_or(&false)) }) {
                // If we are a dependency of someone selected (with "y").
                Some(true)
            } else {
                None
            };
        let e = match decision {
            Some(true) => 'Y',
            Some(false) => 'N',
            None => {
                if let Some(d) = final_decision {
                    d
                } else {
                    debug!("changes: {:?}", changes[i]);
                    try!(print_change(&mut terminal, repository, &mut current_file, &changes[i]));
                    println!("");
                    print!("Shall I {} this change? [ynkad] ", direction.verb());
                    try!(stdout().flush());
                    match getch.getch().ok().and_then(|x| from_u32(x as u32)) {
                        Some(e) => {
                            println!("{}\n", e);
                            let e = e.to_uppercase().next().unwrap_or('\0');
                            match e {
                                'A' => {
                                    final_decision = Some('Y');
                                    'Y'
                                }
                                'D' => {
                                    final_decision = Some('N');
                                    'N'
                                }
                                e => e,
                            }
                        }
                        _ => '\0',
                    }
                }
            }
        };
        match e {
            'Y' => {
                choices.insert(i, direction.is_record());
                i += 1
            }
            'N' => {
                choices.insert(i, !direction.is_record());
                i += 1
            }
            'K' if i > 0 => {
                choices.remove(&i);
                i -= 1
            }
            _ => {}
        }
    }
    Ok(choices)
}

pub fn ask_authors() -> Result<Vec<String>, Error> {

    try!(std::io::stdout().flush());
    let mut rl = rustyline::Editor::<()>::new();
    let input = rl.readline("What is your name <and email address>? ")?;
    Ok(vec![input])
}


pub fn ask_patch_name() -> Result<String, Error> {
    try!(std::io::stdout().flush());
    let mut rl = rustyline::Editor::<()>::new();
    let input = rl.readline("What is the name of this patch? ")?;
    Ok(input)
}

pub fn ask_learn_ssh(host: &str, port: u16, fingerprint: &str) -> Result<bool, Error> {
    try!(std::io::stdout().flush());
    print!("The authenticity of host {:?}:{} cannot be established.\nThe fingerprint is {:?}.",
           host,
           port,
           fingerprint);
    let mut rl = rustyline::Editor::<()>::new();
    let input = rl.readline("Are you sure you want to continue (yes/no)? ")?;
    let input = input.to_uppercase();
    Ok(input.trim() == "YES")
}


pub fn print_status<T: rand::Rng>(repository: &MutTxn<T>,
                                  changes: &[Record]) -> Result<(), Error> {

    debug!("changes: {:?}", changes);
    let mut terminal = term::stdout().unwrap();
    let mut i = 0;
    let mut current_file = None;
    while i < changes.len() {
        debug!("changes: {:?}", changes[i]);
        try!(print_change(&mut terminal, repository, &mut current_file, &changes[i]));
        println!("");
        i += 1
    }
    Ok(())
}
