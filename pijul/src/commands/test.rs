extern crate tempdir;
extern crate env_logger;
extern crate rand;
extern crate walkdir;
use self::walkdir::{DirEntry, WalkDir, WalkDirIterator};
use self::rand::Rng;
use commands::{init, info, record, add, remove, pull, mv, revert};
use error;
use std::fs;
use std::path::PathBuf;
use std;
use std::io::prelude::*;
use self::rand::distributions::{IndependentSample, Range};
use libpijul;
use libpijul::DEFAULT_BRANCH;
use std::mem;

fn mk_tmp_repo() -> tempdir::TempDir {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    {
        let init_params = init::Params {
            location: Some(&dir.path()),
            allow_nested: false,
        };
        init::run(&init_params).unwrap();
    }
    dir
}

fn mk_tmp_repo_pair() -> (tempdir::TempDir, std::path::PathBuf, std::path::PathBuf) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = dir.path().join("a");
    let dir_b = dir.path().join("b");
    {
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        let init_params_a = init::Params {
            location: Some(&dir_a),
            allow_nested: false,
        };
        let init_params_b = init::Params {
            location: Some(&dir_b),
            allow_nested: false,
        };
        init::run(&init_params_a).unwrap();
        init::run(&init_params_b).unwrap();
    }
    (dir, dir_a, dir_b)
}

fn mk_tmp_repos(n: usize) -> (tempdir::TempDir, Vec<std::path::PathBuf>) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let mut v = Vec::new();
    for i in 0..n {
        let dir_a = dir.path().join(format!("{}", i));
        {
            fs::create_dir(&dir_a).unwrap();
            let init_params_a = init::Params {
                location: Some(&dir_a),
                allow_nested: false,
            };
            init::run(&init_params_a).unwrap();
        }
        v.push(dir_a)
    }
    (dir, v)
}

fn add_one_file(repo: &std::path::Path, file: &std::path::Path) -> Result<(), error::Error> {

    let add_params = add::Params {
        repository: Some(&repo),
        touched_files: vec![&file],
    };
    add::run(&add_params)
}

fn record_all(repo: &std::path::Path,
              name: Option<&str>)
              -> Result<Option<libpijul::Hash>, error::Error> {
    let record_params = record::Params {
        repository: Some(repo),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: name,
        branch: Some(DEFAULT_BRANCH),
    };
    debug!("record_all!");
    record::run(&record_params)
}

fn pull_all(from: &std::path::Path,
            from_branch: &str,
            to: &std::path::Path,
            to_branch: &str)
            -> Result<(), error::Error> {

    let pull_params = pull::Params {
        repository: Some(to),
        remote_id: Some(from.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
        local_branch: Some(to_branch),
        remote_branch: from_branch,
    };
    debug!("pull_all!");
    pull::run(&pull_params)

}

fn revert(repo: &std::path::Path) -> Result<(), error::Error> {

    let params = revert::Params {
        repository: Some(repo),
        yes_to_all: true,
        branch: Some(DEFAULT_BRANCH),
    };
    debug!("revert!");
    revert::run(&params)

}

#[test]
fn add_grandchild() -> () {
    let dir = mk_tmp_repo();
    let subdir = &dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let fpath = &subdir.join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }

    add_one_file(&dir.path(), &fpath).unwrap();
    match record_all(&dir.path(), Some("")).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    }
}


#[test]
fn info_only_in_repo() -> () {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let info_params = info::Params {
        repository: Some(&dir.path()),
        debug: false,
        inode: None,
    };
    match info::run(&info_params) {
        Err(error::Error::NotInARepository) => (),
        Ok(_) => panic!("getting info from a non-repository"),
        Err(_) => panic!("funky failure while getting info from a non-repository"),
    }
}

#[test]
fn add_only_in_repo() -> () {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let fpath = &dir.path().join("toto");
    let add_params = add::Params {
        repository: Some(&dir.path()),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params) {
        Err(error::Error::NotInARepository) => (()),
        Ok(_) => panic!("Wait, I can add in a non-repository???"),
        Err(_) => panic!("funky failure while adding a file into a non-repository"),
    }
}

#[test]
fn add_outside_repo() -> () {
    let repo_dir = mk_tmp_repo();
    let not_repo_dir = tempdir::TempDir::new("pijul_not_repo").unwrap();
    let fpath = &not_repo_dir.path().join("toto");
    fs::File::create(&fpath).unwrap();
    let add_params = add::Params {
        repository: Some(&repo_dir.path()),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params) {
        Err(error::Error::InvalidPath(ref p)) if p == fpath.to_str().unwrap() => (()),
        Ok(_) => panic!("Wait, I can add in a non-repository???"),
        Err(e) => {
            panic!("funky failure {} while adding a file into a non-repository",
                   e)
        }
    }
}

#[test]
fn init_creates_repo() -> () {
    let dir = mk_tmp_repo();
    let info_params = info::Params {
        repository: Some(&dir.path()),
        debug: false,
        inode: None,
    };
    info::run(&info_params).unwrap();
}

#[test]
fn init_nested_forbidden() {
    let dir = mk_tmp_repo();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let sub_init_params = init::Params {
        location: Some(&subdir),
        allow_nested: false,
    };
    match init::run(&sub_init_params) {
        Ok(_) => panic!("Creating a forbidden nested repository"),

        Err(error::Error::InARepository(_)) => (),
        Err(_) => panic!("Failed in a funky way while creating a nested repository"),
    }
}


#[test]
fn init_nested_allowed() {
    let dir = mk_tmp_repo();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let sub_init_params = init::Params {
        location: Some(&subdir),
        allow_nested: true,
    };
    init::run(&sub_init_params).unwrap()
}

#[test]
fn in_empty_dir_nothing_to_record() {
    let dir = mk_tmp_repo();

    match record_all(&dir.path(), Some("")).unwrap() {
        None => (),
        Some(_) => panic!("found something to record in an empty repository"),
    }
}

#[test]
fn with_changes_sth_to_record() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");

    let _ = create_file_random_content(fpath, "toto > ");

    add_one_file(&dir.path(), &fpath).unwrap();

    match record_all(&dir.path(), Some("")).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    }
}

#[test]
fn add_same_file_twice() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");

    let _ = create_file_random_content(fpath, "toto > ");

    add_one_file(&dir.path(), &fpath).unwrap();

    assert!(add_one_file(&dir.path(), &fpath).is_err())
}


#[test]
fn add_remove_nothing_to_record() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }

    let add_params = add::Params {
        repository: Some(&dir.path()),
        touched_files: vec![&fpath],
    };
    add::run(&add_params).unwrap();
    println!("added");
    remove::run(&add_params).unwrap();

    println!("removed");

    match record_all(&dir.path(), Some("")).unwrap() {
        None => (),
        Some(_) => panic!("add remove left a trace"),
    }
}

#[test]
fn no_remove_without_add() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }
    let rem_params = remove::Params {
        repository: Some(&dir.path()),
        touched_files: vec![&fpath],
    };
    match remove::run(&rem_params) {
        Ok(_) => panic!("inexistant file can be removed"),
        Err(error::Error::Repository(libpijul::error::Error::FileNotInRepo(_))) => (),
        Err(_) => panic!("funky error when trying to remove inexistant file"),
    }
}

#[test]
fn add_record_pull_stop() {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let fpath = &dir_a.join("toto");
    let text0 = create_file_random_content(&fpath, "toto >");

    add_one_file(&dir_a, &fpath).unwrap();

    match record_all(&dir_a, Some("add toto")).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    }

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    let fpath_b = &dir_b.join("toto");
    let metadata = fs::metadata(&fpath_b).unwrap();
    assert!(metadata.is_file());
    assert!(file_eq(&fpath_b, &text0));
    std::mem::drop(dir);
}

fn read_file(path: &std::path::Path) -> String {
    let mut f = fs::File::open(&path).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    s
}

fn file_eq_str(path: &std::path::Path, fulltext: &str) -> bool {
    let mut f = fs::File::open(&path).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    println!("{:?}, {:?}", fulltext, s);
    if fulltext == s { true } else { false }
}


fn file_eq(path: &std::path::Path, text: &[String]) -> bool {
    let mut fulltext = String::new();
    for line in text.iter() {
        fulltext.push_str(&line);
    }
    file_eq_str(path, &fulltext)
}

fn files_eq(a: &std::path::Path, b: &std::path::Path) -> bool {
    let mut a = fs::File::open(&a).unwrap();
    let mut s = String::new();
    a.read_to_string(&mut s).unwrap();
    file_eq_str(b, &s)
}

fn assert_working_dirs_eq(dir_a: &std::path::Path, dir_b: &std::path::Path) {

    fn is_dot_pijul(p: &DirEntry) -> bool {
        p.file_name().to_str() == Some(".pijul")
    }

    let paths_a: Vec<_> = WalkDir::new(&dir_a)
        .min_depth(1)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|e| !is_dot_pijul(e))
        .collect();
    let paths_b: Vec<_> = WalkDir::new(&dir_b)
        .min_depth(1)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|e| !is_dot_pijul(e))
        .collect();
    assert_eq!(paths_a.len(), paths_b.len());
    println!("Paths left: {:?}", paths_a);
    println!("Paths right: {:?}", paths_b);


    for (a, b) in paths_a.iter().zip(paths_b.iter()) {
        println!("{:?} {:?}", a, b);
        let a = a.as_ref().unwrap();
        let b = b.as_ref().unwrap();
        assert_eq!(a.file_name(), b.file_name());
        assert!(files_eq(a.path(), b.path()));
    }
}


#[test]
fn add_record_pull_edit_record_pull__() {
    add_record_pull_edit_record_pull_(false, true)
}

#[test]
fn add_record_pull_noedit_record_pull() {
    add_record_pull_edit_record_pull_(false, false)
}
#[test]
fn add_record_pull_edit_record_pull_from_empty() {
    add_record_pull_edit_record_pull_(true, true)
}

#[test]
fn add_record_pull_noedit_record_pull_from_empty() {
    add_record_pull_edit_record_pull_(true, false)
}

fn add_record_pull_edit_record_pull_(empty_file: bool, really_edit: bool) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    fs::create_dir(dir_a).unwrap();
    fs::create_dir(dir_b).unwrap();
    let init_params_a = init::Params {
        location: Some(&dir_a),
        allow_nested: false,
    };
    let init_params_b = init::Params {
        location: Some(&dir_b),
        allow_nested: false,
    };
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let fpath = &dir_a.join("toto");

    let text0 = if empty_file {
        Vec::new()
    } else {
        random_text("toto > ")
    };
    {
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }
    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&fpath],
    };
    add::run(&add_params).unwrap();

    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("nothing"),
        branch: Some(DEFAULT_BRANCH),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    }
    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
        local_branch: Some(DEFAULT_BRANCH),
        remote_branch: DEFAULT_BRANCH,
    };
    pull::run(&pull_params).unwrap();
    let text1 = if really_edit {
        edit(&text0, 5, 2)
    } else {
        text0.clone()
    };
    {
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text1.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("edit"),
        branch: Some(DEFAULT_BRANCH),
    };

    match record::run(&record_params).unwrap() {
        None if text0 != text1 => panic!("file edition is not going to be recorded"),
        _ => (),
    }
    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
        local_branch: Some(DEFAULT_BRANCH),
        remote_branch: DEFAULT_BRANCH,
    };
    pull::run(&pull_params).unwrap();

    let fpath_b = &dir_b.join("toto");
    let metadata = fs::metadata(&fpath_b).unwrap();
    println!("dir = {:?}", dir);
    std::mem::forget(dir);
    assert!(metadata.is_file());
    assert!(file_eq(&fpath_b, &text1));
}


#[test]
fn cannot_move_unadded_file() {
    let repo_dir = mk_tmp_repo();
    let mv_params = mv::Params {
        repository: Some(repo_dir.path()),
        movement: mv::Movement::FileToFile {
            from: PathBuf::from("toto"),
            to: PathBuf::from("titi"),
        },
    };
    match mv::run(&mv_params) {
        Err(error::Error::Repository(libpijul::error::Error::FileNotInRepo(ref s)))
            if s.as_path() == std::path::Path::new("toto") => (),
        Err(_) => panic!("funky error"),
        Ok(()) => panic!("Unexpectedly able to move unadded file"),
    }
}


fn edit(input: &[String], percent_add: usize, percent_del: usize) -> Vec<String> {
    let mut text = Vec::new();

    let mut rng = rand::thread_rng();
    let range = Range::new(0, 100);

    for i in input {
        if range.ind_sample(&mut rng) < percent_add {
            let mut s: String = rand::thread_rng().gen_ascii_chars().take(20).collect();
            s.push('\n');
            text.push(s)
        }
        if range.ind_sample(&mut rng) >= percent_del {
            text.push(i.clone())
        }
    }
    text
}

#[test]
fn move_to_file__() {
    move_to_file_(false)
}

#[test]
fn move_to_file_editing() {
    move_to_file_(true)
}

fn random_text(prefix: &str) -> Vec<String> {
    let mut text = Vec::new();
    for _ in 0..20 {
        let mut s: String = rand::thread_rng().gen_ascii_chars().take(20).collect();
        s.push('\n');
        s = prefix.to_string() + &s;
        text.push(s)
    }
    text
}

fn write_file_random_content(path: &std::path::Path,
                             prefix: &str,
                             open_options: &std::fs::OpenOptions)
                             -> Vec<String> {
    let text0 = random_text(prefix);
    {
        let mut file = open_options.open(&path).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    };
    text0
}

fn create_file_random_content(path: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut open_options = std::fs::OpenOptions::new();
    open_options.write(true).create(true);
    write_file_random_content(path, prefix, &open_options)
}


fn append_file_random_content(path: &std::path::Path, prefix: &str) -> Vec<String> {
    let mut open_options = std::fs::OpenOptions::new();
    open_options.append(true);
    write_file_random_content(path, prefix, &open_options)
}

fn move_to_file_(edit_file: bool) {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();
    {
        let toto_path = &dir_a.join("toto");

        let text0 = create_file_random_content(&toto_path, "");

        add_one_file(&dir_a, &toto_path).unwrap();
        match record_all(&dir_a, Some("add toto")).unwrap() {
            None => panic!("file add is not going to be recorded"),
            Some(_) => (),
        };


        let mv_params = mv::Params {
            repository: Some(&dir_a),
            movement: mv::Movement::FileToFile {
                from: PathBuf::from("toto"),
                to: PathBuf::from("titi"),
            },
        };
        mv::run(&mv_params).unwrap();

        println!("moved successfully");

        let text1 = if edit_file {
            edit(&text0, 0, 20)
        } else {
            text0.clone()
        };

        {
            let titi_path = &dir_a.join("titi");
            let mut file = fs::File::create(&titi_path).unwrap();
            for line in text1.iter() {
                println!("line={:?}", line);
                file.write_all(line.as_bytes()).unwrap();
            }
        }

        match record_all(&dir_a, Some("edition")).unwrap() {
            None if text0 != text1 => panic!("file edition is not going to be recorded"),
            _ => (),
        };

        assert!(record_all(&dir_a, Some("edition")).unwrap().is_none());

        println!("Checking the contents of {:?}", &dir_a);
        let paths: Vec<_> = fs::read_dir(&dir_a).unwrap().collect();
        println!("paths a = {:?}", paths);

        pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

        let paths: Vec<_> = fs::read_dir(&dir_b).unwrap().collect();
        println!("paths b = {:?}", paths);

        let fpath_b = dir_b.join("titi");

        {
            let mut f = fs::File::open(&fpath_b).unwrap();
            let mut s = String::new();
            f.read_to_string(&mut s).unwrap();
            let mut fulltext = String::new();
            for line in text1.iter() {
                fulltext.push_str(&line);
            }
            println!("{:?}\n{:?}", fulltext, s);


            std::mem::forget(dir);

            assert!(fulltext == s);
        }
    }
}

#[test]
fn move_to_dir() {
    move_to_dir_editing_(false, false)
}

#[test]
fn move_to_dir_edit() {
    move_to_dir_editing_(false, true)
}
#[test]
fn move_to_dir_empty() {
    move_to_dir_editing_(true, false)
}

#[test]
fn move_to_dir_edit_empty() {
    move_to_dir_editing_(true, true)
}


fn move_to_dir_editing_(empty_file: bool, edit_file: bool) {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();
    std::mem::forget(dir);
    let toto_path = &dir_a.join("toto");

    let text0 = if empty_file {
        Vec::new()
    } else {
        random_text("toto > ")
    };
    {
        let mut file = fs::File::create(&toto_path).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&toto_path],
    };
    add::run(&add_params).unwrap();

    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file add"),
        branch: Some(DEFAULT_BRANCH),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    };
    println!("record 1 done");
    let subdir_a = &dir_a.join("d");
    fs::create_dir(subdir_a).unwrap();
    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![subdir_a],
    };
    add::run(&add_params).unwrap();

    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("dir add"),
        branch: Some(DEFAULT_BRANCH),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    };
    println!("record 2 done");

    let mv_params = mv::Params {
        repository: Some(&dir_a),
        movement: mv::Movement::IntoDir {
            from: vec![PathBuf::from("toto")],
            to: PathBuf::from("d"),
        },
    };
    mv::run(&mv_params).unwrap();
    let text1 = if edit_file {
        edit(&text0, 0, 20)
    } else {
        text0.clone()
    };
    if edit_file {
        let toto_path = &dir_a.join("d").join("toto");
        let mut file = fs::File::create(&toto_path).unwrap();
        for line in text1.iter() {
            println!("line={:?}", line);
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    match record::run(&record_params).unwrap() {
        None => panic!("file move is not going to be recorded"),
        Some(_) => (),
    };
    assert!(record::run(&record_params).unwrap().is_none());
    let paths = fs::read_dir(&subdir_a).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
        local_branch: Some(DEFAULT_BRANCH),
        remote_branch: DEFAULT_BRANCH,
    };
    pull::run(&pull_params).unwrap();

    let subdir_b = &dir_b.join("d");

    let metadata = fs::metadata(&subdir_b).unwrap();
    assert!(metadata.is_dir());

    let paths = fs::read_dir(&dir_b).unwrap();

    println!("enumerating {:?}", &subdir_b);

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    println!("enumeration done");

    let fpath_b = &dir_b.join("d/toto");
    let metadata = fs::metadata(fpath_b).unwrap();
    assert!(metadata.is_file());

    // std::mem::drop(dir);
}

#[test]
fn add_edit_remove_pull() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");

    create_file_random_content(&toto_path, "A");

    add_one_file(&dir_a, &toto_path).unwrap();

    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file add"),
        branch: Some(DEFAULT_BRANCH),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(_) => (),
    };
    println!("done recording add of toto");

    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
        local_branch: Some(DEFAULT_BRANCH),
        remote_branch: DEFAULT_BRANCH,
    };
    std::mem::forget(tmp_dir);
    pull::run(&pull_params).unwrap();
    println!("pulled");
    let remove_params = remove::Params {
        repository: Some(&dir_b),
        touched_files: vec![std::path::Path::new("toto")],
    };
    remove::run(&remove_params).unwrap();
    println!("removed");

    let record_params = record::Params {
        repository: Some(&dir_b),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file remove"),
        branch: Some(DEFAULT_BRANCH),
    };
    println!("recording file remove!");
    match record::run(&record_params).unwrap() {
        None => panic!("file remove is not going to be recorded"),
        Some(_) => (),
    };
    println!("recorded file remove");
    assert!(record::run(&record_params).unwrap().is_none());
    println!("done recording add of toto");

    let pull_params = pull::Params {
        repository: Some(&dir_a),
        remote_id: Some(dir_b.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
        local_branch: Some(DEFAULT_BRANCH),
        remote_branch: DEFAULT_BRANCH,
    };
    pull::run(&pull_params).unwrap();
    println!("pulled again");
    match fs::metadata(toto_path) {
        Ok(_) => panic!("pulling a remove fails to delete the file"),
        Err(_) => (),
    }
}

#[test]
fn pull_merge_symmetric() {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();
    std::mem::forget(dir);

    debug!("dirs: {:?} {:?}", dir_a, dir_b);
    let toto_path = &dir_a.join("toto");

    create_file_random_content(toto_path, "A toto >");

    add_one_file(&dir_a, toto_path).unwrap();

    record_all(&dir_a, Some("add toto")).unwrap();
    assert!(record_all(&dir_a, Some("add toto")).unwrap().is_none());

    let titi_path = &dir_b.join("titi");

    create_file_random_content(titi_path, "B titi >");

    add_one_file(&dir_b, titi_path).unwrap();

    record_all(&dir_b, Some("add titi")).unwrap();
    assert!(record_all(&dir_b, Some("add titi")).unwrap().is_none());

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();
    debug!("dirs: {:?} {:?}", dir_a, dir_b);

    // assert!(files_eq(&toto_path, &dir_b.join("toto")));
    assert_eq!(read_file(&toto_path), read_file(&dir_b.join("toto")));
    assert_eq!(read_file(&titi_path), read_file(&dir_a.join("titi")));
}


#[test]
fn pull_conflict_add_add_symmetric() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();
    let tmp_dir = tmp_dir.into_path();
    println!("working in {:?}", tmp_dir);

    let toto_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_path, "A toto >");

    let _ = add_one_file(&dir_a, toto_path).unwrap();

    record_all(&dir_a, Some("add toto")).unwrap();
    assert!(record_all(&dir_a, Some("add toto")).unwrap().is_none());

    let toto_b_path = &dir_b.join("toto");

    let _ = create_file_random_content(toto_b_path, "B toto >");

    let _ = add_one_file(&dir_b, toto_b_path).unwrap();

    let _ = record_all(&dir_b, Some("add toto again")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();

    assert_working_dirs_eq(&dir_a, &dir_b);
}

#[test]
fn pull_conflict_edit_edit_symmetric() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();
    let tmp_dir = tmp_dir.into_path();
    println!("working in {:?}", tmp_dir);

    let toto_a_path = &dir_a.join("toto");

    let _ = fs::File::create(&toto_a_path).unwrap();
    let _ = add_one_file(&dir_a, &toto_a_path).unwrap();
    record_all(&dir_a, Some("add toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

    let _ = create_file_random_content(&toto_a_path, "A toto >");
    record_all(&dir_a, Some("A: fill toto")).unwrap();

    let toto_b_path = &dir_b.join("toto");
    assert!(&toto_b_path.exists());
    let _ = create_file_random_content(&toto_b_path, "B toto >");
    let _ = record_all(&dir_b, Some("B: fill toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();

    let toto_a = fs::File::open(&toto_a_path).unwrap();
    let mut lines = std::io::BufReader::new(toto_a).lines();
    assert!(lines.any(|l| l.as_ref().unwrap().trim() == libpijul::conflict::SEPARATOR.trim()));
    assert_working_dirs_eq(&dir_a, &dir_b);

}

#[test]
fn pull_conflict_edit_edit_with_context_symmetric() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();
    let tmp_dir = tmp_dir.into_path();
    println!("working in {:?}", tmp_dir);

    let toto_a_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_a_path, "CONTEXT >");
    let _ = add_one_file(&dir_a, toto_a_path).unwrap();
    record_all(&dir_a, Some("add toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

    let _ = append_file_random_content(toto_a_path, "A toto >");
    record_all(&dir_a, Some("A: fill toto")).unwrap();

    let toto_b_path = &dir_b.join("toto");
    assert!(&toto_b_path.exists());
    let _ = append_file_random_content(toto_b_path, "B toto >");
    let _ = record_all(&dir_b, Some("B: fill toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();

    let toto_a = fs::File::open(&toto_a_path).unwrap();
    let mut lines = std::io::BufReader::new(toto_a).lines();

    assert_working_dirs_eq(&dir_a, &dir_b);
    assert!(lines.any(|l| l.unwrap().trim() == libpijul::conflict::SEPARATOR.trim()));
}

#[test]
fn pull_zombie_lines() {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_path, "A toto > ");

    let _ = add_one_file(&dir_a, toto_path).unwrap();

    let _ = record_all(&dir_a, Some("add toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

    // In a, empty toto

    {
        fs::File::create(&toto_path).unwrap();
    }

    let _ = record_all(&dir_a, Some("empty toto")).unwrap();

    // In b, add lines to the end of toto

    let toto_b_path = dir_b.join("toto");

    {
        let mut toto_b = fs::OpenOptions::new()
            .append(true)
            .open(&toto_b_path)
            .unwrap();
        toto_b.write(b"coucou").unwrap();
    }

    record_all(&dir_b, Some("adding soon-to-be zombie lines")).unwrap();

    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();
    debug!("applying patch from dir_a: {:?} to dir_b: {:?}",
           dir_a,
           dir_b);
    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    std::mem::forget(dir);
    assert!(files_eq(&toto_path, &toto_b_path));
}

#[test]
fn pull_30_patches() {
    let (dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");
    {
        fs::File::create(&toto_path).unwrap();
    }
    let _ = add_one_file(&dir_a, &toto_path).unwrap();
    let _ = record_all(&dir_a, Some("")).unwrap();

    for i in 0..30 {
        let _ = create_file_random_content(&toto_path, &format!("toto v{} > ", &i));
        let _ = record_all(&dir_a, Some(&format!("edit #{}", &i))).unwrap();
    }

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    println!("checking a vs b");
    assert!(files_eq(&toto_path, &dir_b.join("toto")));
    mem::drop(dir);
}


#[test]
fn add_record_edit_record() {
    let dir = mk_tmp_repo();
    {
        let fpath = &dir.path().join("toto");
        {
            fs::File::create(&fpath).unwrap();
        }

        let add_params = add::Params {
            repository: Some(&dir.path()),
            touched_files: vec![&fpath],
        };
        add::run(&add_params).unwrap();

        println!("added");

        let _ = create_file_random_content(&fpath, "");

        match record_all(&dir.path(), Some("")).unwrap() {
            None => panic!("file filling will not be recorded"),
            Some(_) => (),
        }

        create_file_random_content(&fpath, "");
        {
            let mut v = Vec::new();
            let mut f = fs::File::open(&fpath).unwrap();
            f.read_to_end(&mut v).unwrap();
            debug!("v = {:?}", v);
        }


        match record_all(&dir.path(), Some("")).unwrap() {
            None => panic!("file editing will not be recorded"),
            Some(_) => (),
        }
        {
            let mut v = Vec::new();
            let mut f = fs::File::open(&fpath).unwrap();
            f.read_to_end(&mut v).unwrap();
            debug!("{:?} v = {:?}", fpath, v);
        }

        fs::File::create(&fpath).unwrap();
        match record_all(&dir.path(), Some("")).unwrap() {
            None => panic!("file emptying will not be recorded"),
            Some(_) => (),
        }
    }
    std::mem::forget(dir);
}



#[test]
fn missing_context() {
    let (dir, dirs) = mk_tmp_repos(3);
    println!("dir = {:?}", dir);
    let dir_a = &dirs[0];
    let dir_b = &dirs[1];
    let dir_c = &dirs[2];

    let toto_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_path, "A toto > ");

    let _ = add_one_file(&dir_a, toto_path).unwrap();

    let _ = record_all(&dir_a, Some("add toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    pull_all(&dir_a, DEFAULT_BRANCH, &dir_c, DEFAULT_BRANCH).unwrap();

    // In a, empty toto

    {
        fs::File::create(&toto_path).unwrap();
    }

    let _ = record_all(&dir_a, Some("empty toto")).unwrap();

    // In b, add lines to the end of toto

    let toto_b_path = dir_b.join("toto");

    {
        let mut toto_b = fs::OpenOptions::new()
            .append(true)
            .open(&toto_b_path)
            .unwrap();
        toto_b.write(b"coucou\n").unwrap();
    }

    record_all(&dir_b, Some("adding soon-to-be zombie lines in B")).unwrap();

    let toto_c_path = dir_c.join("toto");

    {
        let mut toto_c = fs::OpenOptions::new()
            .append(true)
            .open(&toto_c_path)
            .unwrap();
        toto_c.write(b"cuicui\n").unwrap();
    }

    record_all(&dir_c, Some("adding soon-to-be zombie lines in C")).unwrap();
    std::mem::forget(dir);

    debug!("\n\nApplying from B to A\n");
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();
    debug!("\n\nApplying from C to A\n");
    pull_all(&dir_c, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();

    debug!("\n\nApplying from A to B\n");
    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();
    debug!("\n\nApplying from A to C\n");
    pull_all(&dir_a, DEFAULT_BRANCH, &dir_c, DEFAULT_BRANCH).unwrap();
    debug!("applying patches: a: {:?}, b: {:?}, c: {:?}",
           dir_a,
           dir_b,
           dir_c);

    revert(&dir_a).unwrap();
    revert(&dir_b).unwrap();
    revert(&dir_c).unwrap();

    assert!(files_eq(&toto_path, &toto_b_path));
    assert!(files_eq(&toto_path, &toto_c_path));
}


#[test]
fn record_with_conflicts() {
    let (dir, dirs) = mk_tmp_repos(2);
    println!("dir = {:?}", dir);
    let dir_a = &dirs[0];
    let dir_b = &dirs[1];

    let toto_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_path, "A toto > ");

    let _ = add_one_file(&dir_a, toto_path).unwrap();

    let _ = record_all(&dir_a, Some("add toto")).unwrap();

    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

    // In a, add lines to the end of toto

    let toto_a_path = dir_a.join("toto");

    {
        let mut toto_a = fs::OpenOptions::new()
            .append(true)
            .open(&toto_a_path)
            .unwrap();
        toto_a.write(b"coucou\n").unwrap();
    }

    record_all(&dir_a, Some("coucou (A)")).unwrap();

    let toto_b_path = dir_b.join("toto");
    // In b add other lines to the end of toto.
    {
        let mut toto_b = fs::OpenOptions::new()
            .append(true)
            .open(&toto_b_path)
            .unwrap();
        toto_b.write(b"cuicui\n").unwrap();
    }
    record_all(&dir_b, Some("cuicui (B)")).unwrap();


    std::mem::forget(dir);

    debug!("\n\nApplying from B to A\n");
    pull_all(&dir_b, DEFAULT_BRANCH, &dir_a, DEFAULT_BRANCH).unwrap();
    debug!("\n\nApplying from A to B\n");
    pull_all(&dir_a, DEFAULT_BRANCH, &dir_b, DEFAULT_BRANCH).unwrap();

    debug!("applying patches: a: {:?}, b: {:?}", dir_a, dir_b);

    revert(&dir_a).unwrap();
    revert(&dir_b).unwrap();

    // Should be empty.
    let new_patch_hash = record_all(&dir_a, Some("should not exist")).unwrap();
    assert!(new_patch_hash.is_none());
    let new_patch_hash = record_all(&dir_b, Some("should not exist")).unwrap();
    assert!(new_patch_hash.is_none());
}
