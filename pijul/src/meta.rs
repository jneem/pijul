use app_dirs::{app_root, get_app_root, AppDataType, AppInfo};
use libpijul::fs_representation;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Read, Write};
use toml;

use error::Error;

// This information is used to determine where the global config file lives. The `author` field
// only matters on Windows.
const APP_INFO: AppInfo = AppInfo {
    name: "pijul",
    author: "Pijul Developers",
};

const META_FILE_NAME: &'static str = "meta.toml";
const GLOBAL_META_FILE_NAME: &'static str = "global.toml";

#[derive(Debug, Deserialize, Serialize)]
pub enum Repository {
    String(String),
    SSH { address: String, port: u16 },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Meta {
    pub default_authors: Vec<String>,
    pub pull: Option<Repository>,
    pub push: Option<Repository>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GlobalMeta {
    pub default_authors: Vec<String>,
}

impl GlobalMeta {
    fn new() -> GlobalMeta {
        GlobalMeta { default_authors: Vec::new() }
    }

    fn load() -> Result<GlobalMeta, Error> {
        let dir = get_app_root(AppDataType::UserConfig, &APP_INFO);
        if let Ok(mut path) = dir {
            path.push(GLOBAL_META_FILE_NAME);
            // It isn't an error if the global meta file doesn't exist, but it is an error if we
            // fail to read it or it fails to parse.
            if path.exists() {
                let mut s = String::new();
                File::open(path)?.read_to_string(&mut s)?;
                return Ok(toml::from_str(&s)?);
            }
        }
        Ok(GlobalMeta::new())
    }

    fn save(&self) -> Result<(), Error> {
        let mut path = app_root(AppDataType::UserConfig, &APP_INFO)?;
        path.push(GLOBAL_META_FILE_NAME);
        File::create(path)?.write_all(toml::to_string(self)?.as_bytes())?;
        Ok(())
    }

    /// Modifies the global configuration file with a new value for `default_authors`.
    pub fn save_default_authors(authors: &Vec<String>) -> Result<(), Error> {
        let mut meta = GlobalMeta::load().unwrap_or(GlobalMeta::new());
        meta.default_authors = authors.clone();
        meta.save()
    }
}

impl Meta {
    fn new() -> Meta {
        Meta {
            default_authors: Vec::new(),
            push: None,
            pull: None,
        }
    }

    /// Loads pijul's configuration, given the root of the repository.
    ///
    /// This function reads the global configuration (stored in the user's home directory) and the
    /// local configuration (stored at the root of the repository). It then merges them, giving
    /// priority to the local configuration.
    ///
    /// Note that this function never fails: if it runs into a problem loading the file, it just
    /// prints a warning message and continues.
    pub fn load(repo_root: &Path) -> Meta {
        let mut meta = Meta::load_local(repo_root).unwrap_or_else(|e| {
            warn!("Couldn't load repository metadata: {}", e);
            Meta::new()
        });
        meta.merge_global_meta();
        meta
    }

    fn path(repo_root: &Path) -> PathBuf {
        let mut path = fs_representation::repo_dir(repo_root);
        path.push(META_FILE_NAME);
        path
    }

    // Load the repository-local meta file.
    fn load_local(repo_root: &Path) -> Result<Meta, Error> {
        let mut s = String::new();
        File::open(Meta::path(repo_root))?.read_to_string(&mut s)?;
        Ok(toml::from_str(&s)?)
    }

    // Merge in values from the global meta file.
    fn merge_global_meta(&mut self) {
        let global = GlobalMeta::load().unwrap_or_else(|e| {
            warn!("Couldn't load global metadata: {}", e);
            GlobalMeta::new()
        });
        if self.default_authors.is_empty() {
            self.default_authors = global.default_authors
        }
    }

    fn save(&self, repo_root: &Path) -> Result<(), Error> {
        File::create(Meta::path(repo_root))?
            .write_all(toml::to_string(self)?.as_bytes())?;
        Ok(())
    }

    /// Modifies the local configuration file with a new value for `default_authors`.
    pub fn save_default_authors(repo_root: &Path, authors: &Vec<String>) -> Result<(), Error> {
        let mut meta = Meta::load_local(repo_root).unwrap_or(Meta::new());
        meta.default_authors = authors.clone();
        meta.save(repo_root)
    }

    /// Modifies the local configuration file with a new value for `push`.
    pub fn save_push(repo_root: &Path, push: Repository) -> Result<(), Error> {
        let mut meta = Meta::load_local(repo_root).unwrap_or(Meta::new());
        meta.push = Some(push);
        meta.save(repo_root)
    }

    /// Modifies the local configuration file with a new value for `pull`.
    pub fn save_pull(repo_root: &Path, pull: Repository) -> Result<(), Error> {
        let mut meta = Meta::load_local(repo_root).unwrap_or(Meta::new());
        meta.pull = Some(pull);
        meta.save(repo_root)
    }

    /// Prints the location of the various configuration files, so the user knows where to find
    /// them.
    pub fn print_meta_info(repo_root: &Path) {
        let local_path = Meta::path(repo_root);
        if local_path.exists() {
            println!("The repository-wide configuration is in {}.", local_path.display());
        }
        if let Ok(mut path) = get_app_root(AppDataType::UserConfig, &APP_INFO) {
            path.push(GLOBAL_META_FILE_NAME);
            if path.exists() {
                println!("The system-wide configuration is in {}.", path.display());
            }
        }
    }
}
