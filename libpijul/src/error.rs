use std::io;
use std;
use std::fmt;
use std::path::PathBuf;
use bincode;
use sanakirja;
use backend;

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Sanakirja(sanakirja::Error),
    AlreadyApplied,
    AlreadyAdded,
    FileNotInRepo(PathBuf),
    Bincode(bincode::Error),
    NothingToDecode,
    InternalHashNotFound(backend::Hash),
    PatchNotFound(PathBuf, String),
    Utf8(std::str::Utf8Error),
    NoDb(backend::Root),
    ReadOnlyTransaction,
    WrongHash,
    BranchNameAlreadyExists,
    ChangesFile,
    PatchVersionMismatch(u64, u64)
}

impl Error {
    pub fn lacks_space(&self) -> bool {
        match *self {
            Error::Sanakirja(sanakirja::Error::NotEnoughSpace) => true,
            _ => false
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::Sanakirja(ref err) => write!(f, "Sanakirja error: {}", err),
            Error::AlreadyApplied => write!(f, "Patch already applied"),
            Error::AlreadyAdded => write!(f, "File already here"),
            Error::Bincode(ref err) => write!(f, "Bincode error {}", err),
            Error::NothingToDecode => write!(f, "Nothing to decode"),
            Error::FileNotInRepo(ref path) => write!(f, "File {} not tracked", path.display()),
            Error::InternalHashNotFound(ref hash) => {
                write!(f, "Internal hash {:?} not found", hash.as_ref())
            }
            Error::PatchNotFound(ref path, ref hash) => {
                write!(f, "Patch {} not found in {}", hash, path.display())
            }
            Error::Utf8(ref e) => write!(f, "Utf8 Error {:?}", e),
            Error::NoDb(ref e) => write!(f, "No root database {:?}", e),
            Error::ReadOnlyTransaction => write!(f, "Read-only transaction"),
            Error::WrongHash => write!(f, "Wrong patch hash"),
            Error::BranchNameAlreadyExists => write!(f, "Branch name already exists"),
            Error::ChangesFile => write!(f, "Invalid changes file"),
            Error::PatchVersionMismatch(a, b) => write!(f, "Patch version mismatch: this Pijul knows version {}, the patch is version {}", b, a),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref err) => err.description(),
            Error::Sanakirja(ref err) => err.description(),
            Error::AlreadyApplied => "Patch already applied",
            Error::AlreadyAdded => "File already here",
            Error::Bincode(ref err) => err.description(),
            Error::NothingToDecode => "Nothing to decode",
            Error::FileNotInRepo(_) => "Operation on untracked file",
            Error::InternalHashNotFound(_) => "Internal hash not found",
            Error::PatchNotFound(_, _) => "Patch not found",
            Error::Utf8(ref e) => e.description(),
            Error::NoDb(_) => "No root database",
            Error::ReadOnlyTransaction => "Read-only transaction",
            Error::WrongHash => "Wrong patch hash",
            Error::BranchNameAlreadyExists => "Branch name already exists",
            Error::ChangesFile => "Invalid changes file",
            Error::PatchVersionMismatch(_, _) => "Patch version mismatch",
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::Sanakirja(ref err) => Some(err),
            Error::AlreadyApplied => None,
            Error::AlreadyAdded => None,
            Error::Bincode(ref err) => Some(err),
            Error::NothingToDecode => None,
            Error::FileNotInRepo(_) => None,
            Error::InternalHashNotFound(_) => None,
            Error::PatchNotFound(_, _) => None,
            Error::Utf8(ref e) => Some(e),
            Error::NoDb(_) => None,
            Error::ReadOnlyTransaction => None,
            Error::WrongHash => None,
            Error::BranchNameAlreadyExists => None,
            Error::ChangesFile => None,
            Error::PatchVersionMismatch(_, _) => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<sanakirja::Error> for Error {
    fn from(err: sanakirja::Error) -> Error {
        Error::Sanakirja(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::Bincode(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}
