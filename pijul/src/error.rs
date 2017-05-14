use std::io;
use std::error;
use std::fmt;
use std::string;
use std::path;
use {thrussh, libpijul, rustc_serialize, hyper, rustyline, term};
// use toml;

#[derive(Debug)]
pub enum Error {
    NotInARepository,
    InARepository(path::PathBuf),
    IO(io::Error),
    Rustyline(rustyline::error::ReadlineError),
    Term(term::Error),
    Repository(libpijul::error::Error),
    UTF8(string::FromUtf8Error),
    Hex(rustc_serialize::hex::FromHexError),
    SSH(thrussh::Error),
    Hyper(hyper::error::Error),
    // TomlDe(toml::de::Error),
    // TomlSer(toml::ser::Error),
    MetaDecoding,
    MissingRemoteRepository,
    PatchNotFound(String, libpijul::Hash),
    InvalidPath(String),
    WrongHash,
    BranchAlreadyExists,
    CannotDeleteCurrentBranch,
    NoSuchBranch,
    IsDirectory,
}

impl Error {
    pub fn lacks_space(&self) -> bool {
        match *self {
            Error::Repository(ref r) => r.lacks_space(),
            _ => false
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::InARepository(ref p) => write!(f, "Error: inside repository {}", p.display()),
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::Rustyline(ref err) => write!(f, "Rustyline error: {}", err),
            Error::Term(ref err) => write!(f, "Term error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err),
            Error::SSH(ref err) => write!(f, "SSH: {}", err),
            Error::Hex(ref err) => write!(f, "Hex: {}", err),
            Error::Hyper(ref err) => write!(f, "Hyper: {}", err),
            Error::UTF8(ref err) => write!(f, "UTF8Error: {}", err),
            Error::MetaDecoding => write!(f, "MetaDecoding"),
            Error::MissingRemoteRepository => write!(f, "Missing remote repository"),
            Error::PatchNotFound(ref path, ref hash) => {
                write!(f, "Patch {:?} not found in {}", hash, path)
            }
            Error::InvalidPath(ref p) => write!(f, "Invalid path {}", p),
            Error::WrongHash => write!(f, "Wrong hash"),
            Error::BranchAlreadyExists => write!(f, "Branch already exists"),
            Error::CannotDeleteCurrentBranch => write!(f, "Cannot delete current branch"),
            Error::NoSuchBranch => write!(f, "No such branch"),
            Error::IsDirectory => write!(f, "Is a directory"),
            // Error::TomlDe(ref e) => write!(f, "Toml de err: {}", e),
            // Error::TomlSer(ref e) => write!(f, "Toml ser err: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "Not in a repository",
            Error::InARepository(_) => "In a repository",
            Error::IO(ref err) => err.description(),
            Error::Rustyline(ref err) => err.description(),
            Error::Term(ref err) => err.description(),
            Error::Repository(ref err) => err.description(),
            Error::SSH(ref err) => err.description(),
            Error::Hex(ref err) => err.description(),
            Error::Hyper(ref err) => err.description(),
            Error::UTF8(ref err) => err.description(),
            Error::MetaDecoding => "Error in the decoding of metadata",
            Error::MissingRemoteRepository => "Missing remote repository",
            Error::PatchNotFound(_, _) => "Patch not found",
            Error::InvalidPath(_) => "Invalid path",
            Error::WrongHash => "Wrong hash",
            Error::BranchAlreadyExists => "Branch already exists",
            Error::CannotDeleteCurrentBranch => "Cannot delete current branch",
            Error::NoSuchBranch => "No such branch",
            Error::IsDirectory => "Is a directory",
            // Error::TomlDe(ref e) => e.description(),
            // Error::TomlSer(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::Rustyline(ref err) => Some(err),
            Error::Term(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::NotInARepository => None,
            Error::InARepository(_) => None,
            Error::SSH(ref err) => Some(err),
            Error::Hex(ref err) => Some(err),
            Error::Hyper(ref err) => Some(err),
            Error::UTF8(ref err) => Some(err),
            Error::MetaDecoding => None,
            Error::MissingRemoteRepository => None,
            Error::PatchNotFound(_, _) => None,
            Error::InvalidPath(_) => None,
            Error::WrongHash => None,
            Error::BranchAlreadyExists => None,
            Error::CannotDeleteCurrentBranch => None,
            Error::NoSuchBranch => None,
            Error::IsDirectory => None,
            // Error::TomlDe(ref err) => Some(err),
            // Error::TomlSer(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<thrussh::Error> for Error {
    fn from(err: thrussh::Error) -> Error {
        Error::SSH(err)
    }
}

impl From<thrussh::HandlerError<Error>> for Error {
    fn from(err: thrussh::HandlerError<Error>) -> Error {
        match err {
            thrussh::HandlerError::Handler(e) => e,
            thrussh::HandlerError::Error(e) => Error::SSH(e)
        }
    }
}

impl From<libpijul::error::Error> for Error {
    fn from(err: libpijul::error::Error) -> Error {
        Error::Repository(err)
    }
}
impl From<string::FromUtf8Error> for Error {
    fn from(err: string::FromUtf8Error) -> Error {
        Error::UTF8(err)
    }
}
impl From<rustc_serialize::hex::FromHexError> for Error {
    fn from(err: rustc_serialize::hex::FromHexError) -> Error {
        Error::Hex(err)
    }
}
impl From<hyper::error::Error> for Error {
    fn from(err: hyper::error::Error) -> Error {
        Error::Hyper(err)
    }
}

impl From<Error> for thrussh::HandlerError<Error> {
    fn from(e: Error) -> thrussh::HandlerError<Error> {
        thrussh::HandlerError::Handler(e)
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(e: rustyline::error::ReadlineError) -> Error {
        Error::Rustyline(e)
    }
}

impl From<term::Error> for Error {
    fn from(e: term::Error) -> Error {
        Error::Term(e)
    }
}
/*
impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Error {
        Error::TomlDe(e)
    }
}

impl From<toml::ser::Error> for Error {
    fn from(e: toml::ser::Error) -> Error {
        Error::TomlSer(e)
    }
}
*/
