use toml;
use libpijul::fs_representation::meta_file;
use std::path::Path;
use error::Error;
use std::fs::File;
use std::io::{Read, Write};


#[derive(Debug, RustcEncodable, RustcDecodable)]
pub enum Repository {
    String(String),
    SSH { address: String, port: u16 },
}


#[derive(Debug, RustcEncodable, RustcDecodable)]
pub struct Meta {
    pub default_authors: Vec<String>,
    pub pull: Option<Repository>,
    pub push: Option<Repository>,
}

impl Meta {
    pub fn load(r: &Path) -> Result<Meta, Error> {
        let mut str = String::new();
        {
            let mut f = try!(File::open(meta_file(r)));
            try!(f.read_to_string(&mut str));
        }
        Ok(toml::decode_str(&str).unwrap())
    }
    pub fn new() -> Meta {
        Meta {
            default_authors: Vec::new(),
            pull: None,
            push: None,
        }
    }
    pub fn save(self, r: &Path) -> Result<(), Error> {
        let mut f = try!(File::create(meta_file(r)));
        let s: String = toml::encode_str(&self);
        try!(f.write_all(s.as_bytes()));
        Ok(())
    }
}
