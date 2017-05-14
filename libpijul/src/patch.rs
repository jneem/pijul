use rustc_serialize::base64::{URL_SAFE, ToBase64};

use flate2;
use rand;
use chrono;
use chrono::{DateTime, UTC};
use std::path::Path;

use std::io::{Read, BufRead, Write};
use std::fs::{File, metadata};

use std::collections::HashSet;
use std::str::from_utf8;
use std::path::PathBuf;
use std::rc::Rc;
pub type Flag = u8;

use error::Error;

use bincode::{deserialize, deserialize_from, serialize_into, serialize, Infinite};

pub const PATCH_FORMAT_VERSION: u64 = 0;

#[derive(Debug, Serialize, Deserialize)]
pub struct Patch {
    pub version: u64,
    pub header: PatchHeader,
    pub dependencies: Vec<Hash>,
    pub changes: Vec<Change>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchHeader {
    pub authors: Vec<String>,
    pub name: String,
    pub description: Option<String>,
    pub timestamp: DateTime<UTC>,
}

use std::ops::{Deref, DerefMut};
impl Deref for Patch {
    type Target = PatchHeader;
    fn deref(&self) -> &Self::Target {
        &self.header
    }
}
impl DerefMut for Patch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}


/// Options are for when this edge is between vertices introduced by
/// the current patch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEdge {
    pub from: Key<Option<Hash>>,
    pub to: Key<Option<Hash>>,
    pub introduced_by: Option<Hash>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    NewNodes {
        up_context: Vec<Key<Option<Hash>>>,
        down_context: Vec<Key<Option<Hash>>>,
        flag: EdgeFlags,
        line_num: LineId,
        nodes: Vec<Vec<u8>>,
    },
    NewEdges { op: EdgeOp, edges: Vec<NewEdge> },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EdgeOp {
    Map {
        previous: EdgeFlags,
        flag: EdgeFlags,
    },
    Forget { previous: EdgeFlags },
    New { flag: EdgeFlags },
}

impl PatchHeader {
    /// Reads everything in this patch, but the actual contents.
    pub fn from_reader_nochanges<R:Read>(mut r: R) -> Result<PatchHeader, Error> {
        let version:u64 = deserialize_from(&mut r, Infinite)?;
        if version == PATCH_FORMAT_VERSION {
            Ok(deserialize_from(&mut r, Infinite)?)
        } else {
            Err(Error::PatchVersionMismatch(version, PATCH_FORMAT_VERSION))
        }
    }
}


/// Semantic groups of changes, for interface purposes.
#[derive(Debug)]
pub enum Record {
    FileMove {
        new_name: String,
        del: Change,
        add: Change,
    },
    FileDel { name: String, del: Change },
    FileAdd { name: String, add: Change },
    Change { file: Rc<PathBuf>, change: Change, conflict_reordering: Vec<Change> },
    Replace { file: Rc<PathBuf>, adds: Change, dels: Change, conflict_reordering: Vec<Change> },
}

pub struct RecordIter<R, C> {
    rec: Option<R>,
    extra: Option<C>,
}

impl IntoIterator for Record {
    type IntoIter = RecordIter<Record, Change>;
    type Item = Change;
    fn into_iter(self) -> Self::IntoIter {
        RecordIter {
            rec: Some(self),
            extra: None,
        }
    }
}

impl Record {
    pub fn iter(&self) -> RecordIter<&Record, &Change> {
        RecordIter {
            rec: Some(self),
            extra: None,
        }
    }
}

impl Iterator for RecordIter<Record, Change> {
    type Item = Change;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match rec {
                Record::FileMove { del, add, .. } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Record::FileDel { del: c, .. } |
                Record::FileAdd { add: c, .. } |
                Record::Change { change: c, .. } => Some(c),
                Record::Replace { adds, dels, .. } => {
                    self.extra = Some(adds);
                    Some(dels)
                },
            }
        } else {
            None
        }
    }
}

impl<'a> Iterator for RecordIter<&'a Record, &'a Change> {
    type Item = &'a Change;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match *rec {
                Record::FileMove { ref del, ref add, .. } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Record::FileDel { del: ref c, .. } |
                Record::FileAdd { add: ref c, .. } |
                Record::Change { change: ref c, .. } => Some(c),
                Record::Replace { ref adds, ref dels, .. } => {
                    self.extra = Some(adds);
                    Some(dels)
                }
            }
        } else {
            None
        }
    }
}

impl Patch {
    pub fn empty() -> Patch {
        Patch {
            version: PATCH_FORMAT_VERSION,
            header: PatchHeader {
                authors: vec![],
                name: "".to_string(),
                description: None,
                timestamp: chrono::UTC::now(),
            },
            dependencies: Vec::new(),
            changes: vec![],
        }
    }

    pub fn size_upper_bound(&self) -> usize {
        // General overhead for applying a patch; 8 pages.
        let mut size: usize = 1 << 15;
        for c in self.changes.iter() {
            match *c {
                Change::NewNodes { ref nodes, .. } => {
                    size += nodes.iter().map(|x| x.len()).sum();
                    size += nodes.len() * 2048 // + half a page
                },
                Change::NewEdges { ref edges, .. } => {
                    size += edges.len() * 2048
                }
            }
        }
        size
    }

    pub fn from_reader_compressed<R: BufRead>(r: &mut R) -> Result<(Hash, Vec<u8>, Patch), Error> {
        let mut rr = flate2::bufread::GzDecoder::new(r)?;
        let filename = Hash::from_base64(from_utf8(rr.header().filename().unwrap())?).unwrap();

        let mut buf = Vec::new();
        rr.read_to_end(&mut buf)?;

        // Checking the hash.
        let hash = Hash::of_slice(&buf);
        match (&filename, &hash) {
            (&Hash::Sha512(ref filename), &Hash::Sha512(ref hash)) if &filename.0[..] ==
                                                                      &hash.0[..] => {}
            _ => return Err(Error::WrongHash),
        }
        let patch = deserialize(&buf[..])?;

        Ok((filename, buf, patch))
    }

    pub fn to_writer(&self, w: &mut Write) -> Result<(), Error> {
        let mut e = flate2::write::GzEncoder::new(w, flate2::Compression::Best);
        serialize_into(&mut e, self, Infinite)?;
        Ok(())
    }
    pub fn save<P: AsRef<Path>>(&self, dir: P) -> Result<Hash, Error> {
        // Encoding to a buffer.
        let buf = serialize(self, Infinite)?;

        // Hashing the buffer.
        let hash = Hash::of_slice(&buf);

        // Writing to the file.
        let h = hash.to_base64(URL_SAFE);
        let mut path = dir.as_ref().join(&h);
        path.set_extension("gz");
        if metadata(&path).is_err() {
            debug!("save, path {:?}", path);
            let f = try!(File::create(&path));
            debug!("created");
            let mut w = flate2::GzBuilder::new()
                .filename(h.as_bytes())
                .write(f, flate2::Compression::Best);
            try!(w.write_all(&buf));
            try!(w.finish());
            debug!("saved");
        }
        Ok(hash)
    }
}


pub fn read_changes(r: &mut Read) -> Result<HashSet<(Hash, ApplyTimestamp)>, Error> {
    let mut s = String::new();
    r.read_to_string(&mut s)?;
    let mut result = HashSet::new();
    for l in s.lines() {
        let mut sp = l.split(':');
        match (sp.next().and_then(Hash::from_base64), sp.next().and_then(|s| s.parse().ok())) {
            (Some(h), Some(s)) => { result.insert((h, s)); }
            _ => {}
        }
    }
    Ok(result)
}

pub fn read_changes_from_file<P: AsRef<Path>>(changes_file: P)
                                              -> Result<HashSet<(Hash, ApplyTimestamp)>, Error> {
    let mut file = try!(File::open(changes_file));
    read_changes(&mut file)
}

impl<U: Transaction, R> T<U, R> {
    pub fn new_patch(&self,
                     branch: &Branch,
                     authors: Vec<String>,
                     name: String,
                     description: Option<String>,
                     timestamp: DateTime<UTC>,
                     changes: Vec<Change>)
                     -> Patch {
        let deps = self.dependencies(branch, changes.iter());
        Patch {
            version: PATCH_FORMAT_VERSION,
            header: PatchHeader {
                authors: authors,
                name: name,
                description: description,
                timestamp: timestamp,
            },
            dependencies: deps,
            changes: changes,
        }
    }


    pub fn dependencies<'a, I: Iterator<Item = &'a Change>>(&self,
                                                            branch: &Branch,
                                                            changes: I)
                                                            -> Vec<Hash> {
        let mut deps = HashSet::new();
        for ch in changes {
            match *ch {
                Change::NewNodes { ref up_context,
                                   ref down_context,
                                   line_num: _,
                                   flag: _,
                                   nodes: _ } => {
                    for c in up_context.iter().chain(down_context.iter()) {
                        match c.patch {
                            None | Some(Hash::None) => {}
                            Some(ref dep) => {
                                deps.insert(dep.clone());
                            }
                        }
                    }
                }
                Change::NewEdges { ref edges, ref op } => {
                    for e in edges {
                        match e.from.patch {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                deps.insert(h.clone());
                                match *op {
                                    EdgeOp::Map { flag, .. } |
                                    EdgeOp::New { flag }
                                    if flag.contains(DELETED_EDGE|PARENT_EDGE) => {
                                        // Add "known patches" to
                                        // allow identifying missing
                                        // contexts.
                                        let k = Key {
                                            patch: self.get_internal(h.as_ref())
                                                .unwrap().to_owned(),
                                            line: e.from.line.clone(),
                                        };
                                        self.edge_context_deps(branch, &k, &mut deps)
                                    }
                                    _ => {}
                                }
                            }
                        }
                        match e.to.patch {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                deps.insert(h.clone());
                                match *op {
                                    EdgeOp::Map { flag, .. } |
                                    EdgeOp::New { flag } if flag.contains(DELETED_EDGE) &&
                                                             !flag.contains(PARENT_EDGE) => {
                                        // Add "known patches" to
                                        // allow identifying
                                        // missing contexts.
                                        let k = Key {
                                            patch: self.get_internal(h.as_ref())
                                                .unwrap()
                                                .to_owned(),
                                            line: e.to.line.clone(),
                                        };
                                        self.edge_context_deps(branch, &k, &mut deps)
                                    }
                                    _ => {}
                                }
                            }
                        }
                        match e.introduced_by {
                            None | Some(Hash::None) => {}
                            Some(ref h) => {
                                deps.insert(h.clone());
                            }
                        }
                    }
                }
            }
        }
        deps.into_iter().collect()
    }

    fn edge_context_deps(&self, branch: &Branch, k: &Key<PatchId>, deps: &mut HashSet<Hash>) {
        for (_, edge) in self.iter_nodes(branch, Some((&k, None)))
            .take_while(|&(k_, e_)| k_ == k && e_.flag <= PSEUDO_EDGE) {

            let ext = self.get_external(&edge.dest.patch).unwrap().to_owned();
            deps.insert(ext);
        }
    }
}

use backend::*;
use sanakirja;


impl<A: sanakirja::Transaction, R> T<A, R> {
    /// Gets the external key corresponding to the given key, returning an
    /// owned vector. If the key is just a patch internal hash, it returns the
    /// corresponding external hash.
    pub fn external_key(&self, key: &Key<PatchId>) -> Option<Key<Option<Hash>>> {
        if key.patch == ROOT_PATCH_ID {
            Some(Key {
                line: key.line.clone(),
                patch: Some(Hash::None),
            })
        } else if let Some(patch) = self.get_external(&key.patch) {
            Some(Key {
                line: key.line.clone(),
                patch: Some(patch.to_owned()),
            })
        } else {
            None
        }
    }

    pub fn external_hash(&self, key: &PatchId) -> HashRef {
        // println!("internal key:{:?}",&key[0..HASH_SIZE]);
        if *key == ROOT_PATCH_ID {
            ROOT_HASH.as_ref()
        } else {

            match self.get_external(key) {
                Some(pv) => pv,
                None => {
                    println!("internal key or hash:{:?}", key);
                    panic!("external hash not found {:?} !", key)
                }
            }
        }
    }

    /// Create a new internal patch id, register it in the "external" and
    /// "internal" bases, and write the result in its second argument
    /// ("result").
    pub fn new_internal(&self, ext: HashRef) -> PatchId {
        let mut result = ROOT_PATCH_ID.clone();
        match ext {
            HashRef::None => return result,
            HashRef::Sha512(h) => result.clone_from_slice(&h[..PATCH_ID_SIZE]),
        }
        let mut first_random = PATCH_ID_SIZE;
        loop {
            if self.get_external(&result).is_none() {
                break;
            }
            if first_random > 0 {
                first_random -= 1
            };
            for x in &mut result[first_random..].iter_mut() {
                *x = rand::random()
            }
        }
        result
    }
}
