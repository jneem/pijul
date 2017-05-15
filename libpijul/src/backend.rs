use error::Error;
use sanakirja;
pub use sanakirja::Transaction;
use sanakirja::Representable;
use std::path::Path;
use rand;
use std;

mod patch_id {
    use sanakirja::{Representable, Alignment};
    use std;
    // Patch Identifiers.
    pub const PATCH_ID_SIZE: usize = 8;
    pub const ROOT_PATCH_ID: PatchId = PatchId([0; PATCH_ID_SIZE]);

    #[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
    pub struct PatchId([u8; PATCH_ID_SIZE]);

    impl std::fmt::Debug for PatchId {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(fmt, "PatchId 0x{}", self.0.to_hex())
        }
    }

    use rustc_serialize::hex::ToHex;
    impl ToHex for PatchId {
        fn to_hex(&self) -> String {
            self.0.to_hex()
        }
    }
    impl PatchId {
        pub fn new() -> Self {
            PatchId([0; PATCH_ID_SIZE])
        }
    }

    impl std::ops::Deref for PatchId {
        type Target = [u8];
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl std::ops::DerefMut for PatchId {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl Representable for PatchId {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            std::mem::size_of::<PatchId>() as u16
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self as *const PatchId, p as *mut PatchId, 1)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            let mut ret = PatchId::new();
            std::ptr::copy(p as *const PatchId, &mut ret as *mut PatchId, 1);
            ret
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            self.cmp(&x)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::patch_id::*;

pub mod key {
    use sanakirja::{Representable, Alignment};
    use std;
    use super::patch_id::*;

    const LINE_ID_SIZE: usize = 8;
    pub const KEY_SIZE: usize = PATCH_ID_SIZE + LINE_ID_SIZE;
    pub const ROOT_KEY: Key<PatchId> = Key {
        patch: ROOT_PATCH_ID,
        line: LineId([0; LINE_ID_SIZE]),
    };

    use rustc_serialize::hex::{ToHex};
    impl ToHex for Key<PatchId> {
        fn to_hex(&self) -> String {
            self.patch.to_hex() + &self.line.0.to_hex()
        }
    }


    impl Key<PatchId> {
        pub fn from_hex(hex: &str) -> Option<Self> {
            use std::ascii::AsciiExt;
            let mut s = [0; KEY_SIZE];
            let hex = hex.as_bytes();
            if hex.len() <= 2 * s.len() {
                let mut i = 0;

                while i < hex.len() {
                    let h = hex[i].to_ascii_lowercase();
                    if h >= b'0' && h <= b'9' {

                        s[i/2] = s[i/2] << 4 | (h - b'0')

                    } else if h >= b'a' && h <= b'f' {

                        s[i/2] = s[i/2] << 4 | (h - b'a' + 10)

                    } else {
                        return None
                    }
                    i += 1
                }
                if i & 1 == 1 {
                    s[i/2] = s[i/2] << 4
                }
                unsafe {
                    Some(std::mem::transmute(s))
                }
            } else {
                None
            }
        }
    }

    // A LineId contains a counter encoded little-endian, so that it
    // can both be deterministically put into a Sanakirja database,
    // and passed to standard serializers.
    #[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
    pub struct LineId([u8; LINE_ID_SIZE]);


    impl std::fmt::Debug for LineId {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(fmt, "LineId(0x{})", self.0.to_hex())
        }
    }

    impl LineId {
        /// Creates a new `LineId`, initialized to 0.
        pub fn new() -> LineId {
            LineId([0; LINE_ID_SIZE])
        }
        pub fn is_root(&self) -> bool {
            self.0.iter().all(|x| *x == 0)
        }
    }
    use byteorder::{ByteOrder, LittleEndian};
    impl std::ops::Add<usize> for LineId {
        type Output = LineId;
        fn add(self, x: usize) -> Self::Output {
            let a = LittleEndian::read_u64(&self.0);
            let mut b = LineId::new();
            LittleEndian::write_u64(&mut b.0, a + x as u64);
            b
        }
    }
    impl std::ops::AddAssign<usize> for LineId {
        fn add_assign(&mut self, x: usize) {
            *self = self.clone() + x
        }
    }

    #[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
    #[repr(packed)]
    pub struct Key<H> {
        pub patch: H,
        pub line: LineId,
    }
    impl<T> AsRef<LineId> for Key<T> {
        fn as_ref(&self) -> &LineId {
            &self.line
        }
    }
    impl Key<PatchId> {
        pub fn to_unsafe(&self) -> UnsafeKey {
            UnsafeKey(self)
        }
        pub unsafe fn from_unsafe<'a>(p: UnsafeKey) -> &'a Self {
            &*p.0
        }
    }
    impl<T: Clone> Key<Option<T>> {
        pub fn unwrap_patch(&self) -> Key<T> {
            Key {
                patch: self.patch.as_ref().unwrap().clone(),
                line: self.line.clone(),
            }
        }
    }
    #[derive(Clone, Copy)]
    pub struct UnsafeKey(*const Key<PatchId>);

    impl std::fmt::Debug for UnsafeKey {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            unsafe {
                Key::from_unsafe(*self).fmt(fmt)
            }
        }
    }


    impl Representable for UnsafeKey {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            let size = std::mem::size_of::<Key<PatchId>>();
            debug_assert_eq!(size, PATCH_ID_SIZE + LINE_ID_SIZE);
            size as u16
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self.0, p as *mut Key<PatchId>, 1)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeKey(p as *const Key<PatchId>)
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a: &Key<PatchId> = Key::from_unsafe(*self);
            let b: &Key<PatchId> = Key::from_unsafe(x);
            a.cmp(b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::key::*;


mod edge {
    use super::key::*;
    use super::patch_id::*;
    use sanakirja::*;
    use std;

    bitflags! {
        #[derive(Serialize, Deserialize)]
        pub flags EdgeFlags: u8 {
            const PSEUDO_EDGE = 1,
            const FOLDER_EDGE = 2,
            const PARENT_EDGE = 4,
            const DELETED_EDGE = 8,
        }
    }

    #[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
    #[repr(packed)]
    pub struct Edge {
        pub flag: EdgeFlags,
        pub dest: Key<PatchId>,
        pub introduced_by: PatchId,
    }
    impl Edge {
        pub fn zero(flag: EdgeFlags) -> Edge {
            Edge {
                flag: flag,
                dest: ROOT_KEY.clone(),
                introduced_by: ROOT_PATCH_ID.clone(),
            }
        }
        pub fn to_unsafe(&self) -> UnsafeEdge {
            UnsafeEdge(self)
        }
        pub unsafe fn from_unsafe<'a>(p: UnsafeEdge) -> &'a Edge {
            &*p.0
        }
    }

    #[derive(Clone, Copy)]
    pub struct UnsafeEdge(*const Edge);

    impl std::fmt::Debug for UnsafeEdge {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            unsafe {
                Edge::from_unsafe(*self).fmt(fmt)
            }
        }
    }

    impl Representable for UnsafeEdge {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            std::mem::size_of::<Edge>() as u16
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self.0, p as *mut Edge, 1)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeEdge(p as *const Edge)
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a: &Edge = &*self.0;
            let b: &Edge = &*x.0;
            a.cmp(b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::edge::*;

mod hash {
    use sanakirja::{Representable, Alignment};
    use std;
    use rustc_serialize::base64::{ToBase64, FromBase64, Config, URL_SAFE};
    use ring;

    use serde::de::{Deserialize, Deserializer, Visitor};
    use serde::ser::{Serialize, Serializer};
    use serde;

    const SHA512_BYTES: usize = 512 / 8;

    #[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
    pub enum Hash {
        None,
        Sha512(Sha512)
    }

    pub struct Sha512(pub [u8; SHA512_BYTES]);

    impl PartialEq for Sha512 {
        fn eq(&self, h: &Sha512) -> bool {
            (&self.0[..]).eq(&h.0[..])
        }
    }
    impl Eq for Sha512 {}
    impl PartialOrd for Sha512 {
        fn partial_cmp(&self, h: &Sha512) -> Option<std::cmp::Ordering> {
            (&self.0[..]).partial_cmp(&h.0[..])
        }
    }
    impl Ord for Sha512 {
        fn cmp(&self, h: &Sha512) -> std::cmp::Ordering {
            (&self.0[..]).cmp(&h.0[..])
        }
    }

    impl std::hash::Hash for Sha512 {
        fn hash<H:std::hash::Hasher>(&self, h: &mut H) {
            (&self.0[..]).hash(h)
        }
    }

    struct Sha512Visitor;
    impl<'a> Visitor<'a> for Sha512Visitor {

        type Value = Sha512;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "A byte slice of length {}", SHA512_BYTES)
        }

        fn visit_bytes<E:serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            let mut x: [u8; SHA512_BYTES] = [0; SHA512_BYTES];
            x.copy_from_slice(v);
            Ok(Sha512(x))
        }

    }

    impl<'a> Deserialize<'a> for Sha512 {
        fn deserialize<D:Deserializer<'a>>(d: D) -> Result<Sha512, D::Error> {
            d.deserialize_bytes(Sha512Visitor)
        }
    }

    impl Serialize for Sha512 {
        fn serialize<S:Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(&self.0[..])
        }
    }


    impl std::fmt::Debug for Sha512 {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            (&self.0[..]).fmt(fmt)
        }
    }
    impl<'a> std::fmt::Debug for HashRef<'a> {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            match *self {
                HashRef::None => write!(fmt, "None"),
                HashRef::Sha512(e) => write!(fmt, "Sha512({})", e.to_base64(URL_SAFE)),
            }
        }
    }


    #[derive(Copy, Clone, Hash, Eq, Ord, PartialEq, PartialOrd, RustcEncodable)]
    pub enum HashRef<'a> {
        None,
        Sha512(&'a [u8]),
    }

    impl Hash {

        pub fn from_base64(base64: &str) -> Option<Self> {
            if let Ok(v) = base64.from_base64() {
                if v.len() == 0 {
                    None
                } else {
                    if v[0] == Algorithm::Sha512 as u8 && v.len() == 1 + SHA512_BYTES {
                        let mut hash = [0; SHA512_BYTES];
                        hash.clone_from_slice(&v[1..]);
                        Some(Hash::Sha512(Sha512(hash)))
                    } else if v[0] == Algorithm::None as u8 {
                        Some(Hash::None)
                    } else {
                        None
                    }
                }
            } else {
                None
            }
        }

        pub fn as_ref(&self) -> HashRef {
            match *self {
                Hash::None => HashRef::None,
                Hash::Sha512(ref e) => {
                    HashRef::Sha512(unsafe {
                        std::slice::from_raw_parts(e.0.as_ptr() as *const u8, SHA512_BYTES)
                    })
                }
            }
        }

        pub fn of_slice(buf: &[u8]) -> Hash {
            let mut context = ring::digest::Context::new(&ring::digest::SHA512);
            context.update(&buf);
            let hash = context.finish();
            let mut digest: [u8; SHA512_BYTES] = [0; SHA512_BYTES];
            digest.clone_from_slice(hash.as_ref());
            Hash::Sha512(Sha512(digest))
        }
    }

    impl<'a> ToBase64 for HashRef<'a> {
        fn to_base64(&self, config: Config) -> String {
            let u = self.to_unsafe();
            let mut v = vec![0; u.onpage_size() as usize];
            debug!("hash to_hex");
            unsafe { u.write_value(v.as_mut_ptr()) }
            v.to_base64(config)
        }
    }
    impl ToBase64 for Hash {
        fn to_base64(&self, config: Config) -> String {
            self.as_ref().to_base64(config)
        }
    }

    impl<'a> HashRef<'a> {
        pub fn to_owned(&self) -> Hash {
            match *self {
                HashRef::None => Hash::None,
                HashRef::Sha512(e) => {
                    let mut hash = [0; SHA512_BYTES];
                    unsafe {
                        std::ptr::copy_nonoverlapping(e.as_ptr() as *const u8,
                                                      hash.as_mut_ptr() as *mut u8,
                                                      SHA512_BYTES)
                    }
                    Hash::Sha512(Sha512(hash))
                }
            }
        }
    }

    impl Clone for Hash {
        fn clone(&self) -> Self {
            self.as_ref().to_owned()
        }
    }

    pub const ROOT_HASH: &'static Hash = &Hash::None;

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
    #[repr(u8)]
    pub enum Algorithm {
        None = 0,
        Sha512 = 1,
    }

    #[derive(Clone, Copy, Debug)]
    pub enum UnsafeHash {
        None,
        Sha512(*const u8),
    }


    impl<'a> HashRef<'a> {
        pub fn to_unsafe(&self) -> UnsafeHash {
            match *self {
                HashRef::None => UnsafeHash::None,
                HashRef::Sha512(e) => UnsafeHash::Sha512(e.as_ptr()),
            }
        }
        pub unsafe fn from_unsafe(p: UnsafeHash) -> HashRef<'a> {
            match p {
                UnsafeHash::None => HashRef::None,
                UnsafeHash::Sha512(p) => {
                    HashRef::Sha512(std::slice::from_raw_parts(p, SHA512_BYTES))
                }
            }
        }
    }


    impl Representable for UnsafeHash {
        fn alignment() -> Alignment {
            Alignment::B1
        }

        fn onpage_size(&self) -> u16 {
            1 +
            (match *self {
                UnsafeHash::Sha512(_) => 64,
                UnsafeHash::None => 0,
            })
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?} {:?}", self, p);
            match *self {
                UnsafeHash::Sha512(q) => {
                    *p = Algorithm::Sha512 as u8;
                    std::ptr::copy(q, p.offset(1), 64)
                }
                UnsafeHash::None => *p = Algorithm::None as u8,
            }
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?} {:?}", p, *p);
            match std::mem::transmute(*p) {
                Algorithm::Sha512 => UnsafeHash::Sha512(p.offset(1)),
                Algorithm::None => UnsafeHash::None,
            }
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a = HashRef::from_unsafe(*self);
            let b = HashRef::from_unsafe(x);
            a.cmp(&b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::hash::*;



mod inode {
    use sanakirja::{Representable, Alignment};
    use std;

    pub const INODE_SIZE: usize = 8;
    #[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
    pub struct Inode([u8; INODE_SIZE]);
    pub const ROOT_INODE: Inode = Inode([0; INODE_SIZE]);
    impl std::ops::Deref for Inode {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            &self.0
        }
    }
    impl std::ops::DerefMut for Inode {
        fn deref_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
    }

    use rustc_serialize::hex::{ToHex, FromHex, FromHexError};
    impl std::fmt::Debug for Inode {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(fmt, "Inode({})", self.0.to_hex())
        }
    }
    impl Inode {

        pub fn from_hex(hex: &str) -> Result<Inode, FromHexError> {
            let mut i = Inode([0; INODE_SIZE]);
            let hex = hex.from_hex()?;
            i.0.clone_from_slice(&hex);
            Ok(i)
        }

        pub fn to_unsafe(&self) -> UnsafeInode {
            UnsafeInode(self)
        }
        pub unsafe fn from_unsafe<'a>(p: UnsafeInode) -> &'a Self {
            &*p.0
        }
    }

    #[derive(Clone, Copy)]
    pub struct UnsafeInode(*const Inode);
    impl std::fmt::Debug for UnsafeInode {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            unsafe {
                Inode::from_unsafe(*self).fmt(fmt)
            }
        }
    }

    impl Representable for UnsafeInode {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            std::mem::size_of::<Inode>() as u16
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self.0, p as *mut Inode, 1)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeInode(p as *const Inode)
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a: &Inode = &*self.0;
            let b: &Inode = &*x.0;
            a.cmp(b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::inode::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct FileMetadata(u16);
const DIR_BIT: u16 = 0x200;
use byteorder::ByteOrder;
impl FileMetadata {
    pub fn from_contents(p: &[u8]) -> Self {
        debug_assert!(p.len() == 2);
        FileMetadata(BigEndian::read_u16(p))
    }

    pub fn new(perm: usize, is_dir: bool) -> Self {
        let mut m = FileMetadata(0);
        m.set_permissions(perm as u16);
        if is_dir {
            m.set_dir()
        } else {
            m.unset_dir()
        }
        m
    }

    pub fn permissions(&self) -> u16 {
        u16::from_le(self.0) & 0x1ff
    }

    pub fn set_permissions(&mut self, perm: u16) {
        let bits = u16::from_le(self.0);
        let perm = (bits & !0x1ff) | perm;
        self.0 = perm.to_le()
    }

    pub fn is_dir(&self) -> bool {
        u16::from_le(self.0) & DIR_BIT != 0
    }

    pub fn set_dir(&mut self) {
        let bits = u16::from_le(self.0);
        self.0 = (bits | DIR_BIT).to_le()
    }

    pub fn unset_dir(&mut self) {
        let bits = u16::from_le(self.0);
        self.0 = (bits & !DIR_BIT).to_le()
    }
}

use byteorder::{BigEndian, WriteBytesExt};

pub trait WriteMetadata: std::io::Write {
    fn write_metadata(&mut self, m: FileMetadata) -> Result<(), std::io::Error> {
        self.write_u16::<BigEndian>(m.0)
    }
}
impl<W: std::io::Write> WriteMetadata for W {}


mod file_header {
    use sanakirja::{Representable, Alignment};
    use std;
    use super::key::*;
    use super::patch_id::*;
    #[repr(u8)]
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub enum FileStatus {
        Ok = 0,
        Moved = 1,
        Deleted = 2,
    }

    // Warning: FileMetadata is 16 bit-aligned, don't change the order.
    #[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
    #[repr(packed)]
    pub struct FileHeader {
        pub metadata: super::FileMetadata,
        pub status: FileStatus,
        pub key: Key<PatchId>,
    }


    impl FileHeader {
        pub fn to_unsafe(&self) -> UnsafeFileHeader {
            UnsafeFileHeader(self)
        }
        pub unsafe fn from_unsafe<'a>(p: UnsafeFileHeader) -> &'a Self {
            &*p.0
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct UnsafeFileHeader(*const FileHeader);

    impl Representable for UnsafeFileHeader {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            std::mem::size_of::<FileHeader>() as u16
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self.0, p as *mut FileHeader, 1)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeFileHeader(p as *const FileHeader)
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a: &FileHeader = &*self.0;
            let b: &FileHeader = &*x.0;
            a.cmp(b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::file_header::*;

mod file_id {
    use sanakirja::{Representable, Alignment};
    use std;
    use super::inode::*;
    use super::small_string::*;
    // pub const MAX_BASENAME_LENGTH: usize = 255;

    #[derive(Debug)]
    #[repr(packed)]
    pub struct OwnedFileId {
        pub parent_inode: Inode,
        pub basename: SmallString,
    }

    impl OwnedFileId {
        pub fn as_file_id(&self) -> FileId {
            FileId {
                parent_inode: &self.parent_inode,
                basename: self.basename.as_small_str(),
            }
        }
    }

    #[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
    #[repr(packed)]
    pub struct FileId<'a> {
        pub parent_inode: &'a Inode,
        pub basename: SmallStr<'a>,
    }

    #[derive(Clone, Copy, Debug)]
    pub struct UnsafeFileId {
        parent_inode: UnsafeInode,
        basename: UnsafeSmallStr,
    }

    impl<'a> FileId<'a> {
        pub fn to_owned(&self) -> OwnedFileId {
            OwnedFileId {
                parent_inode: self.parent_inode.clone(),
                basename: self.basename.to_owned(),
            }
        }
        pub fn to_unsafe(&self) -> UnsafeFileId {
            UnsafeFileId {
                parent_inode: self.parent_inode.to_unsafe(),
                basename: self.basename.to_unsafe(),
            }
        }
        pub unsafe fn from_unsafe(p: UnsafeFileId) -> FileId<'a> {
            FileId {
                parent_inode: Inode::from_unsafe(p.parent_inode),
                basename: SmallStr::from_unsafe(p.basename),
            }
        }
    }

    impl Representable for UnsafeFileId {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            INODE_SIZE as u16 + self.basename.onpage_size()
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            self.parent_inode.write_value(p);
            self.basename.write_value(p.offset(INODE_SIZE as isize));
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeFileId {
                parent_inode: UnsafeInode::read_value(p),
                basename: UnsafeSmallStr::read_value(p.offset(INODE_SIZE as isize)),
            }
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            trace!("cmp_value file_id");
            let a: FileId = FileId::from_unsafe(*self);
            let b: FileId = FileId::from_unsafe(x);
            a.cmp(&b)
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }

}


pub use self::file_id::*;

mod small_string {
    use sanakirja::{Representable, Alignment};
    use std;
    pub const MAX_LENGTH: usize = 255;

    #[repr(packed)]
    pub struct SmallString {
        pub len: u8,
        pub str: [u8; MAX_LENGTH],
    }

    #[derive(Clone, Copy)]
    pub struct SmallStr<'a>(*const u8, std::marker::PhantomData<&'a ()>);

    impl Clone for SmallString {
        fn clone(&self) -> Self {
            Self::from_str(self.as_str())
        }
    }

    impl std::fmt::Debug for SmallString {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            self.as_small_str().fmt(fmt)
        }
    }


    impl<'a> PartialEq for SmallStr<'a> {
        fn eq(&self, x: &SmallStr) -> bool {
            self.as_str().eq(x.as_str())
        }
    }
    impl<'a> Eq for SmallStr<'a> {}

    impl PartialEq for SmallString {
        fn eq(&self, x: &SmallString) -> bool {
            self.as_str().eq(x.as_str())
        }
    }
    impl Eq for SmallString {}

    impl<'a> std::hash::Hash for SmallStr<'a> {
        fn hash<H:std::hash::Hasher>(&self, x: &mut H) {
            self.as_str().hash(x)
        }
    }

    impl std::hash::Hash for SmallString {
        fn hash<H:std::hash::Hasher>(&self, x: &mut H) {
            self.as_str().hash(x)
        }
    }

    impl<'a> PartialOrd for SmallStr<'a> {
        fn partial_cmp(&self, x: &SmallStr) -> Option<std::cmp::Ordering> {
            self.as_str().partial_cmp(x.as_str())
        }
    }
    impl<'a> Ord for SmallStr<'a> {
        fn cmp(&self, x: &SmallStr) -> std::cmp::Ordering {
            self.as_str().cmp(x.as_str())
        }
    }



    impl<'a> std::fmt::Debug for SmallStr<'a> {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            self.as_str().fmt(fmt)
        }
    }

    impl SmallString {
        pub fn len(&self) -> usize {
            self.len as usize
        }
        pub fn from_str(s: &str) -> Self {
            let mut b = SmallString {
                len: s.len() as u8,
                str: [0; MAX_LENGTH],
            };
            b.clone_from_str(s);
            b
        }
        pub fn clone_from_str(&mut self, s: &str) {
            (&mut self.str[..s.len()]).copy_from_slice(s.as_bytes());
        }
        pub fn clear(&mut self) {
            self.len = 0;
        }
        pub fn push_str(&mut self, s: &str) {
            let l = self.len as usize;
            assert!(l + s.len() <= 0xff);
            (&mut self.str[l..l + s.len()]).copy_from_slice(s.as_bytes());
            self.len += s.len() as u8;
        }

        pub fn as_small_str(&self) -> SmallStr {
            SmallStr(self as *const SmallString as *const u8,
                     std::marker::PhantomData)
        }

        pub fn as_str(&self) -> &str {
            self.as_small_str().as_str()
        }
    }

    impl<'a> SmallStr<'a> {
        pub fn len(&self) -> usize {
            unsafe { (*self.0) as usize }
        }
        pub fn as_str(&self) -> &'a str {
            unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(self.0.offset(1),
                                                                         *self.0 as usize))
            }
        }
        pub fn to_unsafe(&self) -> UnsafeSmallStr {
            UnsafeSmallStr(self.0)
        }
        pub unsafe fn from_unsafe(u: UnsafeSmallStr) -> Self {
            SmallStr(u.0, std::marker::PhantomData)
        }
        pub fn to_owned(&self) -> SmallString {
            SmallString::from_str(self.as_str())
        }
    }

    #[derive(Clone, Copy)]
    pub struct UnsafeSmallStr(*const u8);
    impl std::fmt::Debug for UnsafeSmallStr {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            unsafe {
                SmallStr::from_unsafe(*self).fmt(fmt)
            }
        }
    }

    impl Representable for UnsafeSmallStr {
        fn alignment() -> Alignment {
            Alignment::B1
        }
        fn onpage_size(&self) -> u16 {
            unsafe {
                let len = (*self.0) as u16;
                1 + len
            }
        }
        unsafe fn write_value(&self, p: *mut u8) {
            trace!("write_value {:?}", p);
            std::ptr::copy(self.0, p, self.onpage_size() as usize)
        }
        unsafe fn read_value(p: *const u8) -> Self {
            trace!("read_value {:?}", p);
            UnsafeSmallStr(p)
        }
        unsafe fn cmp_value<T>(&self, _: &T, x: Self) -> std::cmp::Ordering {
            let a = SmallStr::from_unsafe(UnsafeSmallStr(self.0));
            let b = SmallStr::from_unsafe(x);
            a.as_str().cmp(b.as_str())
        }
        type PageOffsets = std::iter::Empty<u64>;
        fn page_offsets(&self) -> Self::PageOffsets { std::iter::empty() }
    }
}

pub use self::small_string::*;

pub type NodesDb = sanakirja::Db<self::key::UnsafeKey, self::edge::UnsafeEdge>;

pub type ApplyTimestamp = u64;

/// The u64 is the epoch time in seconds when this patch was applied
/// to the repository.
type PatchSet = sanakirja::Db<self::patch_id::PatchId, ApplyTimestamp>;

type RevPatchSet = sanakirja::Db<ApplyTimestamp, self::patch_id::PatchId>;

pub struct Dbs {
    /// A map of the files in the working copy.
    tree: sanakirja::Db<self::file_id::UnsafeFileId, self::inode::UnsafeInode>,
    /// The reverse of tree.
    revtree: sanakirja::Db<self::inode::UnsafeInode, self::file_id::UnsafeFileId>,
    /// A map from inodes (in tree) to keys in branches.
    inodes: sanakirja::Db<self::inode::UnsafeInode, self::file_header::UnsafeFileHeader>,
    /// The reverse of inodes, minus the header.
    revinodes: sanakirja::Db<self::key::UnsafeKey, self::inode::UnsafeInode>,
    /// Text contents of keys.
    contents: sanakirja::Db<self::key::UnsafeKey, sanakirja::value::UnsafeValue>,
    /// A map from external patch hashes to internal ids.
    internal: sanakirja::Db<self::hash::UnsafeHash, self::patch_id::PatchId>,
    /// The reverse of internal.
    external: sanakirja::Db<self::patch_id::PatchId, self::hash::UnsafeHash>,
    /// A reverse map of patch dependencies, i.e. (k,v) is in this map
    /// means that v depends on k.
    revdep: sanakirja::Db<self::patch_id::PatchId, self::patch_id::PatchId>,
    /// A map from branch names to graphs.
    branches: sanakirja::Db<self::small_string::UnsafeSmallStr, (NodesDb, PatchSet, RevPatchSet, u64)>,
}


pub struct T<T, R> {
    pub txn: T,
    pub rng: R,
    pub dbs: Dbs,
}

pub type MutTxn<'env, R> = T<sanakirja::MutTxn<'env, ()>, R>;
pub type Txn<'env> = T<sanakirja::Txn<'env>, ()>;

pub const DEFAULT_BRANCH: &'static str = "master";

pub struct Repository {
    env: sanakirja::Env,
}

#[derive(Debug,PartialEq, Clone, Copy)]
pub enum Root {
    Tree,
    RevTree,
    Inodes,
    RevInodes,
    Contents,
    Internal,
    External,
    RevDep,
    Branches,
}

trait OpenDb: Transaction {
    fn open_db<K: Representable, V: Representable>(&mut self,
                                                   num: Root)
                                                   -> Result<sanakirja::Db<K, V>, Error> {
        if let Some(db) = self.root(num as usize) {
            Ok(db)
        } else {
            Err(Error::NoDb(num))
        }
    }
}

impl<'a, T> OpenDb for sanakirja::MutTxn<'a, T> {
    fn open_db<K: Representable, V: Representable>(&mut self,
                                                   num: Root)
                                                   -> Result<sanakirja::Db<K, V>, Error> {
        if let Some(db) = self.root(num as usize) {
            Ok(db)
        } else {
            Ok(try!(self.create_db()))
        }
    }
}
impl<'a> OpenDb for sanakirja::Txn<'a> {}

// Repositories need at least 2^5 = 32 pages, each of size 2^12.
const MIN_REPO_SIZE: u64 = 1 << 17;

impl Repository {

    pub fn repository_size<P: AsRef<Path>>(path: P) -> Result<u64, Error> {
        Ok(sanakirja::Env::file_size(path.as_ref())?)
    }

    pub fn open<P: AsRef<Path>>(path: P, size_increase: Option<u64>) -> Result<Self, Error> {
        let size =
            if let Some(size) = size_increase {
                sanakirja::Env::file_size(path.as_ref()).unwrap_or(MIN_REPO_SIZE) + std::cmp::max(size, MIN_REPO_SIZE)
            } else {
                if let Ok(len) = sanakirja::Env::file_size(path.as_ref()) {
                    std::cmp::max(len, MIN_REPO_SIZE)
                } else {
                    MIN_REPO_SIZE
                }
            };
        Ok(Repository { env: try!(sanakirja::Env::new(path, size)) })
    }

    pub fn mut_txn_begin<R: rand::Rng>(&self, r: R) -> Result<MutTxn<R>, Error> {
        let mut txn = try!(self.env.mut_txn_begin());
        let dbs = try!(Dbs::new(&mut txn));
        let repo = T {
            txn: txn,
            rng: r,
            dbs: dbs,
        };
        Ok(repo)
    }

    pub fn txn_begin(&self) -> Result<Txn, Error> {
        let mut txn = try!(self.env.txn_begin());
        let dbs = try!(Dbs::new(&mut txn));
        let repo = T {
            txn: txn,
            rng: (),
            dbs: dbs,
        };
        Ok(repo)
    }
}

impl Dbs {
    fn new<T: OpenDb>(txn: &mut T) -> Result<Self, Error> {
        let external = try!(txn.open_db(Root::External));
        let branches = try!(txn.open_db(Root::Branches));
        let tree = try!(txn.open_db(Root::Tree));
        let revtree = try!(txn.open_db(Root::RevTree));
        let inodes = try!(txn.open_db(Root::Inodes));
        let revinodes = try!(txn.open_db(Root::RevInodes));
        let internal = try!(txn.open_db(Root::Internal));
        let contents = try!(txn.open_db(Root::Contents));
        let revdep = try!(txn.open_db(Root::RevDep));

        Ok(Dbs {
            external: external,
            branches: branches,
            inodes: inodes,
            tree: tree,
            revtree: revtree,
            revinodes: revinodes,
            internal: internal,
            revdep: revdep,
            contents: contents,
        })
    }
}

#[derive(Debug)]
pub struct Branch {
    pub db: NodesDb,
    pub patches: PatchSet,
    pub revpatches: RevPatchSet,
    pub apply_counter: u64,
    pub name: small_string::SmallString,
}

use sanakirja::Commit;
impl<'env, R: rand::Rng> MutTxn<'env, R> {

    pub fn open_branch<'name>(&mut self, name: &str) -> Result<Branch, Error> {
        let name = small_string::SmallString::from_str(name);
        let (branch, patches, revpatches, counter) = if let Some(x) = self.txn
            .get(&self.dbs.branches, name.as_small_str().to_unsafe(), None) {
                x
            } else {
                (try!(self.txn.create_db()), try!(self.txn.create_db()), try!(self.txn.create_db()), 0)
            };
        Ok(Branch {
            db: branch,
            patches: patches,
            revpatches: revpatches,
            name: name,
            apply_counter: counter
        })
    }

    pub fn commit_branch(&mut self, branch: Branch) -> Result<(), Error> {
        debug!("Commit_branch. This is not too safe.");
        // Since we are replacing the value, we don't want to
        // decrement its reference counter (which del would do), hence
        // the transmute.
        //
        // This would normally be wrong. The only reason it works is
        // because we know that dbs_branches has never been forked
        // from another database, hence all the reference counts to
        // its elements are 1 (and therefore represented as "not
        // referenced" in Sanakirja.
        let mut dbs_branches: sanakirja::Db<UnsafeSmallStr, (u64, u64, u64)> =
            unsafe { std::mem::transmute(self.dbs.branches) };

        debug!("Commit_branch, dbs_branches = {:?}", dbs_branches);
        try!(self.txn.del(&mut self.rng,
                          &mut dbs_branches,
                          branch.name.as_small_str().to_unsafe(),
                          None));
        debug!("Commit_branch, dbs_branches = {:?}", dbs_branches);
        self.dbs.branches = unsafe { std::mem::transmute(dbs_branches) };
        try!(self.txn.put(&mut self.rng,
                          &mut self.dbs.branches,
                          branch.name.as_small_str().to_unsafe(),
                          (branch.db, branch.patches, branch.revpatches, branch.apply_counter)));
        debug!("Commit_branch, self.dbs.branches = {:?}", self.dbs.branches);
        Ok(())
    }

    pub fn rename_branch(&mut self, branch: &mut Branch, new_name: &str) -> Result<(), Error> {
        debug!("Commit_branch. This is not too safe.");
        // Since we are replacing the value, we don't want to
        // decrement its reference counter (which del would do), hence
        // the transmute.
        //
        // Read the note in `commit_branch` to understand why this
        // works.
        let name_exists = self.get_branch(new_name).is_some();
        if name_exists {
            Err(Error::BranchNameAlreadyExists)
        } else {
            let mut dbs_branches: sanakirja::Db<UnsafeSmallStr, (u64, u64)> =
                unsafe { std::mem::transmute(self.dbs.branches) };
            try!(self.txn.del(&mut self.rng,
                              &mut dbs_branches,
                              branch.name.as_small_str().to_unsafe(),
                              None));
            self.dbs.branches = unsafe { std::mem::transmute(dbs_branches) };
            branch.name.clone_from_str(new_name);
            Ok(())
        }
    }

    pub fn commit(mut self) -> Result<(), Error> {

        self.txn.set_root(Root::Tree as usize, self.dbs.tree);
        self.txn.set_root(Root::RevTree as usize, self.dbs.revtree);
        self.txn.set_root(Root::Inodes as usize, self.dbs.inodes);
        self.txn.set_root(Root::RevInodes as usize, self.dbs.revinodes);
        self.txn.set_root(Root::Contents as usize, self.dbs.contents);
        self.txn.set_root(Root::Internal as usize, self.dbs.internal);
        self.txn.set_root(Root::External as usize, self.dbs.external);
        self.txn.set_root(Root::Branches as usize, self.dbs.branches);
        self.txn.set_root(Root::RevDep as usize, self.dbs.revdep);

        try!(self.txn.commit());
        Ok(())
    }
}

use sanakirja::value::*;
use sanakirja::{Cursor, RevCursor};
pub struct TreeIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeFileId, UnsafeInode>);

impl<'a, T: Transaction + 'a> Iterator for TreeIterator<'a, T> {
    type Item = (FileId<'a>, &'a Inode);
    fn next(&mut self) -> Option<Self::Item> {
        debug!("tree iter");
        if let Some((k, v)) = self.0.next() {
            debug!("tree iter: {:?} {:?}", k, v);
            unsafe { Some((FileId::from_unsafe(k), Inode::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct RevtreeIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeInode, UnsafeFileId>);

impl<'a, T: Transaction + 'a> Iterator for RevtreeIterator<'a, T> {
    type Item = (&'a Inode, FileId<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((Inode::from_unsafe(k), FileId::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct NodesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeKey, UnsafeEdge>);

impl<'a, T: Transaction + 'a> Iterator for NodesIterator<'a, T> {
    type Item = (&'a Key<PatchId>, &'a Edge);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((Key::from_unsafe(k), Edge::from_unsafe(v))) }
        } else {
            None
        }
    }
}
pub struct BranchIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeSmallStr, (NodesDb, PatchSet, RevPatchSet, u64)>);

impl<'a, T: Transaction + 'a> Iterator for BranchIterator<'a, T> {
    type Item = Branch;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some(Branch {
                name: SmallStr::from_unsafe(k).to_owned(),
                db: v.0,
                patches: v.1,
                revpatches: v.2,
                apply_counter: v.3
            }) }
        } else {
            None
        }
    }
}


pub struct PatchesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, ApplyTimestamp>);

impl<'a, T: Transaction + 'a> Iterator for PatchesIterator<'a, T> {
    type Item = (PatchId, ApplyTimestamp);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct RevAppliedIterator<'a, T: Transaction + 'a>(RevCursor<'a, T, ApplyTimestamp, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for RevAppliedIterator<'a, T> {
    type Item = (ApplyTimestamp, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct AppliedIterator<'a, T: Transaction + 'a>(Cursor<'a, T, ApplyTimestamp, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for AppliedIterator<'a, T> {
    type Item = (ApplyTimestamp, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}


pub struct InodesIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeInode, UnsafeFileHeader>);

impl<'a, T: Transaction + 'a> Iterator for InodesIterator<'a, T> {
    type Item = (&'a Inode, &'a FileHeader);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((Inode::from_unsafe(k), FileHeader::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct InternalIterator<'a, T: Transaction + 'a>(Cursor<'a, T, UnsafeHash, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for InternalIterator<'a, T> {
    type Item = (HashRef<'a>, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((HashRef::from_unsafe(k), v)) }
        } else {
            None
        }
    }
}
pub struct ExternalIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, UnsafeHash>);

impl<'a, T: Transaction + 'a> Iterator for ExternalIterator<'a, T> {
    type Item = (PatchId, HashRef<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.0.next() {
            unsafe { Some((k, HashRef::from_unsafe(v))) }
        } else {
            None
        }
    }
}

pub struct RevdepIterator<'a, T: Transaction + 'a>(Cursor<'a, T, PatchId, PatchId>);

impl<'a, T: Transaction + 'a> Iterator for RevdepIterator<'a, T> {
    type Item = (PatchId, PatchId);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct ContentsIterator<'a, T: Transaction + 'a>(&'a T, Cursor<'a, T, UnsafeKey, UnsafeValue>);

impl<'a, T: Transaction + 'a> Iterator for ContentsIterator<'a, T> {
    type Item = (&'a Key<PatchId>, Value<'a, T>);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.1.next() {
            unsafe { Some((Key::from_unsafe(k), Value::from_unsafe(&v, self.0))) }
        } else {
            None
        }
    }
}


mod dump {
    use super::*;
    use sanakirja;
    use std::path::Path;
    impl<U: Transaction, R> T<U, R> {
        pub fn dump<P:AsRef<Path>>(&self, path: P) {
            let path = path.as_ref();
            debug!("============= dumping Tree");
            for (k, v) in self.iter_tree(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Inodes");
            for (k, v) in self.iter_inodes(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping RevDep");
            for (k, v) in self.iter_revdep(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Internal");
            for (k, v) in self.iter_internal(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping External");
            for (k, v) in self.iter_external(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Contents");
            {
                sanakirja::debug(&self.txn, &[&self.dbs.contents], "dump_contents");
            }
            for (k, v) in self.iter_contents(None) {
                debug!("> {:?} {:?}", k, v)
            }
            debug!("============= dumping Branches");
            for (br, (db, patches, revpatches, counter)) in self.txn.iter(&self.dbs.branches, None) {
                debug!("patches: {:?} {:?}", patches, revpatches);
                debug!("============= dumping Patches in branch {:?}, counter = {:?}", br, counter);
                for (k, v) in self.txn.iter(&patches, None) {
                    debug!("> {:?} {:?}", k, v)
                }
                debug!("============= dumping RevPatches in branch {:?}", br);
                for (k, v) in self.txn.iter(&revpatches, None) {
                    debug!("> {:?} {:?}", k, v)
                }
                debug!("============= dumping Nodes in branch {:?}", br);
                unsafe {
                    sanakirja::debug(&self.txn, &[&db], path);
                    debug!("> {:?}", SmallStr::from_unsafe(br));
                    for (k, v) in self.txn.iter(&db, None) {
                        debug!(">> {:?} {:?}", Key::from_unsafe(k), Edge::from_unsafe(v))
                    }
                }
            }
        }
    }
}
impl<U: Transaction, R> T<U, R> {

    pub fn has_branch(&self, name: &str) -> bool {
        let name = small_string::SmallString::from_str(name);
        self.txn.get(&self.dbs.branches, name.as_small_str().to_unsafe(), None).is_some()
    }

    pub fn get_branch<'name>(&self, name: &str) -> Option<Branch> {
        let name = small_string::SmallString::from_str(name);
        if let Some((branch, patches, revpatches, counter)) = self.txn.get(&self.dbs.branches, name.as_small_str().to_unsafe(), None) {
            Some(Branch {
                db: branch,
                patches: patches,
                revpatches: revpatches,
                apply_counter: counter,
                name: name,
            })
        } else {
            None
        }
    }


    pub fn debug_db<P, K, V>(&self, db: &[&sanakirja::Db<K, V>], path: P)
        where P: AsRef<std::path::Path>,
              K: Representable,
              V: Representable
    {
        sanakirja::debug(&self.txn, db, path.as_ref())
    }

    pub fn get_nodes<'a>(&'a self,
                         branch: &Branch,
                         key: &Key<PatchId>,
                         edge: Option<&Edge>)
                         -> Option<&'a Edge> {
        self.txn
            .get(&branch.db, key.to_unsafe(), edge.map(|e| e.to_unsafe()))
            .map(|e| unsafe { Edge::from_unsafe(e) })
    }
    pub fn iter_nodes<'a>(&'a self,
                          branch: &'a Branch,
                          key: Option<(&Key<PatchId>, Option<&Edge>)>)
                          -> NodesIterator<'a, U> {
        NodesIterator(self.txn.iter(&branch.db,
                                    key.map(|(k, v)| (k.to_unsafe(), v.map(|v| v.to_unsafe())))))
    }

    pub fn iter_branches<'a>(&'a self,
                             key: Option<&SmallStr>)
                             -> BranchIterator<'a, U> {
        BranchIterator(self.txn.iter(&self.dbs.branches, key.map(|k| (k.to_unsafe(), None))))
    }


    pub fn iter_patches<'a>(&'a self,
                            branch: &'a Branch,
                            key: Option<PatchId>)
                            -> PatchesIterator<'a, U> {

        PatchesIterator(self.txn.iter(&branch.patches, key.map(|k| (k, None))))
    }

    pub fn rev_iter_applied<'a>(&'a self,
                                branch: &'a Branch,
                                key: Option<ApplyTimestamp>)
                                -> RevAppliedIterator<'a, U> {

        RevAppliedIterator(self.txn.rev_iter(&branch.revpatches,
                                             key.map(|k| (k, None))))
    }

    pub fn iter_applied<'a>(&'a self,
                            branch: &'a Branch,
                            key: Option<ApplyTimestamp>)
                            -> AppliedIterator<'a, U> {

        AppliedIterator(self.txn.iter(&branch.revpatches,
                                      key.map(|k| (k, None))))
    }


    pub fn iter_tree<'a>(&'a self,
                         key: Option<(&FileId, Option<&Inode>)>)
                         -> TreeIterator<'a, U> {
        debug!("iter_tree: {:?}", key);
        TreeIterator(self.txn.iter(&self.dbs.tree,
                                   key.map(|(k, v)| (k.to_unsafe(), v.map(|v| v.to_unsafe())))))
    }
    pub fn iter_revtree<'a>(&'a self,
                            key: Option<(&Inode, Option<&FileId>)>)
                            -> RevtreeIterator<'a, U> {
        RevtreeIterator(self.txn.iter(&self.dbs.revtree,
                                      key.map(|(k, v)| (k.to_unsafe(), v.map(|v| v.to_unsafe())))))
    }
    pub fn iter_inodes<'a>(&'a self,
                           key: Option<(&Inode, Option<&FileHeader>)>)
                           -> InodesIterator<'a, U> {
        InodesIterator(self.txn.iter(&self.dbs.inodes,
                                     key.map(|(k, v)| (k.to_unsafe(), v.map(|v| v.to_unsafe())))))
    }
    pub fn iter_external<'a>(&'a self,
                             key: Option<(PatchId, Option<HashRef>)>)
                             -> ExternalIterator<'a, U> {
        ExternalIterator(self.txn.iter(&self.dbs.external,
                                       key.map(|(k, v)| (k, v.map(|v| v.to_unsafe())))))
    }
    pub fn iter_internal<'a>(&'a self,
                             key: Option<(HashRef, Option<PatchId>)>)
                             -> InternalIterator<'a, U> {
        InternalIterator(self.txn.iter(&self.dbs.internal,
                                       key.map(|(k, v)| (k.to_unsafe(), v))))
    }

    pub fn iter_revdep<'a>(&'a self,
                           key: Option<(&PatchId, Option<&PatchId>)>)
                           -> RevdepIterator<'a, U> {
        RevdepIterator(self.txn.iter(&self.dbs.revdep,
                                     key.map(|(x, y)| (*x, y.cloned()))))
    }

    pub fn iter_contents<'a>(&'a self,
                             key: Option<&Key<PatchId>>)
                             -> ContentsIterator<'a, U> {
        ContentsIterator(&self.txn,
                         self.txn.iter(&self.dbs.contents, key.map(|k| (k.to_unsafe(), None))))
    }

    pub fn get_tree<'a>(&'a self, key: &FileId) -> Option<&'a Inode> {
        self.txn
            .get(&self.dbs.tree, key.to_unsafe(), None)
            .map(|e| unsafe { Inode::from_unsafe(e) })
    }

    pub fn get_revtree<'a>(&'a self, key: &Inode) -> Option<FileId<'a>> {
        self.txn
            .get(&self.dbs.revtree, key.to_unsafe(), None)
            .map(|e| unsafe { FileId::from_unsafe(e) })
    }
    pub fn get_inodes<'a>(&'a self, key: &Inode) -> Option<&'a FileHeader> {
        self.txn
            .get(&self.dbs.inodes, key.to_unsafe(), None)
            .map(|e| unsafe { FileHeader::from_unsafe(e) })
    }
    pub fn get_revinodes<'a>(&'a self, key: &Key<PatchId>) -> Option<&'a Inode> {
        self.txn
            .get(&self.dbs.revinodes, key.to_unsafe(), None)
            .map(|e| unsafe { Inode::from_unsafe(e) })
    }

    pub fn get_contents<'a>(&'a self, key: &Key<PatchId>) -> Option<Value<'a, U>> {
        if let Some(e) = self.txn.get(&self.dbs.contents, key.to_unsafe(), None) {
            unsafe { Some(Value::from_unsafe(&e, &self.txn)) }
        } else {
            None
        }
    }

    pub fn get_internal(&self, key: HashRef) -> Option<PatchId> {
        match key {
            HashRef::None => Some(ROOT_PATCH_ID),
            h => {
                self.txn
                    .get(&self.dbs.internal, h.to_unsafe(), None)
            }
        }
    }
    pub fn get_external<'a>(&'a self, key: &PatchId) -> Option<HashRef<'a>> {
        self.txn
            .get(&self.dbs.external, *key, None)
            .map(|e| unsafe { HashRef::from_unsafe(e) })
    }
    pub fn get_patch<'a>(&'a self,
                         patch_set: &PatchSet,
                         patch_id: &PatchId)
                         -> Option<ApplyTimestamp> {
        self.txn
            .get(patch_set,
                 *patch_id,
                 None)
    }
    pub fn get_revdep<'a>(&self,
                          patch_id: &PatchId,
                          dep: Option<PatchId>)
                          -> Option<PatchId> {

        self.txn.get(&self.dbs.revdep, *patch_id, dep)
    }

    pub fn debug<W>(&self, branch_name: &str, w: &mut W)
        where W: std::io::Write
    {
        use rustc_serialize::hex::ToHex;
        debug!("debugging branch {:?}", branch_name);
        let mut styles = Vec::with_capacity(16);
        for i in 0..16 {
            let flag = EdgeFlags::from_bits(i as u8).unwrap();
            styles.push(("color=").to_string() + ["red", "blue", "green", "black"][(i >> 1) & 3] +
                        if flag.contains(DELETED_EDGE) {
                ", style=dashed"
            } else {
                ""
            } +
                        if flag.contains(PSEUDO_EDGE) {
                ", style=dotted"
            } else {
                ""
            })
        }
        w.write(b"digraph{\n").unwrap();
        let branch = self.get_branch(branch_name).unwrap();

        let mut cur: Key<PatchId> = ROOT_KEY.clone();
        for (k, v) in self.iter_nodes(&branch, None) {
            if *k != cur {
                let cont = if let Some(cont) = self.get_contents(k) {
                    let cont = cont.into_cow();
                    let cont = &cont[ .. std::cmp::min(0, cont.len())];
                    format!("{:?}",
                            match std::str::from_utf8(cont) {
                                Ok(x) => x.to_string(),
                                Err(_) => cont.to_hex(),
                            })
                } else {
                    "\"\"".to_string()
                };
                // remove the leading and trailing '"'.
                let cont = &cont [1..(cont.len()-1)];
                write!(w,
                       "n_{}[label=\"{}: {}\"];\n",
                       k.to_hex(),
                       k.to_hex(),
                       cont
                )
                    .unwrap();
                cur = k.clone();
            }
            debug!("debug: {:?}", v);
            let flag = v.flag.bits();
            write!(w,
                   "n_{}->n_{}[{},label=\"{} {}\"];\n",
                   k.to_hex(),
                   &v.dest.to_hex(),
                   styles[(flag & 0xff) as usize],
                   flag,
                   v.introduced_by.to_hex()
            )
                .unwrap();
        }
        w.write(b"}\n").unwrap();
    }
}

impl<'env, R: rand::Rng> MutTxn<'env, R> {

    pub fn drop_branch(&mut self, branch: &str) -> Result<bool, Error> {
        let name = SmallString::from_str(branch);
        Ok(self.txn.del(&mut self.rng, &mut self.dbs.branches, name.as_small_str().to_unsafe(), None)?)
    }

    pub fn put_nodes(&mut self,
                     branch: &mut Branch,
                     key: &Key<PatchId>,
                     edge: &Edge)
                     -> Result<bool, Error> {
        debug!("put_nodes: {:?} {:?}", key, edge);
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut branch.db,
                             key.to_unsafe(),
                             edge.to_unsafe())))
    }
    pub fn del_nodes(&mut self,
                     branch: &mut Branch,
                     key: &Key<PatchId>,
                     edge: Option<&Edge>)
                     -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut branch.db,
                             key.to_unsafe(),
                             edge.map(|e| e.to_unsafe()))))
    }

    pub fn put_tree(&mut self, key: &FileId, edge: &Inode) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.tree,
                             key.to_unsafe(),
                             edge.to_unsafe())))
    }
    pub fn del_tree(&mut self, key: &FileId, edge: Option<&Inode>) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.tree,
                             key.to_unsafe(),
                             edge.map(|e| e.to_unsafe()))))
    }


    pub fn put_revtree(&mut self, key: &Inode, value: &FileId) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.revtree,
                             key.to_unsafe(),
                             value.to_unsafe())))
    }
    pub fn del_revtree(&mut self, key: &Inode, value: Option<&FileId>) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.revtree,
                             key.to_unsafe(),
                             value.map(|e| e.to_unsafe()))))
    }


    pub fn del_inodes(&mut self, key: &Inode, value: Option<&FileHeader>) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.inodes,
                             key.to_unsafe(),
                             value.map(|e| e.to_unsafe()))))
    }
    pub fn replace_inodes(&mut self, key: &Inode, value: &FileHeader) -> Result<bool, Error> {

        self.txn.del(&mut self.rng, &mut self.dbs.inodes, key.to_unsafe(), None)?;
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.inodes,
                        key.to_unsafe(),
                        value.to_unsafe())?)
    }

    pub fn replace_revinodes(&mut self, key: &Key<PatchId>, value: &Inode) -> Result<bool, Error> {
        self.txn.del(&mut self.rng,
                     &mut self.dbs.revinodes,
                     key.to_unsafe(),
                     None)?;
        Ok(self.txn.put(&mut self.rng,
                        &mut self.dbs.revinodes,
                        key.to_unsafe(),
                        value.to_unsafe())?)
    }

    pub fn del_revinodes(&mut self,
                         key: &Key<PatchId>,
                         value: Option<&Inode>)
                         -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.revinodes,
                             key.to_unsafe(),
                             value.map(|e| e.to_unsafe()))))
    }


    pub fn put_contents(&mut self, key: &Key<PatchId>, value: UnsafeValue) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.contents,
                             key.to_unsafe(),
                             value)))
    }

    pub fn del_contents(&mut self,
                        key: &Key<PatchId>,
                        value: Option<UnsafeValue>)
                        -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.contents,
                             key.to_unsafe(),
                             value)))
    }




    pub fn put_internal(&mut self, key: HashRef, value: &PatchId) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.internal,
                             key.to_unsafe(),
                             *value)))
    }
    pub fn del_internal(&mut self, key: HashRef) -> Result<bool, Error> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.internal,
                        key.to_unsafe(),
                        None)?)
    }


    pub fn put_external(&mut self, key: &PatchId, value: HashRef) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.external,
                             *key,
                             value.to_unsafe())))
    }
    pub fn del_external(&mut self, key: &PatchId) -> Result<bool, Error> {
        Ok(self.txn.del(&mut self.rng,
                        &mut self.dbs.external,
                        *key,
                        None)?)
    }


    pub fn put_patches(&mut self, branch: &mut PatchSet, value: &PatchId, time: ApplyTimestamp) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             branch,
                             *value,
                             time)))
    }
    pub fn del_patches(&mut self, branch: &mut PatchSet, value: &PatchId) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             branch,
                             *value,
                             None)))
    }
    pub fn put_revpatches(&mut self, branch: &mut RevPatchSet, time: ApplyTimestamp, value: &PatchId) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             branch,
                             time,
                             *value)))
    }
    pub fn del_revpatches(&mut self, revbranch: &mut RevPatchSet, timestamp: ApplyTimestamp, value: &PatchId) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             revbranch,
                             timestamp,
                             Some(*value))))
    }
    pub fn put_revdep(&mut self, patch: &PatchId, revdep: &PatchId) -> Result<bool, Error> {
        Ok(try!(self.txn.put(&mut self.rng,
                             &mut self.dbs.revdep,
                             *patch,
                             *revdep)))
    }
    pub fn del_revdep(&mut self, patch: &PatchId, revdep: Option<&PatchId>) -> Result<bool, Error> {
        Ok(try!(self.txn.del(&mut self.rng,
                             &mut self.dbs.revdep,
                             *patch,
                             revdep.cloned())))
    }

    pub fn alloc_value(&mut self, slice: &[u8]) -> Result<UnsafeValue, Error> {
        Ok(try!(UnsafeValue::alloc_if_needed(&mut self.txn, slice)))
    }


}

macro_rules! iterate_parents {
    ($txn:expr, $branch:expr, $key:expr, $flag: expr) => { {
        let edge = Edge::zero($flag|PARENT_EDGE);
        $txn.iter_nodes(& $branch, Some(($key, Some(&edge))))
            .take_while(|&(k, parent)| {
                *k == *$key && parent.flag <= $flag|PARENT_EDGE|PSEUDO_EDGE
            })
            .map(|(_,b)| b)
    } }
}
