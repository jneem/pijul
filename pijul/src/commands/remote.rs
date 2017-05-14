use libpijul::{Hash, ApplyTimestamp, apply_resize, Repository};
use libpijul::patch::read_changes;
use libpijul::fs_representation::{branch_changes_base_path, pristine_dir,
                                  patches_dir, PIJUL_DIR_NAME, PATCHES_DIR_NAME, patch_file_name};
use hyper;
use hyper_rustls;
use hyper::net::HttpsConnector;
use regex::Regex;
use rustc_serialize::base64::{ToBase64, URL_SAFE};

use std::path::{Path, PathBuf};
use std::collections::hash_set::HashSet;
use std::fs::{File, hard_link, copy, metadata};
use std;

use error::Error;

use std::io::prelude::*;
use std::net::ToSocketAddrs;
use shell_escape::unix::escape;
use std::borrow::Cow;
use commands::init;
use futures;
use user;
use thrussh;
use commands::ask;
use futures::{Future, Stream, Async, Poll};
use tokio_core;
use tokio_core::net::TcpStream;


const HTTP_MAX_ATTEMPTS: usize = 3;

#[derive(Debug)]
pub enum Remote<'a> {
    Ssh {
        user: Option<&'a str>,
        host: &'a str,
        port: Option<u16>,
        path: &'a str,
        id: &'a str,
    },
    Uri { uri: &'a str },
    Local { path: PathBuf },
}

pub enum Session<'a> {
    Ssh {
        l: tokio_core::reactor::Core,
        id: &'a str,
        path: &'a str,
        session: Option<thrussh::client::Connection<TcpStream, Client>>,
    },
    Uri { uri: &'a str, client: hyper::Client },
    Local { path: &'a Path },
}

pub struct Client {
    exit_status: Option<u32>,
    state: State,
    host: String,
    port: u16,
    channel: Option<thrussh::ChannelId>,
}

enum State {
    None,
    Changes { changes: HashSet<(Hash, ApplyTimestamp)>, },
    DownloadPatch { file: File },
}

enum SendFileState {
    Read(thrussh::client::Connection<TcpStream, Client>),
    Wait(thrussh::client::Data<TcpStream, Client, Vec<u8>>),
}


struct SendFile {
    f: File,
    buf: Option<Vec<u8>>,
    chan: thrussh::ChannelId,
    state: Option<SendFileState>,
}

impl Future for SendFile {
    type Item = (thrussh::client::Connection<TcpStream, Client>, Vec<u8>);
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        debug!("SendFile loop starting");
        loop {
            debug!("sendfile loop");
            match self.state.take() {
                Some(SendFileState::Read(c)) => {
                    debug!("read");
                    let mut buf = self.buf.take().unwrap();
                    buf.resize(BUFFER_SIZE, 0);
                    let len = self.f.read(&mut buf)?;
                    if len == 0 {
                        // If nothing has been read, return.
                        return Ok(Async::Ready((c, buf)));
                    }
                    buf.truncate(len);
                    debug!("sending {:?} bytes, {:?}", len, buf.len());
                    self.state = Some(SendFileState::Wait(c.data(self.chan, None, buf)));
                }
                Some(SendFileState::Wait(mut c)) => {
                    debug!("wait");
                    match try!(c.poll()) {
                        Async::Ready((c, buf)) => {
                            self.buf = Some(buf);
                            self.state = Some(SendFileState::Read(c))
                        }
                        Async::NotReady => {
                            self.state = Some(SendFileState::Wait(c));
                            return Ok(Async::NotReady);
                        }
                    }
                }
                None => unreachable!(),
            }
        }
    }
}




impl thrussh::client::Handler for Client {
    type Error = Error;
    type FutureUnit = futures::Finished<Client, Error>;
    type SessionUnit = futures::Finished<(Client, thrussh::client::Session), Error>;
    type FutureBool = futures::future::FutureResult<(Client, bool), Error>;

    fn data(mut self,
            channel: thrussh::ChannelId,
            stream: Option<u32>,
            data: &[u8],
            session: thrussh::client::Session)
            -> Self::SessionUnit {

        debug!("data ({:?}): {:?}", channel, &data[..std::cmp::min(data.len(), 100)]);
        if stream == Some(1) {
            std::io::stderr().write(data).unwrap();
        } else if stream == None {

            match self.state {
                State::None => {
                    std::io::stdout().write(data).unwrap();
                }
                State::Changes { ref mut changes } => {
                    let data = std::str::from_utf8(data).unwrap();
                    for l in data.lines() {
                        let mut spl = l.split(':');
                        if let (Some(h), Some(s)) = (spl.next(), spl.next()) {
                            if let (Some(h), Ok(s)) =
                                (Hash::from_base64(h), s.parse()) {
                                    changes.insert((h, s));
                            }
                        }
                    }
                }
                State::DownloadPatch { ref mut file } => {
                    file.write_all(data).unwrap();
                }
            }
        } else {
            debug!("SSH data received on channel {:?}: {:?} {:?}",
                   channel,
                   stream,
                   data);
        }
        futures::finished((self, session))
    }
    fn exit_status(mut self,
                   channel: thrussh::ChannelId,
                   exit_status: u32,
                   session: thrussh::client::Session)
                   -> Self::SessionUnit {
        debug!("exit_status received on channel {:?}: {:?}:", channel, exit_status);
        if let Some(c) = self.channel {
            if channel == c {
                self.exit_status = Some(exit_status);
            }
        }
        futures::finished((self, session))
    }

    fn check_server_key(self, server_public_key: &thrussh::key::PublicKey) -> Self::FutureBool {

        let path = std::env::home_dir().unwrap().join(".ssh").join("known_hosts");
        match thrussh::check_known_hosts_path(&self.host, self.port, &server_public_key, &path) {
            Ok(true) => futures::done(Ok((self, true))),
            Ok(false) => {
                if let Ok(false) = ask::ask_learn_ssh(&self.host,
                                                      self.port, "") {
                    // TODO
                    // &server_public_key.fingerprint()) {

                    futures::done(Ok((self, false)))

                } else {
                    thrussh::learn_known_hosts_path(&self.host,
                                                    self.port,
                                                    &server_public_key,
                                                    &path)
                        .unwrap();
                    futures::done(Ok((self, true)))
                }
            }
            Err(thrussh::Error::KeyChanged(line)) => {
                println!("Host key changed! Someone might be eavesdropping this communication, \
                          refusing to continue. Previous key found line {}",
                         line);
                futures::done(Ok((self, false)))
            }
            Err(e) => futures::done(Err(From::from(e))),
        }
    }
}

const BUFFER_SIZE: usize = 1 << 14; // 16 kb.

impl<'a> Session<'a> {
    pub fn changes(&mut self, branch: &str) -> Result<HashSet<(Hash, ApplyTimestamp)>, Error> {
        match *self {
            Session::Ssh { ref mut l, ref path, ref mut session, .. } => {

                let esc_path = escape(Cow::Borrowed(path));
                let cmd = format!("pijul changes --repository {} --branch {:?} --hash-only",
                                  esc_path,
                                  branch);

                if let Some(ref mut session) = *session {
                    session.handler_mut().state = State::Changes { changes: HashSet::new() }
                }
                *session = Some(l.run(session.take()
                        .unwrap()
                        .channel_open_session()
                        .and_then(move |(mut connection, chan)| {
                            debug!("exec: {:?}", cmd);
                            connection.handler_mut().channel = Some(chan);
                            connection.exec(chan, false, &cmd);
                            // Wait until channel close.
                            debug!("waiting channel close");
                            connection.wait(move |session| {
                                session.handler().exit_status.is_some()
                            })
                        }))
                    .unwrap());

                let exit_code: Option<u32> = if let Some(ref mut session) = *session {
                    session.handler().exit_status
                } else {
                    None
                };

                debug!("exit_code = {:?}", exit_code);
                if let Some(ref mut session) = *session {
                    match std::mem::replace(&mut session.handler_mut().state, State::None) {
                        State::Changes { changes } => {
                            debug!("changes: {:?}", changes);
                            Ok(changes)
                        }
                        _ => unreachable!(),
                    }
                } else {
                    unreachable!()
                }
            }
            Session::Local { path } => {
                let repo_dir = pristine_dir(&path);
                let repo = Repository::open(&repo_dir, None)?;
                let txn = repo.txn_begin()?;
                Ok(if let Some(branch) = txn.get_branch(&branch) {
                    txn.iter_patches(&branch, None)
                        .map(|(hash, s)| (txn.get_external(hash).unwrap().to_owned(), s))
                        .collect()
                } else {
                    HashSet::new()
                })
            }
            Session::Uri { uri, ref mut client } => {
                let mut uri = uri.to_string();
                uri = uri + "/" + PIJUL_DIR_NAME + "/" + &branch_changes_base_path(branch);
                let mut res = try!(client.get(&uri)
                    .header(hyper::header::Connection::close())
                    .send());
                let changes = read_changes(&mut res).unwrap_or(HashSet::new());
                debug!("http: {:?}", changes);
                Ok(changes)
            }
        }
    }
    pub fn download_patch(&mut self,
                          repo_root: &Path,
                          patch_hash: &Hash)
                          -> Result<PathBuf, Error> {

        let local_file = patches_dir(repo_root).join(&patch_file_name(patch_hash.as_ref()));

        if metadata(&local_file).is_ok() {
            Ok(local_file)
        } else {
            match *self {
                Session::Local { path } => {
                    debug!("local downloading {:?}", patch_hash);
                    let remote_file = patches_dir(path).join(&patch_file_name(patch_hash.as_ref()));
                    // let local_file=patches_dir(repo_root).join(remote_file.file_name().unwrap());
                    debug!("hard linking {:?} to {:?}", remote_file, local_file);
                    try!(hard_link(&remote_file, &local_file)
                        .or_else(|_| copy(&remote_file, &local_file).and_then(|_| Ok(()))));
                    Ok(local_file)
                }
                Session::Ssh { ref mut l, ref path, ref mut session, .. } => {

                    let esc_path = escape(Cow::Borrowed(path));
                    let cmd = format!("pijul patch --repository {} {}",
                                      esc_path,
                                      patch_hash.to_base64(URL_SAFE));
                    debug!("cmd {:?} {:?}", cmd, local_file);
                    if let Some(ref mut session) = *session {
                        session.handler_mut().state =
                            State::DownloadPatch { file: try!(File::create(&local_file)) };
                        session.handler_mut().exit_status = None;
                        session.handler_mut().channel = None;
                    }
                    *session = Some(l.run(session.take()
                            .unwrap()
                            .channel_open_session()
                            .and_then(move |(mut connection, chan)| {

                                connection.handler_mut().channel = Some(chan);
                                connection.exec(chan, false, &cmd);
                                connection.wait(move |session| {
                                    session.handler().exit_status.is_some()
                                })
                            }))
                        .unwrap());

                    if let Some(ref mut session) = *session {

                        if let State::DownloadPatch { mut file } = std::mem::replace(&mut session.handler_mut().state, State::None) {
                            file.flush()?;
                        }
                    }
                    Ok(local_file)
                }
                Session::Uri { ref mut client, uri } => {

                    let uri =
                        uri.to_string() + "/" + PIJUL_DIR_NAME + "/" + PATCHES_DIR_NAME + "/" +
                        &patch_hash.to_base64(URL_SAFE) + ".gz";
                    debug!("downloading uri {:?}", uri);
                    let mut attempts = 0;
                    while attempts < HTTP_MAX_ATTEMPTS {
                        match client.get(&uri).header(hyper::header::Connection::close()).send() {
                            Ok(ref mut res) if res.status == hyper::status::StatusCode::Ok => {
                                debug!("response={:?}", res);
                                let mut body = Vec::new();
                                try!(res.read_to_end(&mut body));
                                let mut f = try!(File::create(&local_file));
                                try!(f.write_all(&body));
                                debug!("patch downloaded through http: {:?}", body);
                                return Ok(local_file);
                            }
                            Ok(_) => break,
                            Err(e) => {
                                debug!("error downloading : {:?}", e);
                                attempts += 1;
                            }
                        }
                    }
                    Err(Error::PatchNotFound(repo_root.to_str().unwrap().to_string(),
                                             patch_hash.to_owned()))
                }
            }
        }
    }

    fn remote_apply(&mut self,
                    repo_root: &Path,
                    remote_branch: &str,
                    patch_hashes: &HashSet<Hash>)
                    -> Result<(), Error> {

        match *self {
            Session::Ssh { ref mut l, ref mut session, ref path, .. } => {

                let pdir = patches_dir(repo_root);

                if let Some(ref mut session) = *session {
                    session.handler_mut().exit_status = None;
                }

                *session = Some(l.run(session.take()
                        .unwrap()
                        .channel_open_session()
                        .and_then(move |(mut session, chan)| {

                            let esc_path = escape(Cow::Borrowed(path));
                            session.exec(chan,
                                         false,
                                         &format!("pijul apply --repository {} --branch {:?}",
                                                  esc_path,
                                                  remote_branch));

                            let it = patch_hashes.iter().map(|x| {
                                let y: Result<_, Error> = Ok(x);
                                y
                            });
                            futures::stream::iter(it)
                                .fold((session, Vec::new()), move |(session, buf), hash| {
                                    let mut pdir = pdir.clone();
                                    pdir.push(hash.to_base64(URL_SAFE));
                                    pdir.set_extension("gz");
                                    let f = std::fs::File::open(&pdir).unwrap();
                                    pdir.pop();
                                    SendFile {
                                        f: f,
                                        buf: Some(buf),
                                        chan: chan,
                                        state: Some(SendFileState::Read(session)),
                                    }
                                })
                                .and_then(move |(mut session, _)| {
                                    session.channel_eof(chan);
                                    session.channel_close(chan);
                                    session.wait_flush().map_err(Error::from)
                                })
                                .map_err(From::from)
                        }))
                    .unwrap());

                Ok(())
            }
            Session::Local { path } => {
                let mut remote_path = patches_dir(path);
                let mut local_path = patches_dir(repo_root);

                for hash in patch_hashes {
                    remote_path.push(&hash.to_base64(URL_SAFE));
                    remote_path.set_extension("gz");

                    local_path.push(&hash.to_base64(URL_SAFE));
                    local_path.set_extension("gz");

                    debug!("hard linking {:?} to {:?}", local_path, remote_path);
                    if metadata(&remote_path).is_err() {
                        try!(hard_link(&local_path, &remote_path)
                            .or_else(|_| copy(&local_path, &remote_path).and_then(|_| Ok(()))))
                    }

                    local_path.pop();
                    remote_path.pop();
                }

                loop {
                    match apply_resize(&path, &remote_branch, patch_hashes.iter()) {
                        Err(ref e) if e.lacks_space() => {},
                        Ok(()) => return Ok(()),
                        Err(e) => return Err(From::from(e))
                    }
                }
            }
            _ => panic!("upload to URI impossible"),
        }
    }


    pub fn remote_init(&mut self) -> Result<(), Error> {
        match *self {
            Session::Ssh { ref mut l, ref mut session, ref path, .. } => {

                let esc_path = escape(Cow::Borrowed(path));
                let cmd = format!("pijul init {}", esc_path);
                debug!("command line:{:?}", cmd);

                if let Some(ref mut session) = *session {
                    session.handler_mut().exit_status = None
                }

                *session = Some(l.run(session.take()
                        .unwrap()
                        .channel_open_session()
                        .and_then(move |(mut session, chan)| {
                            debug!("chan = {:?}", chan);
                            session.handler_mut().channel = Some(chan);
                            session.exec(chan, false, &cmd);
                            // Wait until channel close.
                            session.wait(move |session| session.handler().exit_status.is_some())
                        }))
                    .unwrap());
                Ok(())
            }
            Session::Local { path } => {
                try!(init::run(&init::Params {
                    location: Some(path),
                    allow_nested: false,
                }));
                Ok(())
            }
            _ => panic!("remote init not possible"),
        }
    }

    pub fn pullable_patches(&mut self,
                            remote_branch: &str,
                            local_branch: &str,
                            target: &Path)
                            -> Result<Pullable, Error> {
        let remote_patches: HashSet<(Hash, ApplyTimestamp)> = try!(self.changes(remote_branch));
        let local_patches: HashSet<(Hash, ApplyTimestamp)> = {
            let repo_dir = pristine_dir(&target);
            let repo = Repository::open(&repo_dir, None)?;
            let txn = repo.txn_begin()?;
            if let Some(branch) = txn.get_branch(&local_branch) {
                txn.iter_patches(&branch, None)
                    .map(|(hash, s)| (txn.get_external(hash).unwrap().to_owned(), s))
                    .collect()
            } else {
                HashSet::new()
            }
        };
        debug!("pullable done: {:?}", remote_patches);
        Ok(Pullable {
            local: local_patches.iter().map(|&(ref h, _)| h.to_owned()).collect(),
            remote: remote_patches
        })
    }

    pub fn pull(&mut self,
                target: &Path,
                to_branch: &str,
                pullable: &[(Hash, ApplyTimestamp)])
                -> Result<(), Error> {

        for &(ref i, _) in pullable {
            try!(self.download_patch(&target, i));
        }
        debug!("patches downloaded");
        loop {
            debug!("apply_resize");
            match apply_resize(&target, &to_branch, pullable.iter().map(|&(ref h, _)| h)) {
                Err(ref e) if e.lacks_space() => {},
                Ok(()) => return Ok(()),
                Err(e) => return Err(From::from(e))
            }
        }
    }

    pub fn pushable_patches(&mut self,
                            from_branch: &str,
                            to_branch: &str,
                            source: &Path)
                            -> Result<Vec<(Hash, ApplyTimestamp)>, Error> {
        debug!("source: {:?}", source);
        let from_changes: HashSet<(Hash, ApplyTimestamp)> = {
            let repo_dir = pristine_dir(&source);
            let repo = Repository::open(&repo_dir, None)?;
            let txn = repo.txn_begin()?;
            if let Some(branch) = txn.get_branch(&from_branch) {
                txn.iter_patches(&branch, None)
                    .map(|(hash, s)| (txn.get_external(hash).unwrap().to_owned(), s))
                    .collect()
            } else {
                HashSet::new()
            }
        };
        debug!("pushing: {:?}", from_changes);
        let to_changes = try!(self.changes(to_branch));
        let to_changes:HashSet<Hash> = to_changes.into_iter().map(|(h, _)| h).collect();
        debug!("to_changes: {:?}", to_changes);

        Ok(from_changes.into_iter()
           .filter(|&(ref h, _)| !to_changes.contains(h))
           .collect())
    }

    pub fn push(&mut self,
                source: &Path,
                remote_branch: &str,
                pushable: &HashSet<Hash>)
                -> Result<(), Error> {
        debug!("push, remote_applying");
        debug!("pushable: {:?}", pushable);
        if pushable.len() > 0 {
            try!(self.remote_apply(source, remote_branch, pushable));
        }
        Ok(())
    }
}


impl<'a> Remote<'a> {
    pub fn session(&'a self) -> Result<Session<'a>, Error> {
        // fn from_remote(remote:&Remote<'a>) -> Result<Session<'a>,Error> {
        match *self {
            Remote::Local { ref path } => Ok(Session::Local { path: path.as_path() }),
            Remote::Uri { uri } => {
                Ok(Session::Uri {
                    uri: uri,
                    client: hyper::Client::with_connector(
                        HttpsConnector::new(hyper_rustls::TlsClient::new())
                    )
                })
            }
            Remote::Ssh { ref user, ref host, ref port, ref path, ref id } => {


                let addr = (*host, port.unwrap_or(22)).to_socket_addrs().unwrap().next().unwrap();
                debug!("addr = {:?}", addr);
                let mut l = tokio_core::reactor::Core::new().unwrap();
                let handle = l.handle();

                let config = std::sync::Arc::new(thrussh::client::Config::default());

                let handler = Client {
                    exit_status: None,
                    state: State::None,
                    port: port.unwrap_or(22),
                    host: host.to_string(),
                    channel: None,
                };

                let session = try!(l.run(tokio_core::net::TcpStream::connect(&addr, &handle)
                    .map_err(From::from)
                    .and_then(|socket| {

                        let connection =
                            thrussh::client::Connection::new(config.clone(), socket, handler, None)
                                .unwrap();
                        let key = {
                            let path =
                                std::env::home_dir().unwrap().join(".ssh").join("id_ed25519");
                            debug!("key path: {:?}", path);
                            thrussh::load_secret_key(&path, None).unwrap()
                        };
                        if let &Some(user) = user {
                            debug!("user = {:?}", user);
                            connection.authenticate_key(user, key)
                        } else {
                            let user = user::get_user_name().unwrap();
                            debug!("user = {:?}", user);
                            connection.authenticate_key(&user, key)
                        }
                    })));
                Ok(Session::Ssh {
                    l: l,
                    session: Some(session),
                    path: path,
                    id: id,
                })
            }
        }
    }
}


pub fn parse_remote<'a>(remote_id: &'a str,
                        port: Option<u16>,
                        base_path: Option<&'a Path>)
                        -> Remote<'a> {
    let ssh = Regex::new(r"^([^:]*):(.*)$").unwrap();
    let uri = Regex::new(r"^([:alpha:]*)://(.*)$").unwrap();
    if uri.is_match(remote_id) {
        let cap = uri.captures(remote_id).unwrap();
        if cap.get(1).unwrap().as_str() == "file" {
            if let Some(a) = base_path {
                let path = a.join(cap.get(2).unwrap().as_str());
                Remote::Local { path: path }
            } else {
                let path = Path::new(cap.get(2).unwrap().as_str()).to_path_buf();
                Remote::Local { path: path }
            }
        } else {
            Remote::Uri { uri: remote_id }
        }
    } else if ssh.is_match(remote_id) {
        let cap = ssh.captures(remote_id).unwrap();
        let user_host = cap.get(1).unwrap().as_str();

        let (user, host) = {
            let ssh_user_host = Regex::new(r"^([^@]*)@(.*)$").unwrap();
            if ssh_user_host.is_match(user_host) {
                let cap = ssh_user_host.captures(user_host).unwrap();
                (Some(cap.get(1).unwrap().as_str()), cap.get(2).unwrap().as_str())
            } else {
                (None, user_host)
            }
        };
        Remote::Ssh {
            user: user,
            host: host,
            port: port,
            path: cap.get(2).unwrap().as_str(),
            id: remote_id,
        }
    } else {
        if let Some(a) = base_path {
            let path = a.join(remote_id);
            Remote::Local { path: path }
        } else {
            let path = Path::new(remote_id).to_path_buf();
            Remote::Local { path: path }
        }
    }
}

#[derive(Debug)]
pub struct Pullable {
    pub local: HashSet<Hash>,
    pub remote: HashSet<(Hash, ApplyTimestamp)>,
}

use std::collections::hash_set::Iter;
// use std::collections::hash_map::RandomState;

pub struct PullableIterator<'a> {
    remote: Iter<'a, (Hash, ApplyTimestamp)>, // Difference<'a, Hash, RandomState>
    local: &'a HashSet<Hash>
}

impl Pullable {
    pub fn iter(&self) -> PullableIterator {
        PullableIterator {
            local: &self.local,
            remote: self.remote.iter()
        }
    }
}

impl<'a> Iterator for PullableIterator<'a> {
    type Item = (Hash, ApplyTimestamp);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(&(ref h, t)) = self.remote.next() {
            if !self.local.contains(h) {
                return Some((h.to_owned(), t))
            }
        }
        None
    }
}
