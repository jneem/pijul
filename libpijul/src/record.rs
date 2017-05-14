use backend::*;
use patch::*;
use error::*;
use graph;
use optimal_diff;

use std::path::{Path, PathBuf};
use std::fs::metadata;
use std;
use std::io::BufRead;
use rustc_serialize::hex::ToHex;
use rand;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;
use std::io::Read;
use std::rc::Rc;

#[cfg(not(windows))]
fn permissions(attr: &std::fs::Metadata) -> Option<usize> {
    Some(attr.permissions().mode() as usize)
}
#[cfg(windows)]
fn permissions(_: &std::fs::Metadata) -> Option<usize> {
    None
}


fn file_metadata(path: &Path) -> Result<FileMetadata, Error> {
    let attr = metadata(&path)?;
    let permissions = permissions(&attr).unwrap_or(0o755);
    Ok(FileMetadata::new(permissions, attr.is_dir()))
}


struct RecordState {
    line_num: LineId,
    updatables: Vec<InodeUpdate>,
    actions: Vec<Record>,
    redundant: Vec<(Key<PatchId>, Edge)>,
}

#[derive(Debug)]
pub enum InodeUpdate {
    Add {
        line: LineId,
        meta: FileMetadata,
        inode: Inode,
    },
    Moved { inode: Inode },
    Deleted { inode: Inode },
}

pub enum WorkingFileStatus {
    Moved {from: FileMetadata, to: FileMetadata},
    Deleted,
    Ok,
}

fn is_text(x: &[u8]) -> bool {
    x.iter().take(8000).all(|&c| c != 0)
}

impl<A: Transaction, R: rand::Rng> T<A, R> {
    /// Create appropriate NewNodes for adding a file.
    fn record_file_addition(&self,
                            st: &mut RecordState,
                            current_inode: &Inode,
                            parent_node: &Key<Option<PatchId>>,
                            realpath: &mut std::path::PathBuf,
                            basename: &str)
                            -> Result<Option<LineId>, Error> {


        let name_line_num = st.line_num.clone();
        let blank_line_num = st.line_num + 1;
        st.line_num += 2;

        debug!("metadata for {:?}", realpath);
        let meta = file_metadata(&realpath)?;

        let mut name = Vec::with_capacity(basename.len() + 2);
        name.write_metadata(meta).unwrap(); // 2 bytes.
        name.extend(basename.as_bytes());

        let mut nodes = Vec::new();

        st.updatables.push(InodeUpdate::Add {
            line: blank_line_num.clone(),
            meta: meta,
            inode: current_inode.clone(),
        });

        st.actions.push(Record::FileAdd {
            name: realpath.to_string_lossy().to_string(),
            add: Change::NewNodes {
                up_context: vec![Key {
                                     patch: if parent_node.line.is_root() {
                                         Some(Hash::None)
                                     } else if let Some(ref patch_id) = parent_node.patch {
                                         Some(self.external_hash(patch_id).to_owned())
                                     } else {
                                         None
                                     },
                                     line: parent_node.line.clone(),
                                 }],
                line_num: name_line_num,
                down_context: vec![],
                nodes: vec![name, vec![]],
                flag: FOLDER_EDGE,
            },
        });

        // Reading the file
        if !meta.is_dir() {
            nodes.clear();

            let mut node = Vec::new();
            {
                let mut f = std::fs::File::open(realpath.as_path())?;
                f.read_to_end(&mut node)?;
            }

            if is_text(&node) {
                let mut line = Vec::new();
                let mut f = &node[..];
                loop {
                    match f.read_until('\n' as u8, &mut line) {
                        Ok(l) => {
                            if l > 0 {
                                nodes.push(line.clone());
                                line.clear()
                            } else {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let len = nodes.len();
                if !nodes.is_empty() {
                    st.actions.push(Record::Change {
                        change: Change::NewNodes {
                            up_context: vec![Key {
                                                 patch: None,
                                                 line: blank_line_num.clone(),
                                             }],
                            line_num: st.line_num,
                            down_context: vec![],
                            nodes: nodes,
                            flag: EdgeFlags::empty(),
                        },
                        file: Rc::new(realpath.clone()),
                        conflict_reordering: Vec::new(),
                    });
                }
                st.line_num += len;
            } else {
                st.actions.push(Record::Change {
                    change: Change::NewNodes {
                        up_context: vec![Key {
                                             patch: None,
                                             line: blank_line_num.clone(),
                                         }],
                        line_num: st.line_num,
                        down_context: vec![],
                        nodes: vec![node],
                        flag: EdgeFlags::empty(),
                    },
                    file: Rc::new(realpath.clone()),
                    conflict_reordering: Vec::new(),
                });
                st.line_num += 1;
            }
            Ok(None)
        } else {
            Ok(Some(blank_line_num))
        }
    }

    /// Diff for binary files, doesn't both splitting the file in
    /// lines. This is wasteful, but doesn't break the format, and
    /// doesn't create conflicts inside binary files.
    fn diff_with_binary(&self,
                        branch: &Branch,
                        st: &mut RecordState,
                        ret: &mut graph::Graph,
                        path: Rc<PathBuf>)
                        -> Result<(), Error> {

        let mut lines_b = Vec::new();
        {
            let mut f = std::fs::File::open(path.as_ref())?;
            f.read_to_end(&mut lines_b)?;
        }
        let lines = if is_text(&lines_b) {
            optimal_diff::read_lines(&lines_b)
        } else {
            vec![&lines_b[..]]
        };

        self.diff(branch,
                  &path,
                  &mut st.line_num,
                  &mut st.actions,
                  &mut st.redundant,
                  ret,
                  &lines)
    }

    fn record_moved_file(&self,
                         branch: &Branch,
                         realpath: &mut std::path::PathBuf,
                         st: &mut RecordState,
                         parent_node: &Key<Option<PatchId>>,
                         current_node: &Key<PatchId>,
                         basename: &str,
                         new_meta: FileMetadata,
                         old_meta: FileMetadata)
                         -> Result<(), Error> {
        // Delete all former names.
        let mut edges = Vec::new();
        // Now take all grandparents of l2, delete them.

        let mut name = Vec::with_capacity(basename.len() + 2);
        name.write_metadata(new_meta).unwrap();
        name.extend(basename.as_bytes());
        for parent in iterate_parents!(self, branch, current_node, FOLDER_EDGE) {
            debug!("iterate_parents: {:?}", parent);
            let previous_name: &[u8] = match self.get_contents(&parent.dest) {
                None => &[],
                Some(n) => n.as_slice(),
            };
            let name_changed =
                (&previous_name[2..] != &name[2..])
                || (new_meta != old_meta && cfg!(not(windows)));

            for grandparent in iterate_parents!(self, branch, &parent.dest, FOLDER_EDGE) {
                debug!("iterate_parents: grandparent = {:?}", grandparent);
                let grandparent_changed = if let Some(ref parent_node) = parent_node.patch {
                    *parent_node != grandparent.dest.patch
                } else {
                    debug_assert!(parent_node.line.is_root());
                    grandparent.dest != ROOT_KEY
                };
                if grandparent_changed || name_changed {
                    edges.push(NewEdge {
                        from: Key {
                            line: parent.dest.line.clone(),
                            patch: Some(self.external_hash(&parent.dest.patch).to_owned()),
                        },
                        to: Key {
                            line: grandparent.dest.line.clone(),
                            patch: Some(self.external_hash(&grandparent.dest.patch).to_owned()),
                        },
                        introduced_by: Some(self.external_hash(&grandparent.introduced_by)
                            .to_owned()),
                    })
                }
            }
        }
        debug!("edges:{:?}", edges);
        if !edges.is_empty() {
            // If this file's name or meta info has changed.
            st.actions.push(Record::FileMove {
                new_name: realpath.to_string_lossy().to_string(),
                del: Change::NewEdges {
                    edges: edges,
                    op: EdgeOp::Map {
                        previous: FOLDER_EDGE | PARENT_EDGE,
                        flag: DELETED_EDGE | FOLDER_EDGE | PARENT_EDGE,
                    },
                },
                add: Change::NewNodes {
                    up_context: vec![Key {
                                         patch: if parent_node.line.is_root() {
                                             Some(Hash::None)
                                         } else if let Some(parent_patch) =
                                             parent_node.patch
                                                 .as_ref() {
                                             Some(self.external_hash(parent_patch).to_owned())
                                         } else {
                                             None
                                         },
                                         line: parent_node.line.clone(),
                                     }],
                    line_num: st.line_num,
                    down_context: vec![Key {
                                           patch: Some(self.external_hash(&current_node.patch)
                                               .to_owned()),
                                           line: current_node.line.clone(),
                                       }],
                    nodes: vec![name],
                    flag: FOLDER_EDGE,
                },
            });
            st.line_num += 1;
        }
        // debug!("directory_flag:{}", old_attr & DIRECTORY_FLAG);
        if !old_meta.is_dir() {
            info!("retrieving");
            let mut ret = self.retrieve(branch, current_node);
            debug!("diff");
            try!(self.diff_with_binary(branch, st, &mut ret, Rc::new(realpath.clone())));
        };
        Ok(())
    }

    fn record_deleted_file(&self,
                           st: &mut RecordState,
                           branch: &Branch,
                           realpath: &Path,
                           current_node: &Key<PatchId>)
                           -> Result<(), Error> {
        debug!("record_deleted_file");
        let mut edges = Vec::new();
        // Now take all grandparents of the current node, delete them.
        for parent in iterate_parents!(self, branch, current_node, FOLDER_EDGE) {
            for grandparent in iterate_parents!(self, branch, &parent.dest, FOLDER_EDGE) {
                edges.push(NewEdge {
                    from: self.external_key(&parent.dest).unwrap(),
                    to: self.external_key(&grandparent.dest).unwrap(),
                    introduced_by: Some(self.external_hash(&grandparent.introduced_by).to_owned()),
                })
            }
        }
        // Delete the file recursively
        let mut file_edges = vec![];
        {
            debug!("del={:?}", current_node);
            let ret = self.retrieve(branch, &current_node);
            for l in ret.lines.iter() {
                if l.key != ROOT_KEY {
                    let ext_key = self.external_key(&l.key).unwrap();
                    debug!("ext_key={:?}", ext_key);
                    for v in iterate_parents!(self, branch, &l.key, EdgeFlags::empty()) {

                        debug!("v={:?}", v);
                        file_edges.push(NewEdge {
                            from: ext_key.clone(),
                            to: self.external_key(&v.dest).unwrap(),
                            introduced_by: Some(self.external_hash(&v.introduced_by)
                                .to_owned()),
                        });
                    }
                    for v in iterate_parents!(self, branch, &l.key, FOLDER_EDGE) {

                        debug!("v={:?}", v);
                        edges.push(NewEdge {
                            from: ext_key.clone(),
                            to: self.external_key(&v.dest).unwrap(),
                            introduced_by: Some(self.external_hash(&v.introduced_by)
                                .to_owned()),
                        });
                    }
                }
            }
        }

        st.actions.push(Record::FileDel {
            name: realpath.to_string_lossy().to_string(),
            del: Change::NewEdges {
                edges: edges,
                op: EdgeOp::Map {
                    previous: FOLDER_EDGE | PARENT_EDGE,
                    flag: FOLDER_EDGE | PARENT_EDGE | DELETED_EDGE,
                },
            },
        });
        if file_edges.len() > 0 {
            st.actions.push(Record::Change {
                change: Change::NewEdges {
                    edges: file_edges,
                    op: EdgeOp::Map {
                        previous: PARENT_EDGE,
                        flag: PARENT_EDGE | DELETED_EDGE,
                    },
                },
                file: Rc::new(realpath.to_path_buf()),
                conflict_reordering: Vec::new(),
            });
        };
        Ok(())
    }

    fn record_root(&self, branch: &Branch, st: &mut RecordState, basepath : &mut PathBuf)
                   -> Result<(), Error> {
        let key = Key { patch: None, line: LineId::new() };
        self.record_children(branch, st, basepath, &key, &ROOT_INODE)
    }

    fn record_children(&self, branch: &Branch, st: &mut RecordState, path: &mut std::path::PathBuf,
                       current_node: &Key<Option<PatchId>>, current_inode: &Inode)
        -> Result<(), Error>
    {
        debug!("children of current_inode {}", current_inode.to_hex());
        let file_id = OwnedFileId {
            parent_inode: current_inode.clone(),
            basename: SmallString::from_str(""),
        };
        debug!("iterating tree, starting from {:?}", file_id.as_file_id());
        for (k, v) in self.iter_tree(Some((&file_id.as_file_id(), None)))
            .take_while(|&(ref k, _)| k.parent_inode == current_inode) {
                debug!("calling record_all recursively, {}", line!());

                if k.basename.len() > 0 {
                    // If this is an actual file and not just the "."
                    self.record_inode(
                        branch,
                        st,
                        current_node.clone(), // parent
                        v, // current_inode
                        path,
                        k.basename.as_str()
                    )?
                }
            };
        Ok(())
    }

    fn inode_status(&self, inode : &Inode, path: &Path)
                    -> (Option<(WorkingFileStatus, FileHeader)>) {
        match self.get_inodes(inode) {
            Some(file_header) => {
                let old_meta = file_header.metadata;
                let new_meta = file_metadata(path).ok();

                debug!("current_node={:?}", file_header);
                debug!("old_attr={:?},int_attr={:?}", old_meta, new_meta);

                let status =
                    match (new_meta, file_header.status) {
                        (Some(new_meta), FileStatus::Moved) =>
                            WorkingFileStatus::Moved { from: old_meta, to: new_meta },
                        (Some(new_meta), _) if old_meta != new_meta =>
                            WorkingFileStatus::Moved { from: old_meta, to: new_meta },
                        (None, _) |
                        (_, FileStatus::Deleted) =>
                            WorkingFileStatus::Deleted,
                        (Some(_), FileStatus::Ok) =>
                            WorkingFileStatus::Ok,
                    };
                Some((status, file_header.clone()))
            },
            None => None
        }
    }


    fn record_inode(&self,
                    branch: &Branch,
                    st: &mut RecordState,
                    parent_node: Key<Option<PatchId>>,
                    current_inode: &Inode,
                    realpath: &mut std::path::PathBuf,
                    basename: &str)
                    -> Result<(), Error> {
        realpath.push(basename);
        debug!("realpath: {:?}", realpath);
        debug!("inode: {:?}", current_inode);
        debug!("header: {:?}", self.get_inodes(current_inode));
        let status_header = self.inode_status(current_inode, realpath);

        let mut current_key =
            match &status_header {
                &Some ((_, ref file_header)) =>
                              { Some(Key {
                                  patch: Some(file_header.key.patch.clone()),
                                  line: file_header.key.line.clone(),
                              })},
                &None => None};

        match status_header {
            Some((WorkingFileStatus::Moved {from: old_meta, to: new_meta}, file_header)) => {
                st.updatables.push(InodeUpdate::Moved { inode: current_inode.clone() });
                self.record_moved_file(branch, realpath, st, &parent_node, &file_header.key,
                                       basename, new_meta, old_meta)?
            },
            Some((WorkingFileStatus::Deleted, file_header)) => {
                st.updatables.push(InodeUpdate::Deleted { inode: current_inode.clone() });
                self.record_deleted_file(st, branch, realpath, &file_header.key)?
            },
            Some((WorkingFileStatus::Ok, file_header)) => {
                if !file_header.metadata.is_dir() {
                    let mut ret = self.retrieve(branch, &file_header.key);
                    info!("now calling diff {:?}", file_header.key);
                    debug!("ret = {:?}", ret);
                    self.diff_with_binary(branch, st, &mut ret, Rc::new(realpath.clone()))?;
                }
            },
            None => {
                let new_key = self.record_file_addition(st, &current_inode, &parent_node, realpath,
                                                        basename)?;
                current_key = new_key.map(|next| {Key {patch: None, line: next}})
            },

        }

        let current_key = current_key;
        debug!("current_node={:?}", current_key);
        if let Some(current_node) = current_key {
            self.record_children(branch, st, realpath, &current_node, current_inode)?;
        };
        realpath.pop();
        Ok(())
    }
}

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    pub fn record(&mut self,
                  branch_name: &str,
                  working_copy: &std::path::Path)
                  -> Result<(Vec<Record>, Vec<InodeUpdate>), Error> {

        let branch = try!(self.open_branch(branch_name));
        let mut st = RecordState {
            line_num: LineId::new() + 1,
            actions: Vec::new(),
            updatables: Vec::new(),
            redundant: Vec::new(),
        };
        {
            let mut realpath = PathBuf::from(working_copy);
            self.record_root(&branch, &mut st, &mut realpath)?;
            debug!("record done, {} changes", st.actions.len());
            debug!("changes: {:?}", st.actions);
        }
        // try!(self.remove_redundant_edges(&mut branch, &mut st.redundant));
        try!(self.commit_branch(branch));
        debug!("remove_redundant_edges done");
        Ok((st.actions, st.updatables))
    }
}
