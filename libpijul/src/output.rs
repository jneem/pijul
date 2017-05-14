use backend::*;
use patch::*;
use record::InodeUpdate;
use error::Error;

use rustc_serialize::hex::ToHex;
use rustc_serialize::base64::{ToBase64, URL_SAFE};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std;
use std::fs;
use rand;
impl<'env, T: rand::Rng> MutTxn<'env, T> {
    // Climp up the tree (using revtree).
    fn filename_of_inode(&self, inode: &Inode, working_copy: &Path) -> Option<PathBuf> {
        let mut components = Vec::new();
        let mut current = inode.clone();
        loop {
            match self.get_revtree(&current) {
                Some(v) => {
                    components.push(v.basename.to_owned());
                    current = v.parent_inode.clone();
                    if current == ROOT_INODE {
                        break;
                    }
                }
                None => return None,
            }
        }
        let mut working_copy = working_copy.to_path_buf();
        for c in components.iter().rev() {
            working_copy.push(c.as_small_str().as_str());
        }
        Some(working_copy)
    }


    /// Returns the path's inode
    pub fn follow_path(&self, path: &[&str]) -> Result<Option<Inode>, Error> {
        // follow in tree, return inode
        let mut buf = OwnedFileId {
            parent_inode: ROOT_INODE.clone(),
            basename: SmallString::from_str(""),
        };
        for p in path {
            buf.basename.clear();
            buf.basename.push_str(*p);
            // println!("follow: {:?}",buf.to_hex());
            match self.get_tree(&buf.as_file_id()) {
                Some(v) => {
                    // println!("some: {:?}",v.to_hex());
                    buf.basename.clear();
                    buf.parent_inode = v.clone()
                }
                None => {
                    // println!("none");
                    return Ok(None);
                }
            }
        }
        Ok(Some(buf.parent_inode))
    }



    /// Collect all the children of key `key` into `files`.
    pub fn collect_children(&mut self,
                            branch: &Branch,
                            path: &Path,
                            key: &Key<PatchId>,
                            inode: &Inode,
                            files: &mut HashMap<PathBuf,
                                                Vec<(Inode,
                                                     FileMetadata,
                                                     Key<PatchId>,
                                                     Option<Inode>)>>) {

        let e = Edge::zero(FOLDER_EDGE);
        for (_, b) in self.iter_nodes(&branch, Some((key, Some(&e))))
            .take_while(|&(k, b)| k == key && b.flag <= FOLDER_EDGE | PSEUDO_EDGE) {

            // debug_assert!(b.len() == 1 + KEY_SIZE + HASH_SIZE);
            debug!("b={:?}", b);
            let cont_b = self.get_contents(&b.dest).unwrap();

            // This is supposed to be a small string anyway.
            let (perms, basename) = cont_b.as_slice().split_at(2);

            let perms = FileMetadata::from_contents(perms);
            let basename = std::str::from_utf8(basename).unwrap();
            debug!("filename: {:?} {:?}", perms, basename);
            let name = path.join(basename);

            let mut children = self.iter_nodes(&branch, Some((&b.dest, Some(&e))))
                .take_while(|&(k, c)| k == &b.dest && c.flag <= FOLDER_EDGE | PSEUDO_EDGE);

            if let Some((_, c)) = children.next() {

                let v = files.entry(name).or_insert(Vec::new());
                v.push((inode.clone(),
                        perms,
                        c.dest.clone(),
                        self.get_revinodes(&c.dest).map(|x| x.to_owned())))
            } else {
                panic!("File name doesn't point to a file")
            }
            // The following assertion means that a name, as
            // introduced by a patch, can point to at most one
            // file.
            assert!(children.next().is_none());

            debug!("/b");
        }
    }



    fn output_repository_assuming_no_pending_patch(&mut self,
                                                   branch: &Branch,
                                                   working_copy: &Path,
                                                   pending_patch_id: PatchId)
                                                   -> Result<(), Error> {


        {
            let mut files = HashMap::new();
            let mut next_files = HashMap::new();
            self.collect_children(branch, working_copy, &ROOT_KEY, &ROOT_INODE, &mut files);
            while !files.is_empty() {
                debug!("files {:?}", files);
                next_files.clear();
                for (a, b) in files.drain() {
                    let b_len = b.len();
                    for (parent_inode, meta, key, inode) in b {

                        let name = if b_len <= 1 {
                            a.clone()
                        } else {
                            let ext = self.get_external(&key.patch).unwrap().to_base64(URL_SAFE);
                            let mut name = a.clone();
                            let basename = {
                                let basename = name.file_name().unwrap().to_string_lossy();
                                format!("{}.{}", basename, &ext[..10])
                            };
                            name.set_file_name(&basename);
                            name
                        };
                        let inode = if let Some(inode) = inode {
                            // If the file already exists, find its
                            // current name and rename it if that name
                            // is different.
                            match self.filename_of_inode(&inode, working_copy) {
                                Some(ref current_name) if current_name != &name => {
                                    try!(std::fs::rename(current_name, &name))
                                }
                                _ => {}
                            }
                            inode
                        } else {
                            // Else, create new inode.
                            let inode = self.create_new_inode();
                            let file_header = FileHeader {
                                key: key.clone(),
                                metadata: meta,
                                status: FileStatus::Ok
                            };
                            try!(self.replace_inodes(&inode, &file_header));
                            try!(self.replace_revinodes(&key, &inode));
                            let file_name = name.file_name().unwrap().to_string_lossy();
                            let file_id = OwnedFileId {
                                parent_inode: parent_inode.clone(),
                                basename: SmallString::from_str(&file_name)
                            };
                            try!(self.put_tree(&file_id.as_file_id(), &inode));
                            try!(self.put_revtree(&inode, &file_id.as_file_id()));
                            inode
                        };

                        if meta.is_dir() {
                            // This is a directory, register it in inodes/trees.
                            try!(std::fs::create_dir_all(&name));
                            self.collect_children(branch, &a, &key, &inode, &mut next_files);
                        } else {
                            // Output file.
                            let mut redundant_edges = Vec::new();
                            let mut l = self.retrieve(branch, &key);
                            debug!("creating file {:?}", &name);
                            let mut f = std::fs::File::create(&name).unwrap();
                            debug!("done");
                            try!(self.output_file(&mut f, &mut l, &mut redundant_edges));
                        }
                    }
                }
                std::mem::swap(&mut files, &mut next_files);
            }
        }

        let test: Vec<_> = self.iter_inodes(None)
            .map(|(u, v)| (u.to_owned(), v.to_owned()))
            .collect();
        debug!("inodes: {:?}", test);
        // Now, garbage collect dead inodes.
        let dead: Vec<(Inode, _)> = self.iter_inodes(None)
            .filter(|&(_, v)| {
                // A file is deleted in a branch if it doesn't have an alive name.
                // Test whether v has an alive name.
                debug!("v.key: {:?}", v.key);
                // Else, check whether the file is still alive/pseudo alive.
                for parent in iterate_parents!(self, branch, &v.key, FOLDER_EDGE) {
                    debug!("parent: {:?}", parent);
                    if self.has_edge(branch, &parent.dest, PARENT_EDGE | FOLDER_EDGE, true) {
                        return false;
                    }
                }
                true
            })
            .map(|(u, v)| {
                // If it was introduced by the pending patch, don't delete this file.
                if v.key.patch == pending_patch_id {
                    (u.to_owned(), None)
                } else {
                    (u.to_owned(), self.filename_of_inode(u, working_copy))
                }
            })
            .collect();
        debug!("dead: {:?}", dead);


        // Now, "kill the deads"
        for (ref inode, ref name) in dead {
            try!(self.remove_inode_rec(inode));
            debug!("removed");
            if let Some(ref name) = *name {
                debug!("deleting {:?}", name);
                let meta = try!(fs::metadata(name));
                if meta.is_dir() {
                    try!(fs::remove_dir_all(name))
                } else {
                    try!(fs::remove_file(name))
                }
            }
        }
        debug!("done raw_output_repository");
        Ok(())
    }

    fn remove_inode_rec(&mut self, inode: &Inode) -> Result<(), Error> {
        debug!("kill dead {:?}", inode.to_hex());
        // Remove the inode from inodes/revinodes.
        let header = self.get_inodes(inode).map(|x| x.to_owned());

        if let Some(header) = header {
            try!(self.del_inodes(inode, None));
            try!(self.del_revinodes(&header.key, None));
            let mut kills = Vec::new();
            // Remove the inode from tree/revtree.
            for (k, v) in self.iter_revtree(Some((&inode, None)))
                .take_while(|&(k, _)| k == inode) {
                kills.push((k.clone(), v.to_owned()))
            }
            for &(ref k, ref v) in kills.iter() {
                try!(self.del_tree(&v.as_file_id(), Some(&k)));
                try!(self.del_revtree(&k, Some(&v.as_file_id())));
            }
            // If the dead is a directory, remove its descendants.
            let inode_fileid = OwnedFileId {
                parent_inode: inode.clone(),
                basename: SmallString::from_str(""),
            };
            let descendants: Vec<_> = self.iter_tree(Some((&inode_fileid.as_file_id(), None)))
                .map(|(_, v)| v.to_owned())
                .collect();
            for inode in descendants.iter() {
                try!(self.remove_inode_rec(inode))
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn output_repository(&mut self,
                             branch_name: &str,
                             working_copy: &Path,
                             pending: &Patch,
                             local_pending: &[InodeUpdate])
                             -> Result<(), Error> {
        debug!("begin output repository");

        debug!("applying pending patch");
        let (_, internal) = self.apply_local_patch(branch_name, working_copy, pending, local_pending, true)?;

        debug!("applied");
        let mut branch = try!(self.open_branch(branch_name));
        try!(self.output_repository_assuming_no_pending_patch(&branch, working_copy, internal));

        debug!("unrecording pending patch");
        self.unrecord(&mut branch, &internal, pending)?;
        self.commit_branch(branch)?;
        Ok(())
    }
}
