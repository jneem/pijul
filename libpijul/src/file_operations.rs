use backend::*;
use backend;
use error::*;


use rand;
use std;
use std::path::{Path, PathBuf};
use rustc_serialize::hex::ToHex;
use std::iter::Iterator;
use std::collections::BTreeMap;

impl<'env, R: rand::Rng> MutTxn<'env, R> {
    fn mark_inode_moved(&mut self, inode: &Inode) {

        let mut header = None;
        if let Some(h) = self.get_inodes(inode) {
            header = Some(h.clone())
        }
        if let Some(mut h) = header {
            h.status = FileStatus::Moved;
            self.replace_inodes(inode, &h).unwrap();
        }
    }

    /// Create an inode that doesn't exist in the repository, but
    /// doesn't put it into the repository.
    pub fn create_new_inode(&self) -> Inode {
        let mut already_taken = true;
        let mut inode: Inode = ROOT_INODE.clone();
        while already_taken {
            for i in inode.iter_mut() {
                *i = rand::random()
            }
            already_taken = self.get_revtree(&inode).is_some();
        }
        inode
    }


    /// Record the information that `parent_inode` is now a parent of
    /// file `filename`, and `filename` has inode `child_inode`.
    fn make_new_child(&mut self,
                      parent_inode: &Inode,
                      filename: &str,
                      is_dir: bool,
                      child_inode: Option<&Inode>)
                      -> Result<Inode, Error> {

        let parent_id = OwnedFileId {
            parent_inode: parent_inode.clone(),
            basename: SmallString::from_str(filename),
        };
        let child_inode = match child_inode {
            None => self.create_new_inode(),
            Some(i) => i.clone(),
        };
        // Avoid inserting a name that already exists.
        if self.get_tree(&parent_id.as_file_id()).is_none() {
            try!(self.put_tree(&parent_id.as_file_id(), &child_inode));
            try!(self.put_revtree(&child_inode, &parent_id.as_file_id()));

            if is_dir {
                // If this is a directory, add a name-less file id without
                // a reverse in revtree.
                let dir_id = OwnedFileId {
                    parent_inode: child_inode.clone(),
                    basename: SmallString::from_str(""),
                };
                try!(self.put_tree(&dir_id.as_file_id(), &child_inode));
            };
            Ok(child_inode)
        } else {
            Err(Error::AlreadyAdded)
        }
    }

    pub fn add_inode(&mut self,
                     inode: Option<&Inode>,
                     path: &std::path::Path,
                     is_dir: bool)
                     -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            let (mut current_inode, unrecorded_path) = self.closest_in_repo_ancestor(&parent).unwrap();

            for c in unrecorded_path {
                current_inode = try!(self.make_new_child(&current_inode,
                                                         c.as_os_str().to_str().unwrap(),
                                                         true,
                                                         None))
            }

            try!(self.make_new_child(&current_inode,
                                     path.file_name().unwrap().to_str().unwrap(),
                                     is_dir,
                                     inode));
        }
        Ok(())
    }

    pub fn move_file(&mut self,
                     path: &std::path::Path,
                     path_: &std::path::Path,
                     is_dir: bool)
                     -> Result<(), Error> {
        debug!("move_file: {:?},{:?}", path, path_);
        if let Some(parent) = path.parent() {
            let fileref = OwnedFileId {
                parent_inode: try!(self.find_inode(parent)),
                basename: SmallString::from_str(path.file_name().unwrap().to_str().unwrap()),
            };

            if let Some(inode) = self.get_tree(&fileref.as_file_id()).map(|i| i.clone()) {

                // Now the last inode is in "*inode"
                debug!("txn.del fileref={:?}", fileref);
                self.del_tree(&fileref.as_file_id(), None)?;
                self.del_revtree(&inode, None)?;

                debug!("inode={} path_={:?}", inode.to_hex(), path_);
                try!(self.add_inode(Some(&inode), path_, is_dir));
                self.mark_inode_moved(&inode);

                return Ok(())

            }
        }
        Err(Error::FileNotInRepo(path.to_path_buf()))
    }

    // Deletes a directory, given by its inode, recursively.
    fn rec_delete(&mut self, key: &Inode) -> Result<(), Error> {

        debug!("rec_delete, key={:?}", key.to_hex());
        let file_id = OwnedFileId {
            parent_inode: key.clone(),
            basename: SmallString::from_str(""),
        };
        let children: Vec<(_, Inode)> = self.iter_tree(Some((&file_id.as_file_id(), None)))
            .take_while(|&(ref k, _)| key == k.parent_inode)
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        {
            for (a, b) in children {
                try!(self.rec_delete(&b));
                println!("deleting {:?} {:?}", a, b);
                debug!("deleting from tree");
                try!(self.del_tree(&a.as_file_id(), Some(&b)));
                try!(self.del_revtree(&b, Some(&a.as_file_id())));
                debug!("done deleting from tree");
            }
        }

        // Now that the directory is empty, mark the corresponding node as deleted (flag '2').
        debug!("b");
        if let Some(mut header) = self.get_inodes(key).map(|h| h.clone()) {
            // If this is was recorded, mark deleted.
            header.status = FileStatus::Deleted;
            try!(self.replace_inodes(key, &header));
        } else {
            // Else, simply delete from the tree.
            let parent = self.get_revtree(key).unwrap().to_owned();
            try!(self.del_tree(&parent.as_file_id(), None));
            try!(self.del_revtree(key, None));
        }
        Ok(())
    }

    /// Removes a file from the repository.
    pub fn remove_file(&mut self, path: &std::path::Path) -> Result<(), Error> {
        debug!("remove_file");
        let inode = try!(self.find_inode(path));
        debug!("rec_delete");
        try!(self.rec_delete(&inode));
        debug!("/rec_delete");
        Ok(())
    }
}

impl<A: Transaction, R> backend::T<A, R> {
    /// Traverses the `tree` base recursively, collecting all descendants of `key`.
    fn collect(&self,
               key: &Inode,
               pb: &Path,
               basename: &str,
               files: &mut Vec<PathBuf>)
               -> Result<(), Error> {
        debug!("collecting {:?},{:?}", key, basename);
        let add = match self.get_inodes(key) {
            Some(inode) => {
                debug!("node = {:?}", inode);
                inode.status != FileStatus::Deleted
            }
            None => true,
        };
        if add {
            debug!("basename = {:?}", basename);
            let next_pb = pb.join(basename);
            let next_pb_ = next_pb.clone();
            if basename.len() > 0 {
                files.push(next_pb)
            }

            debug!("starting iterator, key={:?}", key);
            let fileid = OwnedFileId {
                parent_inode: key.clone(),
                basename: SmallString::from_str(""),
            };
            for (k, v) in self.iter_tree(Some((&fileid.as_file_id(), None)))
                .take_while(|&(ref k, _)| k.parent_inode == key) {
                    debug!("iter: {:?} {:?}", k, v);
                    if k.basename.len() > 0 {
                        try!(self.collect(v, next_pb_.as_path(), k.basename.as_str(), files));
                    }
                }
            debug!("ending iterator {:?}", { let v: Vec<_> = self.iter_tree(Some((&fileid.as_file_id(), None))).collect(); v });
        }
        Ok(())
    }

    /// Returns a vector containing all files in the repository.
    pub fn list_files(&self) -> Result<Vec<PathBuf>, Error> {
        let mut files = Vec::new();
        let mut pathbuf = PathBuf::new();
        try!(self.collect(&ROOT_INODE, &mut pathbuf, "", &mut files));
        Ok(files)
    }

    /// Returns a list of files under the given inode.
    pub fn list_files_under_inode(&self,
                                  inode: &Inode)
                                  -> Vec<(SmallString, Option<Key<PatchId>>, Inode)> {

        let mut result = Vec::new();

        let file_id = OwnedFileId {
            parent_inode: inode.clone(),
            basename: SmallString::from_str(""),
        };
        for (k, v) in self.iter_tree(Some((&file_id.as_file_id(), None)))
            .take_while(|&(ref k, _)| k.parent_inode == inode) {

            let header = self.get_inodes(k.parent_inode).map(|x| x.clone());
                // add: checking that this file has neither been moved nor deleted.
                println!("============= {:?} {:?}", k, v);
            let add = match header {
                Some(ref h) => h.status == FileStatus::Ok,
                None => true,
            };
            if add && k.basename.len() > 0 {
                result.push((k.basename.to_owned(), header.map(|h| h.key.clone()), v.clone()))
            }
        }

        result
    }

    /// Returns a list of files under the given inode.
    pub fn list_files_under_node(&self,
                                 branch: &Branch,
                                 key: &Key<PatchId>)
                                 -> BTreeMap<Key<PatchId>, Vec<(FileMetadata, &str)>> {

        let mut result = BTreeMap::new();

        let e = Edge::zero(FOLDER_EDGE);
        for (_, child) in self.iter_nodes(branch, Some((key, Some(&e))))
            .take_while(|&(ref k, ref v)| *k == key && v.flag <= FOLDER_EDGE|PSEUDO_EDGE) {

                let name = self.get_contents(&child.dest).unwrap();
                // This is supposed to be a small string anyway.
                let (perms, basename) = name.as_slice().split_at(2);
                let perms = FileMetadata::from_contents(perms);
                let basename = std::str::from_utf8(basename).unwrap();

                for (_, grandchild) in self.iter_nodes(branch, Some((&child.dest, Some(&e))))
                    .take_while(|&(k, ref v)| *k == child.dest && v.flag <= FOLDER_EDGE|PSEUDO_EDGE) {


                        let names = result.entry(grandchild.dest.to_owned()).or_insert(vec![]);
                        names.push((perms, basename))

                    }
            }
        result
    }



    pub fn is_directory(&self, inode: &Inode) -> bool {
        let file_id = OwnedFileId {
            parent_inode: inode.clone(),
            basename: SmallString::from_str(""),
        };
        inode == &ROOT_INODE || self.get_tree(&file_id.as_file_id()).is_some()
    }

    /// Splits a path into (1) the first inode from the root that is
    /// an ancestor of the path and (2) the remainder of this path
    fn closest_in_repo_ancestor<'a>
        (&self,
         path: &'a std::path::Path)
         -> Result<(Inode, std::iter::Peekable<std::path::Components<'a>>), Error> {

        let mut components = path.components().peekable();
        let mut fileid = OwnedFileId {
            parent_inode: ROOT_INODE,
            basename: SmallString::from_str(""),
        };

        loop {
            if let Some(c) = components.peek() {

                fileid.basename = SmallString::from_str(c.as_os_str().to_str().unwrap());
                if let Some(v) = self.get_tree(&fileid.as_file_id()) {
                    fileid.parent_inode = v.clone()
                } else {
                    break;
                }
            } else {
                break;
            }
            components.next();
        }
        Ok((fileid.parent_inode.clone(), components))
    }

    /// Find the inode corresponding to that path, or return an error if there's no such inode.
    pub fn find_inode(&self, path: &std::path::Path) -> Result<Inode, Error> {

        let (inode, mut remaining_path_components) = try!(self.closest_in_repo_ancestor(path));
        if remaining_path_components.next().is_none() {
            Ok(inode)
        } else {
            Err(Error::FileNotInRepo(path.to_path_buf()))
        }
    }


    pub fn file_names(&self, branch: &Branch, key: &Key<PatchId>) -> Vec<(Key<PatchId>, FileMetadata, &str)> {

        let mut result = Vec::new();
        let e = Edge::zero(FOLDER_EDGE | PARENT_EDGE);

        for (_, parent) in self.iter_nodes(branch, Some((key, Some(&e))))
            .take_while(|&(ref k, ref v)| *k == key && v.flag <= FOLDER_EDGE | PARENT_EDGE | PSEUDO_EDGE) {

                match self.get_contents(&parent.dest) {
                    Some(ref name) if name.len() >= 2 => {
                        // This is supposed to be a small string anyway.
                        let (perms, basename) = name.as_slice().split_at(2);
                        let perms = FileMetadata::from_contents(perms);
                        let basename = std::str::from_utf8(basename).unwrap();

                        for (_, grandparent) in self.iter_nodes(branch, Some((&parent.dest, Some(&e))))
                            .take_while(|&(k, ref v)| *k == parent.dest && v.flag <= FOLDER_EDGE | PARENT_EDGE | PSEUDO_EDGE) {

                                result.push((grandparent.dest.to_owned(), perms, basename));
                                break

                            }
                    },
                    _ => error!("Key: {:?}, file {}, line {}",
                                key,
                                file!(), line!())
                }
            }
        result
    }


}
