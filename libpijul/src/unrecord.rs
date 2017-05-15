use rand;
use backend::*;
use patch::*;
use error::*;
use std::mem::swap;
use std::collections::HashSet;

impl<'env, T: rand::Rng> MutTxn<'env, T> {
    /// Unrecord the patch, returning true if and only if another
    /// branch still uses this patch.
    pub fn unapply(&mut self,
                   branch: &mut Branch,
                   patch_id: &PatchId,
                   patch: &Patch)
                   -> Result<(), Error> {

        debug!("revdep: {:?}", self.get_revdep(patch_id, None));

        // Check that the branch has no patch that depends on this one.
        assert!(self.iter_revdep(Some((patch_id, None)))
                .take_while(|&(p, _)| p == *patch_id)
                .all(|(_, p)| self.get_patch(&branch.patches, &p).is_none()));

        let mut moves_newnames: HashSet<Key<PatchId>> = HashSet::new();
        let mut moves = Vec::new();

        let mut context_edges = Vec::new();

        // Check applied, check dependencies.
        for change in patch.changes.iter() {
            info!("unapplying {:?}", change);
            match *change {

                Change::NewEdges { ref op, ref edges } => {

                    // Revert the edges, adding pseudo-edges if flag does not contain DELETED.
                    let mut del_edge = Edge::zero(EdgeFlags::empty());
                    del_edge.introduced_by = patch_id.clone();

                    let mut edge = Edge::zero(EdgeFlags::empty());
                    edge.introduced_by = patch_id.clone();

                    for e in edges {

                        let mut key = self.internal_key(&e.from, patch_id).to_owned();

                        // Delete the edge introduced by this patch,
                        // if this NewEdges is not forgetting its
                        // edges.
                        match *op {
                            EdgeOp::Map { flag, .. } |
                            EdgeOp::New { flag } => {
                                del_edge.flag = flag;
                                del_edge.dest = self.internal_key(&e.to, patch_id).to_owned();
                                self.del_nodes(branch, &key, Some(&del_edge))?;

                                del_edge.flag.toggle(PARENT_EDGE);
                                swap(&mut key, &mut del_edge.dest);
                                self.del_nodes(branch, &key, Some(&del_edge))?;

                                swap(&mut key, &mut del_edge.dest);
                            }
                            _ => {}
                        }
                        // Add its previous version, if this NewEdges
                        // is not introducing brand new edges.
                        match *op {
                            EdgeOp::Map { previous, .. } |
                            EdgeOp::Forget { previous } => {
                                edge.flag = previous;
                                edge.dest = del_edge.dest.clone();
                                edge.introduced_by = self.internal_hash(&e.introduced_by, patch_id);
                                self.put_nodes(branch, &key, &edge)?;

                                edge.flag.toggle(PARENT_EDGE);
                                swap(&mut key, &mut edge.dest);
                                self.put_nodes(branch, &key, &edge)?;
                            }
                            _ => {}
                        }
                    }


                    // If this NewEdges caused repair edges to be
                    // inserted, remove the repair edges.
                    match *op {
                        EdgeOp::New { flag } |
                        EdgeOp::Map { flag, .. } if !flag.contains(DELETED_EDGE) => {
                            for e in edges {

                                let key = if flag.contains(PARENT_EDGE) {
                                    self.internal_key(&e.from, patch_id).to_owned()
                                } else {
                                    self.internal_key(&e.to, patch_id).to_owned()
                                };

                                self.remove_up_context_repair(branch,
                                                              &key,
                                                              patch_id,
                                                              &mut context_edges)?;

                                self.remove_down_context_repair(branch,
                                                                &key,
                                                                patch_id,
                                                                &mut context_edges)?
                            }
                        }
                        _ => {}
                    }


                    // We now take care of the connectivity of the
                    // alive graph, which we must maintain.
                    //
                    // If the NewEdges we're unapplying introduced a
                    // new edge, or "undeleted" one (by turning a
                    // DELETED edge into another type of edge), the
                    // unapply might disconnect the alive connected
                    // component.


                    // First, if this NewEdges introduced a new edge,
                    // this might have changed the order relation,
                    // some pseudo-edges might be shortcutting that
                    // new edge. These pseudo-edges will no longer be
                    // correct after this unapply, so we need to
                    // delete them.  This can happen only if the
                    // source of an edge is not alive.

                    if let EdgeOp::New { flag } = *op {

                        let mut alive_ancestors = Vec::new();
                        let mut targets = Vec::new();

                        for e in edges.iter() {

                            let source = if flag.contains(PARENT_EDGE) {
                                &e.to
                            } else {
                                &e.from
                            };
                            let source = self.internal_key(source, patch_id);

                            // If the source is not alive.
                            if !self.is_alive(branch, &source) {

                                // Collect its closest alive ancestors.
                                self.collect_alive_ancestors(branch, &source, &mut alive_ancestors);

                                // Collecting all pseudo-edges from
                                // all alive ancestors.
                                for key in alive_ancestors.drain(..) {
                                    let edge = Edge::zero(PSEUDO_EDGE);
                                    for (k, v) in self.iter_nodes(branch, Some((&key, Some(&edge))))
                                        .take_while(|&(k, v)| {
                                            k == &key && v.flag <= PSEUDO_EDGE | FOLDER_EDGE
                                        }) {

                                        targets.push((k.to_owned(), v.to_owned()))
                                    }
                                }

                                // Destroy these pseudo-edges.
                                for &(ref k, ref v) in targets.iter() {
                                    let mut k = k.to_owned();
                                    let mut v = v.to_owned();
                                    self.del_nodes(branch, &k, Some(&v))?;
                                    swap(&mut k, &mut v.dest);
                                    v.flag ^= PARENT_EDGE;
                                    self.del_nodes(branch, &k, Some(&v))?;
                                }

                                // Collect the alive ancestors of the target's deleted parents.
                                self.reconnect_broken_down_context(branch,
                                                                   targets.drain(..)
                                                                       .map(|(_, v)| v.dest))?;
                            }
                        }
                    }


                    // If unapplying this NewEdges introduces DELETED
                    // edges to the graph, or causes edges to be
                    // forgotten, add pseudo edges where necessary to
                    // keep the alive component of the graph
                    // connected.
                    //
                    // This happens either if flag is EdgeOp::Map,
                    // with the previous field DELETED, or else if it
                    // is EdgeOp::New.

                    let (needs_reconnection, is_upwards) = match *op {
                        EdgeOp::Map { previous, flag } => {
                            (previous.contains(DELETED_EDGE), flag.contains(PARENT_EDGE))
                        }
                        EdgeOp::New { flag } => (true, flag.contains(PARENT_EDGE)),
                        _ => (false, false),
                    };

                    if needs_reconnection {

                        // For all targets of the edge, finds its
                        // alive ascendants, and add pseudo-edges.
                        let targets: Vec<_> = edges.iter()
                            .map(|e| if is_upwards { &e.from } else { &e.to })
                            .map(|c| self.internal_key(c, patch_id))
                            .collect();
                        self.reconnect_broken_down_context(branch, targets.into_iter())?

                    }

                    // If unapplying this NewEdges re-inserts a node
                    // whose descendants had been deleted in another
                    // patch, we need to find its closest alive
                    // descendants, and reconnect.
                    let (needs_reconnection, is_upwards) = match *op {
                        EdgeOp::Map { flag, .. } => {
                            (flag.contains(DELETED_EDGE), flag.contains(PARENT_EDGE))
                        }
                        EdgeOp::Forget { previous } => (true, previous.contains(PARENT_EDGE)),
                        _ => (false, false),
                    };

                    if needs_reconnection {

                        // For all targets of this edges, finds its
                        // alive ascendants, and add pseudo-edges.
                        let mut alive_descendants = Vec::new();
                        for e in edges.iter() {

                            let source = if is_upwards {
                                &e.from
                            } else {
                                &e.to
                            };
                            let source = self.internal_key(source, patch_id);

                            // Collect the source's closest alive descendants.
                            alive_descendants.clear();
                            self.collect_alive_descendants(branch, &source, &mut alive_descendants, true);
                            debug!("alive_descendants: {:?}", alive_descendants);
                            let mut edge = Edge::zero(EdgeFlags::empty());
                            for desc in alive_descendants.iter() {

                                let mut key = source.clone();
                                assert!(&source != desc);
                                edge.flag = PSEUDO_EDGE;
                                edge.dest = desc.clone();
                                edge.introduced_by = self.internal_hash(&e.introduced_by, patch_id);
                                self.put_nodes(branch, &key, &edge)?;

                                edge.flag.toggle(PARENT_EDGE);
                                swap(&mut key, &mut edge.dest);
                                self.put_nodes(branch, &key, &edge)?;

                            }
                        }

                    }


                    // If this NewEdges deleted a folder edge (because
                    // it moved or deleted a file), unapplying it
                    // needs to add it back to "marked for deletion"
                    // in the inodes database.
                    //
                    // Because we don't yet know whether this is a
                    // file move or a file deletion, we're just
                    // pushing the key to the "moves" vector, and
                    // we'll handle that after unapplying the whole
                    // patch (see below).
                    let (deletes_file, is_upwards) = match *op {
                        EdgeOp::New { flag } |
                        EdgeOp::Map { flag, .. } if flag.contains(DELETED_EDGE | FOLDER_EDGE) => {
                            (true, flag.contains(PARENT_EDGE))
                        }
                        EdgeOp::Forget { previous } if previous.contains(FOLDER_EDGE) &&
                                                        !previous.contains(DELETED_EDGE) => {
                            (true, previous.contains(PARENT_EDGE))
                        }
                        _ => (false, false),
                    };

                    if deletes_file {

                        for e in edges {
                            let dest = if is_upwards { &e.from } else { &e.to };
                            let internal = self.internal_key(dest, patch_id).to_owned();
                            let inode = self.get_revinodes(&internal).map(|x| x.to_owned());
                            if let Some(inode) = inode {
                                moves.push((internal, inode));
                            }
                        }
                    }

                    // Conversely, if this NewEdges added a folder
                    // edge, and we're unapplying it, remove the files
                    // from inodes.
                    match *op {
                        EdgeOp::New { flag } |
                        EdgeOp::Map { flag, .. } if flag.contains(FOLDER_EDGE) &&
                                                     !flag.contains(DELETED_EDGE) => {
                            for e in edges {
                                let dest = if flag.contains(PARENT_EDGE) {
                                    &e.from
                                } else {
                                    &e.to
                                };
                                let internal = self.internal_key(dest, patch_id).to_owned();
                                self.remove_file_from_inodes(&internal)?;
                                // This might be a file move, there's
                                // no way to tell until we've looked
                                // at the whole patch. Insert it to
                                // `moves_newnames`, just in case (the
                                // final algorithm for that is a set
                                // difference, so non-moves will be
                                // ignored).
                                moves_newnames.insert(internal);
                            }
                        }
                        _ => {}
                    }


                }
                Change::NewNodes { ref up_context,
                                   ref down_context,
                                   ref line_num,
                                   ref flag,
                                   ref nodes } => {

                    // Delete the new nodes.


                    // Start by deleting all the "missing context
                    // repair" we've added when applying this patch,
                    // i.e. all the extra pseudo-edges that were
                    // inserted to connect the alive set of vertices.

                    // We make the assumption that no pseudo-edge is a
                    // shortcut for this NewNodes. This is because
                    // `nodes` is nonempty: indeed, any such
                    // pseudo-edge would stop at one of the nodes
                    // introduced by this NewNodes.
                    assert!(nodes.len() != 0);

                    // Remove the zombie edges introduced to repair
                    // the context, if it was missing when we applied
                    // this NewNodes.
                    for c in up_context.iter() {
                        let c = self.internal_key(c, patch_id);
                        self.remove_up_context_repair(branch, &c, patch_id, &mut context_edges)?;
                    }
                    for c in down_context.iter() {
                        let c = self.internal_key(c, patch_id);
                        self.remove_down_context_repair(branch, &c, patch_id, &mut context_edges)?;
                    }

                    // Delete the nodes and all their adjacent edges.
                    let mut k = Key {
                        patch: patch_id.clone(),
                        line: line_num.clone(),
                    };
                    for i in 0..nodes.len() {

                        debug!("starting k: {:?}", k);
                        // Delete the contents of this node.
                        self.del_contents(&k, None)?;

                        // Delete all edges adjacent to this node,
                        // which will also delete the node (we're only
                        // storing edges).
                        loop {

                            // Find the next edge from this key, or break if we're done.
                            let mut edge = if let Some(edge) = self.get_nodes(branch, &k, None) {
                                edge.to_owned()
                            } else {
                                break;
                            };

                            debug!("{:?} {:?}", k, edge);
                            // Kill that edge in both directions.
                            self.del_nodes(branch, &k, Some(&edge))?;
                            edge.flag.toggle(PARENT_EDGE);
                            swap(&mut edge.dest, &mut k);
                            self.del_nodes(branch, &k, Some(&edge))?;
                            swap(&mut edge.dest, &mut k);

                        }

                        // If this is a file addition, delete it from inodes/revinodes.
                        if flag.contains(FOLDER_EDGE) {
                            self.remove_file_from_inodes(&k)?;
                            if i == nodes.len() - 1 {
                                // If this is a file move, record that information.
                                for d in down_context {
                                    let d = self.internal_key(&d, patch_id);
                                    moves_newnames.insert(d);
                                }
                            }
                        }

                        // Increment the line id (its type, LineId,
                        // implements little-endian additions with
                        // usize. See the `backend` module).
                        k.line += 1
                    }

                    // From all nodes in the down context, climb
                    // deleted paths up until finding alive ancestors,
                    // and add pseudo-edges from these ansestors to
                    // the down context.
                    let internal_down_context: Vec<_> =
                        down_context.iter().map(|c| self.internal_key(c, patch_id)).collect();
                    self.reconnect_broken_down_context(branch, internal_down_context.into_iter())?
                }
            }
        }

        let mut moved = HashSet::new();
        for &(ref key, ref inode) in moves.iter() {
            let mut header = self.get_inodes(&inode).unwrap().to_owned();
            header.status = if moves_newnames.contains(&key) {
                moved.insert(key);
                FileStatus::Moved
            } else {
                FileStatus::Deleted
            };
            self.replace_inodes(&inode, &header)?;
        }

        Ok(())
    }


    fn reconnect_broken_down_context<'a, I: Iterator<Item = Key<PatchId>>>(&mut self,
                                                                           branch: &mut Branch,
                                                                           down_context: I)
                                                                           -> Result<(), Error> {
        debug!("reconnect_broken_down_context");
        let mut alive_ancestors_of_down_context = Vec::new();
        for c in down_context {

            // For all parents of c, collect their alive ancestors.
            let edge = Edge::zero(PARENT_EDGE | PSEUDO_EDGE);
            for (_, v) in
                self.iter_nodes(branch, Some((&c, Some(&edge))))
                    .take_while(|&(k, v)| {
                        k == &c && v.flag <= PARENT_EDGE | FOLDER_EDGE | PSEUDO_EDGE
                    }) {

                if !self.is_alive(branch, &v.dest) {
                    self.collect_alive_ancestors(branch,
                                                 &v.dest,
                                                 &mut alive_ancestors_of_down_context)
                }
            }
            // Add all necessary pseudo-edges for this element of the down context.
            for dest in alive_ancestors_of_down_context.drain(..) {
                let mut edge = Edge::zero(PSEUDO_EDGE);
                edge.dest = dest.clone();
                self.put_nodes(branch, &c, &edge)?;
                edge.dest = c.clone();
                self.put_nodes(branch, &dest, &edge)?;
            }
        }
        Ok(())
    }

    fn remove_file_from_inodes(&mut self, k: &Key<PatchId>) -> Result<(), Error> {
        let inode = self.get_revinodes(&k).map(|x| x.to_owned());
        if let Some(inode) = inode {
            self.del_revinodes(&k, None)?;
            self.del_inodes(&inode, None)?;
        }
        Ok(())
    }

    fn collect_up_context_repair(&self,
                                 branch: &Branch,
                                 key: &Key<PatchId>,
                                 patch_id: &PatchId,
                                 edges: &mut Vec<(Key<PatchId>, Edge)>) {

        debug!("collect up {:?}", key);
        let edge = Edge::zero(PARENT_EDGE | PSEUDO_EDGE);
        for (k, v) in self.iter_nodes(branch, Some((key, Some(&edge))))
            .take_while(|&(k, v)| {
                k == key && v.flag <= PARENT_EDGE | PSEUDO_EDGE | FOLDER_EDGE &&
                v.introduced_by == *patch_id
            }) {

            edges.push((k.to_owned(), v.to_owned()));

            self.collect_up_context_repair(branch, &v.dest, patch_id, edges)
        }

    }

    fn collect_down_context_repair(&self,
                                   branch: &Branch,
                                   key: &Key<PatchId>,
                                   patch_id: &PatchId,
                                   edges: &mut Vec<(Key<PatchId>, Edge)>) {

        debug!("collect down {:?}", key);

        let edge = Edge::zero(PSEUDO_EDGE);
        for (k, v) in self.iter_nodes(branch, Some((key, Some(&edge))))
            .take_while(|&(k, v)| {
                k == key && v.flag <= PSEUDO_EDGE | FOLDER_EDGE && v.introduced_by == *patch_id
            }) {

            edges.push((k.to_owned(), v.to_owned()));

            self.collect_down_context_repair(branch, &v.dest, patch_id, edges)
        }

    }

    fn remove_up_context_repair(&mut self,
                                branch: &mut Branch,
                                key: &Key<PatchId>,
                                patch_id: &PatchId,
                                edges: &mut Vec<(Key<PatchId>, Edge)>)
                                -> Result<(), Error> {

        self.collect_up_context_repair(branch, key, patch_id, edges);
        for (k, v) in edges.drain(..) {

            debug!("remove {:?} {:?}", k, v);

            self.del_nodes(branch, &k, Some(&v))?;
        }

        Ok(())
    }

    fn remove_down_context_repair(&mut self,
                                  branch: &mut Branch,
                                  key: &Key<PatchId>,
                                  patch_id: &PatchId,
                                  edges: &mut Vec<(Key<PatchId>, Edge)>)
                                  -> Result<(), Error> {

        self.collect_down_context_repair(branch, key, patch_id, edges);
        for (k, v) in edges.drain(..) {
            self.del_nodes(branch, &k, Some(&v))?;
        }

        Ok(())
    }


    fn collect_alive_ancestors(&self,
                               branch: &Branch,
                               key: &Key<PatchId>,
                               edges: &mut Vec<Key<PatchId>>) {

        if self.is_alive(branch, key) {
            edges.push(key.clone())
        } else {
            debug!("collect alive ancestors, key = {:?}", key);
            let edge = Edge::zero(PARENT_EDGE | DELETED_EDGE);
            for (_, v) in
                self.iter_nodes(branch, Some((key, Some(&edge))))
                    .take_while(|&(k, v)| {
                        k == key && v.flag <= PARENT_EDGE | DELETED_EDGE | FOLDER_EDGE
                    }) {

                self.collect_alive_ancestors(branch, &v.dest, edges)
            }
        }
    }

    fn collect_alive_descendants(&self,
                                 branch: &Branch,
                                 key: &Key<PatchId>,
                                 edges: &mut Vec<Key<PatchId>>,
                                 is_first_key: bool) {

        if self.is_alive(branch, key) && !is_first_key {
            edges.push(key.clone())
        } else {
            debug!("collect alive descendants, key = {:?}", key);
            let edge = Edge::zero(DELETED_EDGE);
            for (_, v) in
                self.iter_nodes(branch, Some((key, Some(&edge))))
                    .take_while(|&(k, v)| {
                        k == key && v.flag <= DELETED_EDGE | FOLDER_EDGE
                    }) {

                self.collect_alive_descendants(branch, &v.dest, edges, false)
            }
        }
    }


    pub fn unrecord(&mut self,
                    branch: &mut Branch,
                    patch_id: &PatchId,
                    patch: &Patch)
                    -> Result<bool, Error> {

        if self.get_patch(&branch.patches, patch_id).is_some() {

            debug!("unrecord: {:?} {:?}", patch_id, patch);
            self.unapply(branch, patch_id, patch)?;

            let timestamp = self.get_patch(&branch.patches, patch_id).unwrap();

            self.del_patches(&mut branch.patches, patch_id)?;
            self.del_revpatches(&mut branch.revpatches, timestamp, patch_id)?;

            for dep in patch.dependencies.iter() {
                let internal_dep = self.get_internal(dep.as_ref()).unwrap().to_owned();
                // Test whether other branches have this patch.
                let other_branches_have_dep = self.iter_branches(None)
                    .any(|branch| self.get_patch(&branch.patches, &internal_dep).is_some());

                if !other_branches_have_dep {
                    self.del_revdep(&internal_dep, Some(&patch_id))?;
                }
            }
        }


        // If no other branch uses this patch, delete from revdeps.
        if !self.iter_branches(None)
            .any(|branch| self.get_patch(&branch.patches, patch_id).is_some()) {

                info!("deleting patch");
            // Delete all references to patch_id in revdep.
            while self.del_revdep(patch_id, None)? {}
            let ext = self.get_external(patch_id).unwrap().to_owned();
            self.del_external(patch_id)?;
            self.del_internal(ext.as_ref())?;
            Ok(false)
        } else {
            Ok(true)
        }
    }
}
