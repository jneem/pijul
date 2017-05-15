use backend::*;
use record::InodeUpdate;
use error::Error;
use patch::*;
use fs_representation::{patches_dir};
use std::collections::{HashSet, HashMap};
use std::path::Path;
use rand;
use std::mem::swap;

impl<U: Transaction, R> T<U, R> {

    /// Return the patch id corresponding to `e`, or `internal` if `e==None`.
    pub fn internal_hash(&self, e: &Option<Hash>, internal: &PatchId) -> PatchId {
        match *e {
            Some(Hash::None) => ROOT_PATCH_ID.clone(),
            Some(ref h) => self.get_internal(h.as_ref()).unwrap().to_owned(),
            None => internal.clone(),
        }
    }

    /// Fetch the internal key for this external key (or `internal` if
    /// `key.patch` is `None`).
    pub fn internal_key(&self, key: &Key<Option<Hash>>, internal: &PatchId) -> Key<PatchId> {
        Key {
            patch: self.internal_hash(&key.patch, internal),
            line: key.line.clone(),
        }
    }

    pub fn internal_key_unwrap(&self, key: &Key<Option<Hash>>) -> Key<PatchId> {
        Key {
            patch: self.get_internal(key.patch.as_ref().unwrap().as_ref()).unwrap().to_owned(),
            line: key.line.clone(),
        }
    }
}


impl<'env, T: rand::Rng> MutTxn<'env, T> {

    /// Applies a patch to a repository. "new_patches" are patches that
    /// just this repository has, and the remote repository doesn't have.
    fn apply(&mut self,
             branch: &mut Branch,
             patch: &Patch,
             patch_id: &PatchId,
             timestamp: ApplyTimestamp)
             -> Result<(), Error> {

        assert!(self.put_patches(&mut branch.patches, patch_id, timestamp)?);
        assert!(self.put_revpatches(&mut branch.revpatches, timestamp, patch_id)?);

        debug!("apply_raw");
        let mut parents: Vec<Key<PatchId>> = Vec::new();
        let mut children: Vec<Edge> = Vec::new();
        for ch in patch.changes.iter() {
            match *ch {
                Change::NewEdges { ref op, ref edges } => {

                    debug!("apply: edges");

                    // Delete the old version of the edge. If the new
                    // flag is a deletion, this can break the
                    // invariant that alive nodes are reachable by
                    // alive paths.
                    self.delete_old_edges(branch, patch_id, op, edges)?;

                    // At this point, alive nodes might be
                    // disconnected (and even the graph itself might
                    // be disconnected).

                    match *op {
                        EdgeOp::Map { flag, .. } |
                        EdgeOp::New { flag } => {
                            // Add the new version of the edges (if the
                            // operation is Map or New).
                            parents.clear();
                            children.clear();
                            // Add the new edges, reconnecting the
                            // graph, adding new pseudo-edges from all
                            // ancestors/pseudo-ancestors to all
                            // descendants/pseudo-descendants of the
                            // deleted block.
                            self.add_new_edges(branch, patch_id, flag, edges,
                                               &mut parents, &mut children)?;

                            // Now, pseudo-descendants and
                            // pseudo-ancestors can be deleted, to
                            // save space and reduce the degree of
                            // deleted keys.
                            self.delete_old_pseudo_edges(
                                branch, patch_id, *op, edges
                            )?;
                        }
                        EdgeOp::Forget { previous } => {

                            // Forget these edges, and remove
                            // pseudo-edges that shortcut these edges.
                            parents.clear();
                            self.forget_edges(branch, patch_id, previous, edges,
                                              &mut parents, &mut children)?;
                        }
                    }
                    debug!("apply_raw:edges.done");
                }
                Change::NewNodes { ref up_context,
                                   ref down_context,
                                   ref line_num,
                                   flag,
                                   ref nodes } => {

                    assert!(!nodes.is_empty());
                    debug!("apply: newnodes");
                    self.add_new_nodes(branch, patch_id, up_context, down_context, line_num, flag, nodes)?;
                }
            }
        }

        // If there is a missing context, add pseudo-edges along the
        // edges that deleted the conflict, until finding (in both
        // directions) an alive context.
        self.repair_deleted_contexts(branch, patch, patch_id)?;

        Ok(())
    }


    /// Delete old versions of `edges`.
    fn delete_old_edges(&mut self, branch: &mut Branch, patch_id: &PatchId, flag: &EdgeOp, edges: &[NewEdge]) -> Result<(), Error> {
        match *flag {
            EdgeOp::Forget { previous } | EdgeOp::Map { previous, .. } => {
                for e in edges {
                    let (from, to) =
                        (self.internal_key(&e.from, patch_id),
                         self.internal_key(&e.to, patch_id));

                    let mut deleted_v = from.clone();
                    let mut deleted_e = Edge {
                        flag: previous,
                        dest: to.clone(),
                        introduced_by: self.internal_hash(&e.introduced_by, patch_id),
                    };
                    self.del_nodes(branch, &deleted_v, Some(&deleted_e))?;
                    swap(&mut deleted_v, &mut deleted_e.dest);
                    deleted_e.flag.toggle(PARENT_EDGE);
                    self.del_nodes(branch, &deleted_v, Some(&deleted_e))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn delete_old_pseudo_edges(&mut self, branch: &mut Branch, patch_id: &PatchId, op: EdgeOp, edges: &[NewEdge]) -> Result<(), Error> {
        for e in edges {

            match op {
                EdgeOp::Map { previous, .. } => {

                    let to = if previous.contains(PARENT_EDGE) {
                        self.internal_key(&e.from, patch_id)
                    } else {
                        self.internal_key(&e.to, patch_id)
                    };

                    // Maybe what we just deleted the last
                    // alive edge to the destination of e.
                    //
                    // If this is the case, we can remove pseudo-edges to the
                    // destination of e.

                    if !self.is_alive(&branch, &to) {
                        self.kill_pseudo_edges_to(branch, &to)?;
                        self.kill_pseudo_edges_from(branch, &to)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn add_new_edges(&mut self, branch: &mut Branch, patch_id: &PatchId,
                     flag: EdgeFlags,
                     edges: &[NewEdge],
                     parents: &mut Vec<Key<PatchId>>,
                     children: &mut Vec<Edge>) -> Result<(), Error> {

        for e in edges {

            // If the edge has not been forgotten about,
            // insert the new version.
            let (to, from) = if flag.contains(PARENT_EDGE) {
                (&e.from, &e.to)
            } else {
                (&e.to, &e.from)
            };
            let mut v = self.internal_key(to, patch_id);
            let mut e = Edge {
                flag: flag | PARENT_EDGE,
                dest: self.internal_key(from, patch_id),
                introduced_by: patch_id.clone()
            };
            self.put_nodes(branch, &v, &e)?;
            swap(&mut v, &mut e.dest);
            e.flag.toggle(PARENT_EDGE);
            self.put_nodes(branch, &v, &e)?;

            // Here, v contains the origin of the edge, and e the
            // destination. Beware, though: the edge might be
            // PARENT_EDGE, and then these are backwards.

            if flag.contains(DELETED_EDGE) && !flag.contains(FOLDER_EDGE) {

                // collect alive parents/children of hunk

                // Collect all the alive parents of this edge.
                let edge = Edge::zero(PARENT_EDGE);
                if self.is_alive(branch, &v) {
                    parents.push(v.clone())
                }
                parents.extend(
                    self.iter_nodes(&branch, Some((&e.dest, Some(&edge))))
                        .take_while(|&(k, v)| *k == e.dest &&
                                    v.flag <= PARENT_EDGE | FOLDER_EDGE | PSEUDO_EDGE)
                        .filter(|&(_, e)| self.is_alive(branch, &e.dest))
                        .map(|(_, e)| e.dest.clone())
                );

                // Now collect all the alive children of this edge.
                let edge = Edge::zero(EdgeFlags::empty());
                children.extend(
                    self.iter_nodes(&branch, Some((&e.dest, Some(&edge))))
                        .take_while(|&(k, v)| *k == e.dest && v.flag <= PSEUDO_EDGE | FOLDER_EDGE)
                        .map(|(_, e)| e.clone())
                )
            }
        }
        // If these edges are being deleted, add pseudo-edges between
        // parents and children of this hunk, to keep the alive
        // component of the graph connected.
        //
        // If these extra edges are redundant, they will be collected in
        // module `graph` and deleted in module `output`.
        if flag.contains(DELETED_EDGE) {
            self.reconnect_parents_children(branch, patch_id, parents, children)?;
        }
        Ok(())
    }

    /// Add pseudo edges from all keys of `parents` to all `dest` of
    /// the edges in `children`, with the same edge flags as in
    /// `children`, plus `PSEUDO_EDGE`.
    pub fn reconnect_parents_children(&mut self, branch: &mut Branch, patch_id: &PatchId, parents: &mut Vec<Key<PatchId>>, children: &mut Vec<Edge>) -> Result<(), Error> {

        debug!("reconnecting {:?} {:?}", parents, children);
        for mut parent in parents.drain(..) {

            for child in children.drain(..) {

                // If these are not already connected
                // or pseudo-connected, add a
                // pseudo-edge.
                if !self.is_connected(branch, &parent, &child.dest) {

                    let mut pseudo_edge = Edge {
                        flag: child.flag | PSEUDO_EDGE,
                        dest: child.dest,
                        introduced_by: patch_id.clone(),
                    };
                    debug!("reconnect_parents_children: {:?} {:?}", parent, pseudo_edge);
                    self.put_nodes(branch, &parent, &pseudo_edge)?;
                    swap(&mut parent, &mut pseudo_edge.dest);
                    pseudo_edge.flag.toggle(PARENT_EDGE);
                    self.put_nodes(branch, &parent, &pseudo_edge)?;
                    // Revert the parent to what it was.
                    swap(&mut parent, &mut pseudo_edge.dest);
                }
            }
        }
        Ok(())
    }

    /// Forget edges, delete all pseudo-edges to the target of the
    /// edges, and then rebuild the pseudo-edges by collecting the
    /// alive ancestors. This ensures that any transitive relation
    /// introduced by these edges is forgotten, and the alive
    /// component is still connected.
    fn forget_edges(&mut self, branch: &mut Branch, patch_id: &PatchId, previous: EdgeFlags,
                    edges: &[NewEdge], ancestors: &mut Vec<Key<PatchId>>,
                    children: &mut Vec<Edge>) -> Result<(), Error> {

        if previous.contains(DELETED_EDGE) {
            self.forget_dead_edges(branch, patch_id, previous.contains(PARENT_EDGE), edges,
                                   ancestors, children)
        } else {
            self.forget_alive_edges(branch, patch_id, previous.contains(PARENT_EDGE), edges, ancestors)
        }
    }

    fn forget_alive_edges(&mut self, branch: &mut Branch, patch_id: &PatchId, is_upwards: bool,
                          edges: &[NewEdge], ancestors: &mut Vec<Key<PatchId>>) -> Result<(), Error> {

        let mut cache = HashSet::new();
        for e in edges {

            let to =
                if is_upwards {
                    self.internal_key(&e.from, patch_id)
                } else {
                    self.internal_key(&e.to, patch_id)
                };


            // First, delete pseudo-edges pointing to `to`.
            // edges. Since the edge is assumed to be alive, and there
            // are no forward edges, this destroys all pseudo-edges
            // shortcutting `e`.
            self.kill_pseudo_edges_to(branch, &to)?;


            // At the time of calling this function, we have deleted
            // the forgotten edge `e`. The problem we have is that
            // `to` might not be reachable anymore, hence we need to
            // collect its alive ancestors (by following edges
            // pointing to it, then deleted paths, until finding alive
            // keys).


            // First, Collect all alive ancestors of deleted parents of `to`.
            let mut deleted_file = None;
            ancestors.clear();
            cache.clear();
            let e = Edge::zero(PARENT_EDGE);
            for (_, e) in self.iter_nodes(&branch, Some((&to, Some(&e))))
                .take_while(|&(k,e)| k == &to && e.flag == PARENT_EDGE) {

                    if !self.is_alive(&branch, &e.dest) {
                        self.find_alive_ancestors(&branch, &mut cache, ancestors,
                                                  &mut deleted_file, &e.dest)
                    }

                }

            // Then add pseudo-edges from all alive ancestors (if any)
            // of `to`, to `to` itself. If they are useless
            // (i.e. forward), they will be deleted by output
            // functions.
            for alive_parent in ancestors.iter() {

                if !self.is_connected(branch, alive_parent, &to) {

                    let mut alive_parent = alive_parent.clone();
                    let mut e = Edge {
                        flag: PSEUDO_EDGE,
                        dest: to.clone(),
                        introduced_by: patch_id.clone()
                    };
                    debug!("forget_edges: {:?} {:?}", alive_parent, e);
                    self.put_nodes(branch, &alive_parent, &e)?;
                    swap(&mut alive_parent, &mut e.dest);
                    e.flag.toggle(PARENT_EDGE);
                    self.put_nodes(branch, &alive_parent, &e)?;
                    // no need to swap back (neither alive_parents nor
                    // e are alive after this iteration).
                }
            }
        }
        Ok(())
    }



    fn forget_dead_edges(&mut self, branch: &mut Branch, patch_id: &PatchId, is_upwards: bool,
                         edges: &[NewEdge], ancestors: &mut Vec<Key<PatchId>>,
                         children: &mut Vec<Edge>) -> Result<(), Error> {

        let mut cache = HashSet::new();
        for e in edges {


            let to =
                if is_upwards {
                    self.internal_key(&e.from, patch_id)
                } else {
                    self.internal_key(&e.to, patch_id)
                };

            // Collect all alive children of `to`.
            let e = Edge::zero(EdgeFlags::empty());
            for (_, e) in self.iter_nodes(&branch, Some((&to, Some(&e))))
                .take_while(|&(k,e)| k == &to && e.flag <= PSEUDO_EDGE | FOLDER_EDGE) {

                    children.push(e.clone())
                }

            // And, if `to` is dead, all the alive ancestors of `to`.
            let mut deleted_file = None;
            if !self.is_alive(&branch, &to) {
                self.find_alive_ancestors(&branch, &mut cache, ancestors,
                                          &mut deleted_file, &to)
            }

            // Then add pseudo-edges from all alive ancestors (if any)
            // of `to`, to `to` itself. If they are useless
            // (i.e. forward), they will be deleted by output
            // functions.
            for alive_parent in ancestors.iter() {

                for alive_child in children.iter() {

                    if !self.is_connected(branch, alive_parent, &alive_child.dest) {

                        let mut alive_parent = alive_parent.clone();
                        let mut e = Edge {
                            flag: PSEUDO_EDGE,
                            dest: alive_child.dest.clone(),
                            introduced_by: patch_id.clone()
                        };
                        debug!("forget_edges: {:?} {:?}", alive_parent, e);
                        self.put_nodes(branch, &alive_parent, &e)?;
                        swap(&mut alive_parent, &mut e.dest);
                        e.flag.toggle(PARENT_EDGE);
                        self.put_nodes(branch, &alive_parent, &e)?;
                        // no need to swap back (neither alive_parents nor
                        // e are alive after this iteration).
                    }
                }
            }
        }
        Ok(())
    }







    /// Find the alive ancestors of `current`. `cache` is here to
    /// avoid cycles, and `alive` is an accumulator of the
    /// result. Since this search stops at files, if the file
    /// containing these lines is ever hit, it will be put in
    /// `file`.
    fn find_alive_ancestors(&self, branch: &Branch, cache: &mut HashSet<Key<PatchId>>, alive: &mut Vec<Key<PatchId>>, file: &mut Option<Key<PatchId>>, current: &Key<PatchId>) {
        if !cache.contains(current) {
            cache.insert(current.clone());
            if self.is_alive(branch, current) {
                alive.push(current.clone())
            } else {
                let e = Edge::zero(PARENT_EDGE|DELETED_EDGE);
                for (_, e) in self.iter_nodes(branch, Some((current, Some(&e))))
                    .take_while(|&(k, v)| k == current && v.flag.contains(DELETED_EDGE|PARENT_EDGE)) {

                        // e might be FOLDER_EDGE here.
                        if e.flag.contains(FOLDER_EDGE) {
                            *file = Some(current.clone())
                        } else {
                            self.find_alive_ancestors(branch, cache, alive, file, &e.dest)
                        }
                    }
            }
        }
    }

    /// Test whether `key` has a neighbor with flag `flag0`. If
    /// `include_pseudo`, this includes pseudo-neighbors.
    pub fn has_edge(&self,
                    branch: &Branch,
                    key: &Key<PatchId>,
                    flag: EdgeFlags,
                    include_pseudo: bool)
                    -> bool {

        let e = Edge::zero(flag);
        if let Some((k, v)) = self.iter_nodes(&branch, Some((key, Some(&e)))).next() {
            if include_pseudo {
                k == key && (v.flag <= flag | PSEUDO_EDGE)
            } else {
                k == key && v.flag == flag
            }
        } else {
            false
        }
    }

    /// Is there an alive/pseudo edge from `a` to `b`.
    fn is_connected(&self, branch: &Branch, a: &Key<PatchId>, b: &Key<PatchId>) -> bool {

        if a == b {
            return true
        }
        let mut edge = Edge::zero(EdgeFlags::empty());
        edge.dest = b.clone();

        if let Some((k, v)) = self.iter_nodes(&branch, Some((a, Some(&edge)))).next() {

            k == a && v.dest == *b && v.flag | FOLDER_EDGE == edge.flag | FOLDER_EDGE

        } else {
            false
        }
    }


    /// Remove all pseudo edges from branch `branch`, that end at node
    /// `pv`.
    fn kill_pseudo_edges_to(&mut self,
                            branch: &mut Branch,
                            target: &Key<PatchId>)
                            -> Result<(), Error> {

        self.kill_pseudo_edges_direction(
            branch, target,
            &[PSEUDO_EDGE | PARENT_EDGE, FOLDER_EDGE | PSEUDO_EDGE | PARENT_EDGE]
        )
    }

    /// Remove all pseudo edges from branch `branch`, that start at
    /// node `pv`.
    fn kill_pseudo_edges_from(&mut self,
                              branch: &mut Branch,
                              target: &Key<PatchId>)
                              -> Result<(), Error> {

        self.kill_pseudo_edges_direction(
            branch, target,
            &[PSEUDO_EDGE, FOLDER_EDGE|PSEUDO_EDGE]
        )
    }

    fn kill_pseudo_edges_direction(&mut self,
                                   branch: &mut Branch,
                                   target: &Key<PatchId>,
                                   direction: &[EdgeFlags])
                                   -> Result<(), Error> {
        // Kill all pseudo-edges of `target`.
        for flag in direction.iter() {
            let e = Edge::zero(*flag);
            loop {
                let mut deleted = None;
                if let Some((k, v)) = self.iter_nodes(&branch, Some((target, Some(&e)))).next() {

                    if k == target && v.flag == *flag {
                        deleted = Some((k.to_owned(), v.to_owned()))
                    }
                }
                if let Some((mut k, mut v)) = deleted {
                    try!(self.del_nodes(branch, &k, Some(&v)));
                    v.flag.toggle(PARENT_EDGE);
                    swap(&mut k, &mut v.dest);
                    try!(self.del_nodes(branch, &k, Some(&v)));
                    // no need to swap back: k and v are not alive
                    // after this (they were created just above in the
                    // `deleted = Some(..)` line.
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Add the new nodes (not repairing missing contexts).
    fn add_new_nodes(&mut self, branch: &mut Branch, patch_id: &PatchId,
                     up_context:&[Key<Option<Hash>>], down_context:&[Key<Option<Hash>>],
                     line_num:&LineId, flag: EdgeFlags, nodes: &[Vec<u8>])
                     -> Result<(), Error> {
        let mut v = Key {
            patch: patch_id.clone(),
            line: line_num.clone(),
        };
        let mut e = Edge {
            flag: EdgeFlags::empty(),
            dest: ROOT_KEY.clone(),
            introduced_by: patch_id.clone(),
        };

        // Connect the first line to the up context.
        for c in up_context {
            v.patch = patch_id.clone();
            v.line = line_num.clone();
            e.flag = flag ^ PARENT_EDGE;
            e.dest = self.internal_key(c, patch_id);
            debug!("put_nodes {:?} {:?}", v, e);
            self.put_nodes(branch, &v, &e)?;

            swap(&mut v, &mut e.dest);
            e.flag.toggle(PARENT_EDGE);

            debug!("put_nodes {:?} {:?}", v, e);
            try!(self.put_nodes(branch, &v, &e));
            // no need to swap back: v and e were just written at the
            // beginning of the iteration.
        }
        debug!("up context done");

        // Insert the contents and new nodes.
        v.patch = patch_id.clone();
        v.line = line_num.clone();
        e.flag = flag;
        e.dest.patch = patch_id.clone();

        let mut nodes = nodes.iter();
        if let Some(first_line) = nodes.next() {
            let value = try!(self.alloc_value(&first_line));
            debug!("put_contents {:?} {:?}", v, value);
            try!(self.put_contents(&v, value));
        }
        for content in nodes {

            e.flag = flag;
            e.dest.line = v.line + 1;
            debug!("put_nodes {:?} {:?}", v, e);
            try!(self.put_nodes(branch, &v, &e));
            swap(&mut v.line, &mut e.dest.line);
            e.flag.toggle(PARENT_EDGE);
            debug!("put_nodes {:?} {:?}", v, e);
            try!(self.put_nodes(branch, &v, &e));

            // v.line has just been incremented.

            let value = try!(self.alloc_value(&content));
            debug!("put_contents {:?} {:?}", v, value);
            try!(self.put_contents(&v, value));

            // no need to swap back: this is intended to "shift" the
            // counter to the next iteration.
        }
        debug!("newnodes core done");

        // Connect the last new line to the down context.
        e.flag = flag;

        for c in down_context {
            debug!("internal key of {:?}", c);
            e.dest = self.internal_key(c, patch_id);

            debug!("put_nodes {:?} {:?}", v, e);
            try!(self.put_nodes(branch, &v, &e));

            swap(&mut v, &mut e.dest);
            e.flag.toggle(PARENT_EDGE);

            debug!("put_nodes {:?} {:?}", v, e);
            try!(self.put_nodes(branch, &v, &e));

            swap(&mut v, &mut e.dest);
            e.flag.toggle(PARENT_EDGE);
        }
        debug!("down context done");
        Ok(())
    }

    /// Deleted contexts are conflicts. Reconnect the graph by
    /// inserting pseudo-edges alongside deleted edges.
    fn repair_deleted_contexts(&mut self,
                               branch: &mut Branch,
                               patch: &Patch,
                               patch_id: &PatchId)
                               -> Result<(), Error> {

        let mut relatives = Vec::new();
        let mut unknown_children = Vec::new();

        // repair_missing_context adds all zombie edges needed.
        for ch in patch.changes.iter() {
            match *ch {
                Change::NewEdges { ref op, ref edges, .. } => {

                    match *op {

                        EdgeOp::Map { flag, .. } |
                        EdgeOp::New { flag }
                        if !flag.contains(DELETED_EDGE) => {
                            debug!("repairing missing contexts for non-deleted edges");
                            // If we're adding an alive edge, and its
                            // origin and/or destination is deleted.
                            for e in edges {
                                let (up_context, down_context) = if flag.contains(PARENT_EDGE) {
                                    (self.internal_key(&e.to, patch_id),
                                     self.internal_key(&e.from, patch_id))
                                } else {
                                    (self.internal_key(&e.from, patch_id),
                                     self.internal_key(&e.to, patch_id))
                                };
                                self.repair_missing_up_context(branch,
                                                               &up_context,
                                                               patch_id,
                                                               &mut relatives)?;

                                self.repair_missing_down_context(branch,
                                                                 &down_context,
                                                                 patch_id,
                                                                 &mut relatives)?;
                            }
                        }
                        EdgeOp::Map { flag, .. } |
                        EdgeOp::New { flag } => {
                            debug!("repairing missing contexts for deleted edges");
                            debug_assert!(flag.contains(DELETED_EDGE));
                            // Here, flag contains DELETED_EDGE and
                            // not FOLDER_EDGE.
                            //
                            // If we have deleted edges without
                            // knowning about some of their alive
                            // children, this is a conflict, repair.
                            for e in edges {

                                let dest = if flag.contains(PARENT_EDGE) {
                                    self.internal_key(&e.from, patch_id)
                                } else {
                                    self.internal_key(&e.to, patch_id)
                                };


                                // If there is at least one unknown
                                // child, repair the context.
                                unknown_children.clear();
                                unknown_children.extend(
                                    self.iter_nodes(branch, Some((&dest, None)))
                                        .take_while(|&(k, v)| *k == dest && v.flag <= PSEUDO_EDGE)
                                        .filter(|&(_, v)| {
                                            let ext = self.external_hash(&v.introduced_by).to_owned();
                                            v.introduced_by != *patch_id && !patch.dependencies.contains(&ext)
                                        })
                                        .map(|(k, _)| k.to_owned())
                                );

                                for ch in unknown_children.drain(..) {
                                    self.repair_missing_up_context(
                                        branch,
                                        &ch,
                                        &ch.patch,
                                        &mut relatives
                                    )?
                                }

                                // If there is at least one alive
                                // parent we don't know about, repair.
                                let e = Edge::zero(PARENT_EDGE);
                                unknown_children.extend(
                                    self.iter_nodes(branch, Some((&dest, Some(&e))))
                                        .take_while(|&(k, v)| *k == dest && (
                                            v.flag == PARENT_EDGE || v.flag == PARENT_EDGE | FOLDER_EDGE
                                        ))
                                        .filter(|&(_, v)| {
                                            let ext = self.external_hash(&v.introduced_by).to_owned();
                                            v.introduced_by != *patch_id && !patch.dependencies.contains(&ext)
                                        })
                                        .map(|(k, _)| k.to_owned())
                                );

                                for ch in unknown_children.drain(..) {
                                    self.repair_missing_down_context(
                                        branch,
                                        &ch,
                                        &ch.patch,
                                        &mut relatives
                                    )?
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Change::NewNodes { ref up_context, ref down_context, .. } => {

                    debug!("repairing missing contexts for newnodes");
                    // If not all lines in `up_context` are alive, this
                    // is a conflict, repair.
                    for c in up_context {
                        let c = self.internal_key(c, patch_id);
                        try!(self.repair_missing_up_context(branch,
                                                            &c,
                                                            patch_id,
                                                            &mut relatives))
                    }
                    // If not all lines in `down_context` are alive,
                    // this is a conflict, repair.
                    for c in down_context {
                        let c = self.internal_key(c, patch_id);
                        try!(self.repair_missing_down_context(branch,
                                                              &c,
                                                              patch_id,
                                                              &mut relatives))
                    }
                    debug!("apply: newnodes, done");
                }
            }
        }
        Ok(())
    }

    /// Checks whether a line in the up context of a hunk is marked
    /// deleted, and if so, reconnect the alive parts of the graph,
    /// marking this situation as a conflict.
    fn repair_missing_up_context(&mut self,
                                 branch: &mut Branch,
                                 context: &Key<PatchId>,
                                 patch_id: &PatchId,
                                 relatives: &mut Vec<(Key<PatchId>, Edge)>)
                                 -> Result<(), Error> {


        // The up context needs a repair iff it's deleted.

        // Is the up context deleted?
        let up_context_deleted = self.has_edge(&branch, &context, PARENT_EDGE | DELETED_EDGE, false);

        // If so, collect edges to add.
        if up_context_deleted {

            // Alright, the up context was deleted, so the alive
            // component of the graph might be disconnected, and needs
            // a repair.

            // Follow all paths upwards (in the direction of
            // DELETED_EDGE|PARENT_EDGE) until finding an alive
            // ancestor, and turn them all into zombie edges.
            let mut visited = HashSet::new();
            let mut alive = Vec::new();
            self.find_alive_ancestors_with_edges(
                &branch,
                &context,
                &mut visited,
                &mut alive,
                relatives
            );

            for (mut key, mut edge) in relatives.drain(..) {
                if !self.is_connected(branch, &key, &edge.dest) {
                    edge.introduced_by = patch_id.clone();
                    edge.flag = PSEUDO_EDGE | PARENT_EDGE | (edge.flag&FOLDER_EDGE);
                    debug!("repairing up context: {:?} {:?}", key, edge);
                    self.put_nodes(branch, &key, &edge)?;

                    swap(&mut key, &mut edge.dest);
                    edge.flag.toggle(PARENT_EDGE);

                    self.put_nodes(branch, &key, &edge)?;
                    // no need to swap back: `key` and `edge` won't be
                    // alive after this iteration.
                }
            }
        }
        Ok(())
    }

    /// Checks whether a line in the down context of a hunk is marked
    /// deleted, and if so, reconnect the alive parts of the graph,
    /// marking this situation as a conflict.
    fn repair_missing_down_context(&mut self,
                                   branch: &mut Branch,
                                   context: &Key<PatchId>,
                                   patch_id: &PatchId,
                                   relatives: &mut Vec<(Key<PatchId>, Edge)>)
                                   -> Result<(), Error> {

        let down_context_deleted = self.has_edge(&branch, &context, PARENT_EDGE | DELETED_EDGE, false);

        if down_context_deleted {

            // Find all alive descendants, as well as the paths
            // leading to them, and double these edges with
            // pseudo-edges everywhere.
            let mut visited = HashSet::new();
            let mut alive = Vec::new();
            self.find_alive_descendants_with_edges(
                &branch,
                &context,
                &mut visited,
                &mut alive,
                relatives
            );
            debug!("down context relatives: {:?}", relatives);
            for (mut key, mut edge) in relatives.drain(..) {

                if !self.is_connected(branch, &key, &edge.dest) {
                    edge.introduced_by = patch_id.clone();
                    edge.flag = PSEUDO_EDGE | (edge.flag&FOLDER_EDGE);
                    debug!("repairing down context: {:?} {:?}", key, edge);
                    try!(self.put_nodes(branch, &key, &edge));

                    swap(&mut key, &mut edge.dest);
                    edge.flag.toggle(PARENT_EDGE);

                    try!(self.put_nodes(branch, &key, &edge));

                    // no need to swap back: `key` and `edge` are not
                    // alive after this iteration.
                }
            }
        }
        Ok(())
    }


    /// Recursively find all ancestors by doing a DFS, and collect all
    /// edges until finding an alive ancestor.
    fn find_alive_ancestors_with_edges(&self,
                                       branch: &Branch,
                                       a: &Key<PatchId>,
                                       visited: &mut HashSet<Key<PatchId>>,
                                       alive: &mut Vec<Key<PatchId>>,
                                       ancestors: &mut Vec<(Key<PatchId>, Edge)>) {

        if !visited.contains(a) {

            visited.insert(a.to_owned());

            let i = ancestors.len();

            let e = Edge::zero(PARENT_EDGE);
            if let Some((k,v)) = self.iter_nodes(&branch, Some((a, Some(&e)))).next() {

                if k == a && (v.flag == PARENT_EDGE|FOLDER_EDGE || v.flag == PARENT_EDGE) {

                    // This node is alive.
                    alive.push(k.to_owned());
                    return
                }
            }

            let e = Edge::zero(PARENT_EDGE|DELETED_EDGE);
            for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                .take_while(|&(k, v)| k == a && v.flag <= e.flag | FOLDER_EDGE)
                .filter(|&(_, v)| !v.flag.contains(PSEUDO_EDGE)) {

                    debug!("candidate relative {:?}", v);
                    ancestors.push((a.clone(), v.clone()))
                }
            let j = ancestors.len();
            for k in i..j {
                let dest = ancestors[k].1.dest.clone();
                self.find_alive_ancestors_with_edges(branch,
                                                     &dest,
                                                     visited,
                                                     alive,
                                                     ancestors);
            }
        }
    }



    /// Recursively find all descendants by doing a DFS, and collect
    /// all edges until finding an alive descendant.
    fn find_alive_descendants_with_edges(&self,
                                         branch: &Branch,
                                         a: &Key<PatchId>,
                                         visited: &mut HashSet<Key<PatchId>>,
                                         alive: &mut Vec<Key<PatchId>>,
                                         descendants: &mut Vec<(Key<PatchId>, Edge)>) {
        debug!("find_alive_descendants_with_edges: {:?}", a);
        if !visited.contains(a) {

            visited.insert(a.to_owned());
            let i = descendants.len();

            let e = Edge::zero(PARENT_EDGE);

            // First, test whether `a` is fully alive.
            let mut is_alive = false;
            let mut is_dead = false;
            for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                .take_while(|&(k, v)| k == a && v.flag <= PARENT_EDGE | DELETED_EDGE) {

                    is_dead |= v.flag.contains(DELETED_EDGE);
                    is_alive |= !v.flag.contains(DELETED_EDGE);
                }

            // If `a` is fully alive, we're done, return.
            if is_alive && !is_dead {
                alive.push(a.to_owned());
                return
            }

            // Else, we need to explore its deleted descendants.
            let e = Edge::zero(DELETED_EDGE);
            for (_, v) in self.iter_nodes(&branch, Some((a, Some(&e))))
                .take_while(|&(k, v)| k == a && v.flag <= e.flag | FOLDER_EDGE) {

                    debug!("candidate relative {:?}", v);
                    descendants.push((a.clone(), v.clone()))
                }
            let j = descendants.len();
            for k in i..j {
                let dest = descendants[k].1.dest.clone();
                self.find_alive_descendants_with_edges(branch,
                                                       &dest,
                                                       visited,
                                                       alive,
                                                       descendants);
            }
        }
    }


    /// Assumes all patches have been downloaded. The third argument
    /// `remote_patches` needs to contain at least all the patches we want
    /// to apply, and the fourth one `local_patches` at least all the patches the other
    /// party doesn't have.
    pub fn apply_patches(&mut self,
                         branch_name: &str,
                         r: &Path,
                         remote_patches: &HashMap<Hash, Patch>)
                         -> Result<(), Error> {
        let (pending, local_pending) = {
            let (changes, local) = try!(self.record(branch_name, &r));
            let mut p = Patch::empty();
            p.changes = changes.into_iter().flat_map(|x| x.into_iter()).collect();
            (p, local)
        };
        let mut new_patches_count = 0;
        let mut branch = self.open_branch(branch_name)?;
        for (p, patch) in remote_patches {
            debug!("apply_patches: {:?}", p);
            try!(self.apply_patches_rec(&mut branch, remote_patches,
                                        p, patch, &mut new_patches_count))
        }
        debug!("{} patches applied", new_patches_count);
        if new_patches_count > 0 {
            try!(self.output_changes_file(&branch, r));
            self.commit_branch(branch)?;
            debug!("output_repository");
            try!(self.output_repository(branch_name, &r, &pending, &local_pending));
            debug!("done outputting_repository");
        } else {
            // The branch needs to be committed in all cases to avoid
            // leaks.
            self.commit_branch(branch)?;
        }
        debug!("finished apply_patches");
        Ok(())
    }

    /// Lower-level applier. This function only applies patches as
    /// found in `patches_dir`, following dependencies recursively. It
    /// outputs neither the repository nor the "changes file" of the
    /// branch, necessary to exchange patches locally or over HTTP.
    pub fn apply_patches_rec(&mut self,
                             branch: &mut Branch,
                             patches: &HashMap<Hash, Patch>,
                             patch_hash: &Hash,
                             patch: &Patch,
                             new_patches_count: &mut usize)
                             -> Result<(), Error> {

        let internal = {
            if let Some(internal) = self.get_internal(patch_hash.as_ref()) {
                if self.get_patch(&branch.patches, &internal).is_some() {
                    None
                } else {
                    // Doesn't have patch, but the patch is known in
                    // another branch
                    Some(internal.to_owned())
                }
            } else {
                // The patch is totally new to the repository.
                let internal = self.new_internal(patch_hash.as_ref());
                Some(internal)
            }
        };
        if let Some(internal) = internal {

            debug!("Now applying patch {:?}", patch.name);
            debug!("pulling and applying patch {:?}", patch_hash);

            for dep in patch.dependencies.iter() {
                debug!("Applying dependency {:?}", dep);
                if let Some(patch) = patches.get(dep) {
                    try!(self.apply_patches_rec(branch,
                                                patches,
                                                &dep,
                                                patch,
                                                new_patches_count));
                } else {
                    debug!("Cannot find patch");
                }
                let dep_internal = self.get_internal(dep.as_ref()).unwrap().to_owned();
                self.put_revdep(&dep_internal, &internal)?;
            }

            // Sanakirja doesn't let us insert the same pair twice.
            self.put_external(&internal, patch_hash.as_ref())?;
            self.put_internal(patch_hash.as_ref(), &internal)?;

            let now = branch.apply_counter;
            branch.apply_counter += 1;
            try!(self.apply(branch, &patch, &internal, now));

            *new_patches_count += 1;

            Ok(())
        } else {
            debug!("Patch {:?} has already been applied", patch_hash);
            Ok(())
        }
    }

    /// Apply a patch from a local record: register it, give it a hash, and then apply.
    pub fn apply_local_patch(&mut self,
                             branch_name: &str,
                             working_copy: &Path,
                             patch: &Patch,
                             inode_updates: &[InodeUpdate],
                             is_pending: bool)
                             -> Result<(Hash, PatchId), Error> {

        info!("registering a patch with {} changes: {:?}",
              patch.changes.len(),
              patch);
        let mut branch = self.open_branch(branch_name)?;

        // let child_patch = patch.clone();
        let patches_dir = patches_dir(working_copy);

        let hash = patch.save(&patches_dir)?;

        let internal: PatchId = self.new_internal(hash.as_ref());

        for dep in patch.dependencies.iter() {
            let dep_internal = self.get_internal(dep.as_ref()).unwrap().to_owned();
            self.put_revdep(&dep_internal, &internal)?;
        }
        self.put_external(&internal, hash.as_ref())?;
        self.put_internal(hash.as_ref(), &internal)?;

        debug!("applying patch");
        let now = branch.apply_counter;
        self.apply(&mut branch, &patch, &internal, now)?;
        debug!("synchronizing tree: {:?}", inode_updates);
        for update in inode_updates.iter() {

            self.update_inode(&branch, &internal, update)?;
        }
        debug!("committing branch");
        if !is_pending {
            debug!("not pending, adding to changes");
            branch.apply_counter += 1;
            self.output_changes_file(&branch, working_copy)?;
        }
        self.commit_branch(branch)?;

        Ok((hash, internal))
    }

    /// Update the inodes/revinodes, tree/revtrees databases with the
    /// patch we just applied. This is because files don't really get
    /// moved or deleted before we apply the patch, they are just
    /// "marked as moved/deleted". This function does the actual
    /// update.
    fn update_inode(&mut self, branch: &Branch, internal: &PatchId, update: &InodeUpdate) -> Result<(), Error>{
        match *update {
            InodeUpdate::Add { ref line, ref meta, ref inode } => {
                let key = FileHeader {
                    metadata: *meta,
                    status: FileStatus::Ok,
                    key: Key {
                        patch: internal.clone(),
                        line: line.clone(),
                    },
                };
                // If this file addition was actually recorded.
                if self.get_nodes(&branch, &key.key, None).is_some() {
                    debug!("it's in here!: {:?} {:?}", key, inode);
                    self.replace_inodes(&inode, &key)?;
                    self.replace_revinodes(&key.key, &inode)?;
                }
            },
            InodeUpdate::Deleted { ref inode } => {
                // If this change was actually applied.
                debug!("deleted: {:?}", inode);
                let header = self.get_inodes(inode).unwrap().clone();
                debug!("deleted header: {:?}", header);
                let edge = Edge::zero(PARENT_EDGE|FOLDER_EDGE|DELETED_EDGE);
                if self.iter_nodes(&branch, Some((&header.key, Some(&edge))))
                    .take_while(|&(k,v)| k == &header.key && edge.flag == v.flag)
                    .any(|(_, v)| v.introduced_by == *internal)
                {
                    self.del_inodes(&inode, Some(&header))?;
                    self.del_revinodes(&header.key, Some(&inode))?;

                    let parent = self.get_revtree(&inode).unwrap().to_owned();
                    let parent = parent.as_file_id();
                    self.del_tree(&parent, None)?;
                    self.del_revtree(&inode, None)?;
                }
            },
            InodeUpdate::Moved { ref inode } => {
                // If this change was actually applied.
                let mut header = self.get_inodes(inode).unwrap().clone();
                let edge = Edge::zero(PARENT_EDGE|FOLDER_EDGE);
                if self.iter_nodes(&branch, Some((&header.key, Some(&edge))))
                    .take_while(|&(k, v)| k == &header.key && edge.flag == v.flag)
                    .any(|(_, v)| v.introduced_by == *internal)
                {
                    header.status = FileStatus::Ok;
                    self.replace_inodes(&inode, &header)?;
                    self.replace_revinodes(&header.key, &inode)?;
                }
            },
        }
        Ok(())
    }
}
