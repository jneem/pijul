// This module defines the data structures representing contents of a
// pijul repository at any point in time. It is a Graph of Lines.
// Each Line corresponds to either a bit of contents of a file, or a
// bit of information about fs layout within the working directory
// (files and directories).
//
// Lines are organised in a Graph, which encodes which line belongs to what
// file, in what order they appear, and any conflict.


use super::backend::*;
use super::error::Error;
use super::conflict;

use std::collections::{HashMap, HashSet, BTreeSet};
use std::collections::hash_map::Entry;
use std::cmp::min;


// use patch::{HASH_SIZE, KEY_SIZE};
use std;
use rand;
pub const DIRECTORY_FLAG: usize = 0x200;

bitflags! {
    pub flags Flags: u8 {
        const LINE_HALF_DELETED = 4,
        const LINE_VISITED = 2,
        const LINE_ONSTACK = 1,
    }
}


/// The elementary datum in the representation of the repository state
/// at any given point in time. We need this structure (as opposed to
/// working directly on a branch) in order to add more data, such as
/// strongly connected component identifier, to each node.
#[derive(Debug)]
pub struct Line {
    pub key: Key<PatchId>,
    /// A unique identifier for the line. It is
    /// guaranteed to be universally unique if the line
    /// appears in a commit, and locally unique
    /// otherwise.
    flags: Flags,
    /// The status of the line with respect to a dfs of
    /// a graph it appears in. This is 0 or
    /// LINE_HALF_DELETED unless some dfs is being run.
    children: usize,
    n_children: usize,
    index: usize,
    lowlink: usize,
    scc: usize,
}


impl Line {
    pub fn is_zombie(&self) -> bool {
        self.flags.contains(LINE_HALF_DELETED)
    }
}

/// A graph, representing the whole content of the repository state at
/// a point in time. The encoding is a "flat adjacency list", where
/// each vertex contains a index `children` and a number of children
/// `n_children`. The children of that vertex are then
/// `&g.children[children .. children + n_children]`.
#[derive(Debug)]
pub struct Graph {
    /// Array of all alive lines in the graph. Line 0 is a dummy line
    /// at the end, so that all nodes have a common successor
    pub lines: Vec<Line>,
    /// Edge + index of the line in the "lines" array above. "None"
    /// means "dummy line at the end", and corresponds to line number
    /// 0.
    children: Vec<(Option<Edge>, VertexId)>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct VertexId(usize);

const DUMMY_VERTEX: VertexId = VertexId(0);

impl std::ops::Index<VertexId> for Graph {
    type Output = Line;
    fn index(&self, idx: VertexId) -> &Self::Output {
        self.lines.index(idx.0)
    }
}
impl std::ops::IndexMut<VertexId> for Graph {
    fn index_mut(&mut self, idx: VertexId) -> &mut Self::Output {
        self.lines.index_mut(idx.0)
    }
}

use std::io::Write;
use rustc_serialize::hex::ToHex;

impl Graph {
    fn children(&self, i: VertexId) -> &[(Option<Edge>, VertexId)] {
        let ref line = self[i];
        &self.children[line.children..line.children + line.n_children]
    }

    fn child(&self, i: VertexId, j: usize) -> &(Option<Edge>, VertexId) {
        &self.children[self[i].children + j]
    }

    pub fn debug<W:Write>(&self, mut w: W) -> Result<(), std::io::Error> {
        writeln!(w, "digraph {{")?;
        for (line, i) in self.lines.iter().zip(0..) {
            writeln!(w,
                     "n_{}[label=\"{}: {}\"];",
                     i, i, line.key.to_hex())?;
            for &(ref edge, VertexId(j)) in &self.children[line.children .. line.children + line.n_children] {
                if let Some(ref edge) = *edge {
                    writeln!(w,
                             "n_{}->n_{}[label=\"{:?} {}\"];",
                             i, j, edge.flag, edge.introduced_by.to_hex())?
                } else {
                    writeln!(w,
                             "n_{}->n_{}[label=\"none\"];",
                             i, j)?
                }
            }
        }
        writeln!(w, "}}")?;
        Ok(())
    }
}

use sanakirja::value::Value;
pub trait LineBuffer<'a, T: 'a + Transaction> {
    fn output_line(&mut self, key: &Key<PatchId>, contents: Value<'a, T>) -> Result<(), Error>;

    fn output_conflict_marker(&mut self, s: &'a str) -> Result<(), Error>;
    fn begin_conflict(&mut self) -> Result<(), Error> {
        self.output_conflict_marker(conflict::START_MARKER)
    }
    fn conflict_next(&mut self) -> Result<(), Error> {
        self.output_conflict_marker(conflict::SEPARATOR)
    }
    fn end_conflict(&mut self) -> Result<(), Error> {
        self.output_conflict_marker(conflict::END_MARKER)
    }
}

impl<'a, T: 'a + Transaction, W: std::io::Write> LineBuffer<'a, T> for W {
    fn output_line(&mut self, _: &Key<PatchId>, c: Value<T>) -> Result<(), Error> {
        for chunk in c {
            try!(self.write_all(chunk))
        }
        Ok(())
    }

    fn output_conflict_marker(&mut self, s: &'a str) -> Result<(), Error> {
        try!(self.write(s.as_bytes()));
        Ok(())
    }
}

struct DFS {
    visits: Vec<(usize, usize)>,
    counter: usize,
}

impl DFS {
    fn mark_discovered(&mut self, scc: usize) {
        if self.visits[scc].0 == 0 {
            self.visits[scc].0 = self.counter;
            self.counter += 1;
        }
    }

    fn mark_last_visit(&mut self, scc: usize) {
        self.visits[scc].1 = self.counter;
        self.counter += 1;
    }

    fn first_visit(&self, scc: usize) -> usize {
        self.visits[scc].0
    }

    fn last_visit(&self, scc: usize) -> usize {
        self.visits[scc].1
    }
}

impl Graph {
    /*
    /// This is basically just Tarjan's strongly connected component algorithm.
    fn tarjan_dfs(&mut self,
                  scc: &mut Vec<Vec<VertexId>>,
                  stack: &mut Vec<VertexId>,
                  index: &mut usize,
                  n_l: VertexId) {
        {
            let ref mut l = self[n_l];
            debug!("tarjan: {:?}", l.key);
            (*l).index = *index;
            (*l).lowlink = *index;
            (*l).flags = (*l).flags | LINE_ONSTACK | LINE_VISITED;
            debug!("{:?} {:?} chi", (*l).key, (*l).n_children);
        }
        stack.push(n_l);
        *index = *index + 1;

        for i in 0..self[n_l].n_children {
            let &(_, n_child) = self.child(n_l, i);
            if !self[n_child].flags.contains(LINE_VISITED) {

                self.tarjan_dfs(scc, stack, index, n_child);
                self[n_l].lowlink = std::cmp::min(self[n_l].lowlink, self[n_child].lowlink);
            } else {
                if self[n_child].flags.contains(LINE_ONSTACK) {
                    self[n_l].lowlink = min(self[n_l].lowlink, self[n_child].index)
                }
            }
        }

        if self[n_l].index == self[n_l].lowlink {

            let mut v = Vec::new();
            loop {
                match stack.pop() {
                    None => break,
                    Some(n_p) => {
                        self[n_p].scc = scc.len();
                        self[n_p].flags = self[n_p].flags ^ LINE_ONSTACK;
                        v.push(n_p);
                        if n_p == n_l {
                            break;
                        }
                    }
                }
            }
            scc.push(v);
        }
    }
    */

    /// Tarjan's strongly connected component algorithm, returning a
    /// vector of strongly connected components, where each SCC is a
    /// vector of vertex indices.
    fn tarjan(&mut self) -> Vec<Vec<VertexId>> {
        if self.lines.len() == 0 {
            return vec![vec![VertexId(0)]]
        }

        let mut call_stack = vec![(VertexId(1), 0, true)];

        let mut index = 0;
        let mut stack = Vec::new();
        let mut scc = Vec::new();
        while let Some((n_l, i, first_visit)) = call_stack.pop() {

            if first_visit {

                // First time we visit this node.
                let ref mut l = self[n_l];
                debug!("tarjan: {:?}", l.key);
                (*l).index = index;
                (*l).lowlink = index;
                (*l).flags = (*l).flags | LINE_ONSTACK | LINE_VISITED;
                debug!("{:?} {:?} chi", (*l).key, (*l).n_children);
                stack.push(n_l);
                index = index + 1;

            } else {

                let &(_, n_child) = self.child(n_l, i);
                self[n_l].lowlink = std::cmp::min(self[n_l].lowlink, self[n_child].lowlink);

            }

            let call_stack_length = call_stack.len();
            for j in i..self[n_l].n_children {
                let &(_, n_child) = self.child(n_l, j);
                if !self[n_child].flags.contains(LINE_VISITED) {

                    call_stack.push((n_l, j, false));
                    call_stack.push((n_child, 0, true));
                    break
                    // self.tarjan_dfs(scc, stack, index, n_child);
                } else {
                    if self[n_child].flags.contains(LINE_ONSTACK) {
                        self[n_l].lowlink = min(self[n_l].lowlink, self[n_child].index)
                    }
                }
            }
            if call_stack_length < call_stack.len() {
                // recursive call
                continue
            }
            // Here, all children of n_l have been visited.

            if self[n_l].index == self[n_l].lowlink {

                let mut v = Vec::new();
                loop {
                    match stack.pop() {
                        None => break,
                        Some(n_p) => {
                            self[n_p].scc = scc.len();
                            self[n_p].flags = self[n_p].flags ^ LINE_ONSTACK;
                            v.push(n_p);
                            if n_p == n_l {
                                break;
                            }
                        }
                    }
                }
                scc.push(v);
            }
        }
        scc
    }



    /*
    /// Run a depth-first search on this graph, assigning the
    /// `first_visit` and `last_visit` numbers to each node.
    fn dfs(&mut self,
           scc: &[Vec<VertexId>],
           n_scc: usize,
           dfs: &mut DFS,
           forward: &mut Vec<(Key<PatchId>, Edge)>) {

        debug!("dfs");
        dfs.mark_discovered(n_scc);

        // After Tarjan's algorithm, the SCC numbers are in reverse
        // topological order.
        //
        // Here, we want to visit the first child in topological
        // order, hence the one with the largest SCC number first.
        //

        // Collect all descendants of this SCC, in order of decreasing
        // SCC (notice the "-").
        let mut descendants = BTreeSet::new();

        for cousin in scc[n_scc].iter() {

            for &(_, n_child) in self.children(*cousin) {

                let child_component = self[n_child].scc;
                if child_component < n_scc {

                    // If this is a child and not a sibling.
                    descendants.insert( - (child_component as isize));

                }

            }

        }

        // SCCs to which we have forward edges.
        let mut forward_scc = HashSet::new();


        // Now run the DFS on all descendants.
        for child in descendants.iter() {

            let child = (-child) as usize;

            if dfs.first_visit(child) == 0 {

                // This SCC has not yet been visited, visit it.
                self.dfs(scc, child, dfs, forward);

            } else if dfs.last_visit(child) != 0 && dfs.first_visit(child) > dfs.first_visit(n_scc) {

                // This is a forward edge.
                forward_scc.insert(child);
                dfs.mark_last_visit(child);

            } else {

                dfs.mark_last_visit(child);

            }
        }

        dfs.mark_last_visit(n_scc);


        // After this, collect forward edges.
        for cousin in scc[n_scc].iter() {
            for &(ref edge, n_child) in self.children(*cousin) {
                if let Some(ref edge) = *edge {
                    if forward_scc.contains(&self[n_child].scc) && edge.flag.contains(PSEUDO_EDGE) {
                        forward.push((self[*cousin].key.clone(), edge.clone()))
                    }
                }
            }
        }
    }
    */

    /// Run a depth-first search on this graph, assigning the
    /// `first_visit` and `last_visit` numbers to each node.
    fn dfs(&mut self,
           scc: &[Vec<VertexId>],
           dfs: &mut DFS,
           forward: &mut Vec<(Key<PatchId>, Edge)>) {

        debug!("dfs");
        let mut call_stack = vec![(scc.len()-1, HashSet::new(), None)];
        while let Some((n_scc, mut forward_scc, descendants)) = call_stack.pop() {

            let mut descendants = if let Some(descendants) = descendants {
                descendants
            } else {

                // First visit / discovery of SCC n_scc.

                dfs.mark_discovered(n_scc);

                // After Tarjan's algorithm, the SCC numbers are in reverse
                // topological order.
                //
                // Here, we want to visit the first child in topological
                // order, hence the one with the largest SCC number first.
                //

                // Collect all descendants of this SCC, in order of decreasing
                // SCC.
                let mut descendants = Vec::new();
                for cousin in scc[n_scc].iter() {

                    for &(_, n_child) in self.children(*cousin) {

                        let child_component = self[n_child].scc;
                        if child_component < n_scc {

                            // If this is a child and not a sibling.
                            descendants.push(child_component)

                        }

                    }
                }
                descendants.sort();
                descendants
            };

            // SCCs to which we have forward edges.
            let mut recursive_call = None;
            while let Some(child) = descendants.pop() {

                if dfs.first_visit(child) == 0 {

                    // This SCC has not yet been visited, visit it.
                    recursive_call = Some(child);
                    break

                } else if dfs.last_visit(child) != 0 && dfs.first_visit(child) > dfs.first_visit(n_scc) {

                    // This is a forward edge.
                    forward_scc.insert(child);
                    dfs.mark_last_visit(child);

                } else {

                    dfs.mark_last_visit(child);

                }
            }
            if let Some(child) = recursive_call {
                call_stack.push((n_scc, forward_scc, Some(descendants)));
                call_stack.push((child, HashSet::new(), None));
                continue
            } else {
                dfs.mark_last_visit(n_scc);


                // After this, collect forward edges.
                for cousin in scc[n_scc].iter() {
                    for &(ref edge, n_child) in self.children(*cousin) {
                        if let Some(ref edge) = *edge {
                            if forward_scc.contains(&self[n_child].scc) && edge.flag.contains(PSEUDO_EDGE) {
                                forward.push((self[*cousin].key.clone(), edge.clone()))
                            }
                        }
                    }
                }
            }
        }
    }






    fn not_conflicting(&self, dfs: &DFS, scc: &[Vec<VertexId>], n: usize) -> bool {
        let not_conflicting = scc[n].len() == 1
            && dfs.first_visit(n) <= dfs.first_visit(0)
            && dfs.last_visit(n) >= dfs.last_visit(0)
            && !self[scc[n][0]].flags.contains(LINE_HALF_DELETED);
        if !not_conflicting {
            debug!("Conflict: scc[{}] = {:?}, dfs.first_visit({}) = {}, dfs.first_visit(0) = {}, dfs.last_visit({}) = {}, dfs.last_visit(0) = {}, contains(LINE_HALF_DELETED): {:?}",
                   n, scc[n],
                   n, dfs.first_visit(n), dfs.first_visit(0),
                   n, dfs.last_visit(n), dfs.last_visit(0),
                   self[scc[n][0]].flags.contains(LINE_HALF_DELETED))
        }
        not_conflicting
    }
}



impl<A: Transaction, R> T<A, R> {

    /// This function constructs a graph by reading the branch from the
    /// input key. It guarantees that all nodes but the first one (index
    /// 0) have a common descendant, which is index 0.
    pub fn retrieve<'a>(&'a self, branch: &Branch, key: &Key<PatchId>) -> Graph {

        let mut graph = Graph {
            lines: Vec::new(),
            children: Vec::new(),
        };
        // Insert last "dummy" line (so that all lines have a common descendant).
        graph.lines.push(Line {
            key: ROOT_KEY,
            flags: Flags::empty(),
            children: 0,
            n_children: 0,
            index: 0,
            lowlink: 0,
            scc: 0,
        });

        // Avoid the root key.
        let mut cache: HashMap<Key<PatchId>, VertexId> = HashMap::new();
        cache.insert(ROOT_KEY.clone(), DUMMY_VERTEX);
        let mut stack = vec![key.clone()];
        while let Some(key) = stack.pop() {
            let idx = VertexId(graph.lines.len());

            // If this key is not yet registered.
            if let Entry::Vacant(e) = cache.entry(key.clone()) {
                e.insert(idx);
                debug!("{:?}", key);
                let is_zombie = {
                    let mut tag = PARENT_EDGE | DELETED_EDGE;
                    let mut is_zombie = false;

                    // Find the first (k, v) after (key, tag).
                    let first_edge = Edge::zero(tag);
                    if let Some((k, v)) = self.iter_nodes(&branch, Some((&key, Some(&first_edge))))
                        .next() {
                            if *k == key && v.flag == tag {
                                is_zombie = true
                            }
                        }
                    if !is_zombie {
                        tag = PARENT_EDGE | DELETED_EDGE | FOLDER_EDGE;
                        let first_edge = Edge::zero(tag);
                        if let Some((k, v)) =
                               self.iter_nodes(&branch, Some((&key, Some(&first_edge)))).next() {
                            if *k == key && v.flag == tag {
                                is_zombie = true
                            }
                        }
                    }
                    is_zombie
                };
                debug!("is_zombie: {:?}", is_zombie);
                let mut l = Line {
                    key: key.clone(),
                    flags: if is_zombie {
                        LINE_HALF_DELETED
                    } else {
                        Flags::empty()
                    },
                    children: graph.children.len(),
                    n_children: 0,
                    index: 0,
                    lowlink: 0,
                    scc: 0,
                };

                for (_, v) in self.iter_nodes(&branch, Some((&key, None)))
                    .take_while(|&(k, v)| *k == key && v.flag <= PSEUDO_EDGE | FOLDER_EDGE) {

                        debug!("-> v = {:?}", v);
                        graph.children.push((Some(v.clone()), DUMMY_VERTEX));
                        l.n_children += 1;
                        stack.push(v.dest.clone())

                    }
                // If this key has no children, give it the dummy child.
                if l.n_children == 0 {
                    graph.children.push((None, DUMMY_VERTEX));
                    l.n_children = 1;
                }
                graph.lines.push(l)
            } else {
                debug!("already visited");
            }
        }
        for &mut (ref child_key, ref mut child_idx) in graph.children.iter_mut() {
            if let Some(ref child_key) = *child_key {
                if let Some(idx) = cache.get(&child_key.dest) {
                    *child_idx = *idx
                }
            }
        }
        graph
    }

}


impl<'a, A: Transaction + 'a, R> T<A, R> {
    pub fn output_file<B: LineBuffer<'a, A>>(&'a self,
                                             buf: &mut B,
                                             graph: &mut Graph,
                                             forward: &mut Vec<(Key<PatchId>, Edge)>)
                                             -> Result<(), Error> {

        debug!("output_file");

        let scc = graph.tarjan(); // SCCs are given here in reverse order.
        info!("There are {} SCC", scc.len());

        let mut dfs = DFS {
            visits: vec![(0,0); scc.len()],
            counter: 1,
        };

        graph.dfs(&scc, &mut dfs, forward);

        debug!("dfs done");

        let mut i = scc.len() - 1;

        let mut output_scc = HashSet::new();

        // The following loop goes through each SCC.
        loop {
            // Test for conflict
            //
            // scc[i] is not involved in a conflict if all the following conditions are met:
            //
            // 1. scc[i] contains at most one (and therefore exactly one) element.
            //
            // 2. In the DFS, the "visit interval" of scc[i]
            // (i.e. interval (first_visit[i], last_visit[i]))
            // contains the visit interval of the dummy line at the
            // end. This means that scc[i] is a bridge in the graph of
            // the SCCs.
            //
            // 3. The unique line in scc[i] is not half-deleted.

            debug_assert!(scc[i].len() >= 1); // All SCCs should have at least one vertex.

            if graph.not_conflicting(&dfs, &scc, i) {

                // SCC i is non-conflicting. Its successor is SCC i - 1.

                debug!("SCC {:?} = {:?} is not in conflict", i, scc[i]);

                let ref key = graph[scc[i][0]].key;
                if *key != ROOT_KEY {
                    if let Some(cont) = self.get_contents(&key) {
                        try!(buf.output_line(&key, cont))
                    }
                }
                output_scc.insert(i);

                if i == 0 {
                    break;
                } else {
                    i -= 1
                }

            } else {

                // Since some sides of the conflict might be empty,
                // and "begin" symbols might be different from end
                // symbols (depending on the `LineBuffer`
                // implementation), we output separators lazily, as
                // needed before outputting a new line, which is why
                // we need these booleans.
                let mut last_side_had_lines = false;
                let mut needs_separator = false;

                // `next` is the highest SCC (i.e. largest number)
                // known to be after the conflict.
                let mut next = 0;


                try!(buf.begin_conflict());

                // Run until the current SCC (i) is the first SCC
                // known to be after the conflict.
                while i != next {
                    debug!("i = {:?}, next = {:?}", i, next);
                    if last_side_had_lines {
                        needs_separator = true;
                    }

                    last_side_had_lines = false;

                    // If this SCC has not already been output.
                    if !output_scc.contains(&i) {
                        // Run a BFS, stopping at the first non-conflicting descendant.
                        let mut current = BTreeSet::new();
                        let mut children = BTreeSet::new();
                        current.insert(i);
                        while current.len() > 0 {
                            debug!("current: {:?}", current);
                            for &breadth in current.iter() {
                                // `breadth` is the number of an SCC.
                                output_scc.insert(breadth);

                                // For all cousins in this SCC
                                for &cousin in scc[breadth].iter() {

                                    let ref key = graph[cousin].key;
                                    if *key != ROOT_KEY {
                                        if let Some(cont) = self.get_contents(&key) {
                                            last_side_had_lines = true;
                                            if needs_separator {
                                                try!(buf.conflict_next());
                                                needs_separator = false;
                                            }
                                            try!(buf.output_line(&key, cont))
                                        }
                                    }

                                    // For all their children.
                                    for &(_, child) in graph.children(cousin) {

                                        // TODO: Beware of the forward edges
                                        if ! graph.not_conflicting(&dfs, &scc, graph[child].scc) {

                                            if graph[child].scc < graph[cousin].scc {
                                                children.insert(graph[child].scc);
                                            }

                                        } else {

                                            next = std::cmp::max(next, graph[child].scc)
                                        }
                                    }
                                }
                            }
                            current.clear();
                            std::mem::swap(&mut current, &mut children);
                        }
                    }

                    if i == 0 {
                        break
                    } else {
                        i -= 1
                    }
                }

                try!(buf.end_conflict());

                if next == 0 {
                    break;
                } else {
                    debug!("i = next = {:?}", next);
                    i = next
                }
            }
        }
        debug!("/output_file");
        Ok(())
    }
}

impl<'env, A: rand::Rng> MutTxn<'env, A> {
    pub fn remove_redundant_edges(&mut self,
                                  branch: &mut Branch,
                                  forward: &Vec<(Key<PatchId>, Edge)>)
                                  -> Result<(), Error> {

        for &(ref key, ref edge) in forward.iter() {

            try!(self.del_nodes(branch, key, Some(edge)));
            let mut reverse = Edge {
                dest: key.clone(),
                flag: edge.flag,
                introduced_by: edge.introduced_by.clone(),
            };
            reverse.flag.toggle(PARENT_EDGE);
            try!(self.del_nodes(branch, &edge.dest, Some(&reverse)));
        }
        Ok(())
    }
}
