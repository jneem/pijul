use patch;
use graph;
use patch::{Change, Record, EdgeOp};
use error::Error;
use backend::*;
use graph::Graph;

use std;
use sanakirja::value::Value;
use rand;
use std::path::PathBuf;
use std::rc::Rc;
use std::cmp::min;
use conflict;
use std::collections::HashMap;

struct Diff<'a, T: 'a> {
    lines_a: Vec<Key<PatchId>>,
    contents_a: Vec<Value<'a, T>>,
    conflicts_ancestors: HashMap<usize, usize>,
    conflicts_descendants: HashMap<usize, usize>,
    current_conflict_ancestor: Option<usize>
}

impl<'a, T: Transaction + 'a> graph::LineBuffer<'a, T> for Diff<'a, T> {
    fn output_line(&mut self, k: &Key<PatchId>, c: Value<'a, T>) -> Result<(), Error> {
        self.lines_a.push(k.clone());
        self.contents_a.push(c);
        Ok(())
    }

    fn begin_conflict(&mut self) -> Result<(), Error> {
        self.current_conflict_ancestor = Some(self.lines_a.len());
        self.output_conflict_marker(conflict::START_MARKER)
    }

    fn end_conflict(&mut self) -> Result<(), Error> {
        self.output_conflict_marker(conflict::END_MARKER)?;

        let len = self.lines_a.len();
        self.conflicts_descendants.insert(self.current_conflict_ancestor.unwrap(), len);
        Ok(())
    }

    fn conflict_next(&mut self) -> Result<(), Error> {
        self.output_conflict_marker(conflict::SEPARATOR)
    }

    fn output_conflict_marker(&mut self, marker: &'a str) -> Result<(), Error> {
        let l = self.lines_a.len();
        self.lines_a.push(ROOT_KEY.clone());
        self.contents_a.push(Value::from_slice(marker.as_bytes()));
        self.conflicts_ancestors.insert(l, self.current_conflict_ancestor.unwrap());
        Ok(())
    }

}

struct Deletion {
    del: Option<Change>,
    conflict_ordering: Vec<Change>
}

enum Pending {
    None,
    Deletion(Deletion),
    Addition(Change)
}

impl Pending {
    fn take(&mut self) -> Pending {
        std::mem::replace(self, Pending::None)
    }
    fn is_none(&self) -> bool {
        if let Pending::None = *self { true } else { false }
    }
}

use std::ops::{Index, IndexMut};
struct Matrix<T> {
    rows: usize,
    cols: usize,
    v: Vec<T>,
}
impl<T: Clone> Matrix<T> {
    fn new(rows: usize, cols: usize, t: T) -> Self {
        Matrix {
            rows: rows,
            cols: cols,
            v: vec![t; rows * cols],
        }
    }
}
impl<T> Index<usize> for Matrix<T> {
    type Output = [T];
    fn index(&self, i: usize) -> &[T] {
        &self.v[i * self.cols..(i + 1) * self.cols]
    }
}
impl<T> IndexMut<usize> for Matrix<T> {
    fn index_mut(&mut self, i: usize) -> &mut [T] {
        &mut self.v[i * self.cols..(i + 1) * self.cols]
    }
}

impl<A: Transaction, R: rand::Rng> T<A, R> {
    fn delete_edges(&self,
                    branch: &Branch,
                    edges: &mut Vec<patch::NewEdge>,
                    key: Option<&Key<PatchId>>,
                    flag: EdgeFlags) {
        debug!("deleting edges");
        match key {
            Some(key) => {
                let ext_hash = self.external_hash(&key.patch);
                let edge = Edge::zero(flag);
                // For all non-pseudo edges pointing to `key`.
                for (k, v) in self.iter_nodes(&branch, Some((&key, Some(&edge))))
                    .take_while(|&(k, v)| {
                        k == key && v.flag >= flag && v.flag <= flag | (PSEUDO_EDGE | FOLDER_EDGE)
                    })
                    .filter(|&(_, v)| !v.flag.contains(PSEUDO_EDGE)) {
                    debug!("delete: {:?} {:?}", k, v);
                    debug!("actually deleting");
                    edges.push(patch::NewEdge {
                        from: Key {
                            patch: Some(ext_hash.to_owned()),
                            line: key.line.clone(),
                        },
                        to: Key {
                            patch: Some(self.external_hash(&v.dest.patch).to_owned()),
                            line: v.dest.line.clone(),
                        },
                        introduced_by: Some(self.external_hash(&v.introduced_by).to_owned()),
                    });
                }
            }
            None => {}
        }
    }

    fn add_lines(&self,
                 line_index: usize,
                 diff: &Diff<A>,
                 line_num: &mut LineId,
                 up_context: &Key<PatchId>,
                 down_context: &[Key<PatchId>],
                 lines: &[&[u8]])
                 -> patch::Change {
        debug!("add_lines: {:?}", lines);
        debug!("up_context: {:?}", up_context);
        debug!("down_context: {:?}", down_context);
        // If the up context is a conflict separator, link this hunk to its parent.
        let up_context = if *up_context == ROOT_KEY {
            diff.lines_a[
                *diff.conflicts_ancestors.get(&line_index).unwrap()
            ].to_owned()
        } else {
            up_context.to_owned()
        };
        // assert!(down_context.iter().all(|k| k != up_context));
        debug!("adding lines {}", lines.len());
        let changes = Change::NewNodes {
            up_context: vec![Key {
                                 patch: Some(self.external_hash(&up_context.patch).to_owned()),
                                 line: up_context.line.clone(),
                             }],
            down_context: down_context.iter()
                .map(|key| {
                    if *key == ROOT_KEY {
                        if let Some(ancestor) = diff.conflicts_ancestors.get(&line_index) {
                            if let Some(&i) = diff.conflicts_descendants.get(ancestor) {
                                if i < diff.lines_a.len() {
                                    let key = diff.lines_a[i].to_owned();
                                    return Some(Key {
                                        patch: Some(self.external_hash(&key.patch).to_owned()),
                                        line: key.line.clone(),
                                    })
                                }
                            }
                        }
                        None
                    } else {
                        Some(Key {
                            patch: Some(self.external_hash(&key.patch).to_owned()),
                            line: key.line.clone(),
                        })
                    }
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect(),
            line_num: line_num.clone(),
            flag: EdgeFlags::empty(),
            nodes: lines.iter().map(|x| x.to_vec()).collect(),
        };
        *line_num += lines.len();
        changes
    }

    // i0: index of the first deleted line.
    // i > i0: index of the first non-deleted line (might or might not exist).
    fn delete_lines(&self, branch: &Branch, diff: &Diff<A>, i0: usize, i1: usize) -> Deletion {
        debug!("delete_lines: {:?}", i1 - i0);
        let mut edges = Vec::with_capacity(i1 - i0);
        let mut contains_conflict = None;
        for i in i0..i1 {
            debug!("deleting line {:?}", diff.lines_a[i]);
            if diff.lines_a[i] == ROOT_KEY {
                // We've deleted a conflict marker.
                contains_conflict = diff.conflicts_ancestors.get(&i)
            } else {
                self.delete_edges(branch, &mut edges, Some(&diff.lines_a[i]), PARENT_EDGE)
            }
        }

        let mut conflict_ordering = Vec::new();

        // If this is an ordering conflict, add the relevant edges.
        if let Some(&ancestor) = contains_conflict {
            if i0 > 0 && i1 < diff.lines_a.len() && i0 > ancestor {
                let from_patch = self.external_hash(&diff.lines_a[i0 - 1].patch);
                let to_patch = self.external_hash(&diff.lines_a[i1].patch);
                // TODO: check that these two lines are not already linked.
                debug!("conflict ordering between {:?} and {:?}", i0-1, i1);
                conflict_ordering.push(Change::NewEdges {
                    edges: vec![patch::NewEdge {
                        from: Key {
                            patch: Some(from_patch.to_owned()),
                            line: diff.lines_a[i0-1].line.clone(),
                        },
                        to: Key {
                            patch: Some(to_patch.to_owned()),
                            line: diff.lines_a[i1].line.clone(),
                        },
                        introduced_by: None,
                    }],
                    op: EdgeOp::New { flag: EdgeFlags::empty() }
                })
            }
        }

        // Deletion
        Deletion {
            del: if edges.len() > 0 {
                Some(Change::NewEdges {
                    edges: edges,
                    op: EdgeOp::Map {
                        previous: PARENT_EDGE,
                        flag: PARENT_EDGE | DELETED_EDGE,
                    },
                })
            } else {
                None
            },
            conflict_ordering: conflict_ordering
        }
    }

    fn confirm_zombie(&self, branch: &Branch, file: Rc<PathBuf>, actions: &mut Vec<Record>, key: &Key<PatchId>) {
        debug!("confirm_zombie: {:?}", key);
        let mut zombie_edges = Vec::new();
        self.delete_edges(branch, &mut zombie_edges, Some(key), PARENT_EDGE|DELETED_EDGE);
        if !zombie_edges.is_empty() {
            actions.push(Record::Change {
                change: Change::NewEdges {
                    edges: zombie_edges,
                    op: EdgeOp::Map {
                        previous: PARENT_EDGE|DELETED_EDGE,
                        flag: PARENT_EDGE
                    }
                },
                file: file.clone(),
                conflict_reordering: Vec::new(),
            })
        }
    }

    fn local_diff<'a>(&'a self,
                      branch: &Branch,
                      file: &Rc<PathBuf>,
                      actions: &mut Vec<Record>,
                      line_num: &mut LineId,
                      diff: &Diff<A>,
                      b: &[&'a [u8]]) {
        debug!("local_diff {} {}", diff.contents_a.len(), b.len());

        // Compute the costs.

        // Start by computing the leading and trailing equalities.
        let leading_equals = diff.contents_a.iter()
            .skip(1)
            .zip(b.iter())
            .take_while(|&(a, b)| {
                let b: Value<'a, A> = Value::from_slice(b);
                a.clone().eq(b)
            })
            .count();

        let trailing_equals = if leading_equals >=
            std::cmp::min(diff.contents_a.len() - 1, b.len()) {
                0
            } else {
                (&diff.contents_a[leading_equals+1..]).iter().rev()
                    .zip((&b[leading_equals+1..]).iter().rev())
                    .take_while(|&(a, b)| {
                        let b: Value<'a, A> = Value::from_slice(b);
                        a.clone().eq(b)
                    })
                    .count()
            };


        // Now, if there are repeated lines in the middle,
        // (leading_equals + trailing_equals) might be larger than the
        // size of one of the files. Check that:
        let trailing_equals =
            min(trailing_equals,
                min(diff.contents_a.len() - leading_equals,
                    b.len() - leading_equals));
        debug!("equals: {:?} {:?}", leading_equals, trailing_equals);

        let mut opt = Matrix::new(diff.contents_a.len() + 1 - leading_equals - trailing_equals,
                                  b.len() + 1 - leading_equals - trailing_equals,
                                  0);
        debug!("opt.rows: {:?}, opt.cols: {:?}", opt.rows, opt.cols);
        if diff.contents_a.len() - trailing_equals - leading_equals > 0 {
            let mut i = diff.contents_a.len() - 1 - trailing_equals - leading_equals;
            loop {
                if b.len() - trailing_equals - leading_equals > 0 {
                    let mut j = b.len() - 1 - trailing_equals - leading_equals;
                    loop {
                        let contents_a_i = diff.contents_a[leading_equals + i].clone();
                        let contents_b_j: Value<'a, A> = Value::from_slice(&b[leading_equals + j]);
                        opt[i][j] = if contents_a_i.eq(contents_b_j) {
                            opt[i + 1][j + 1] + 1
                        } else {
                            std::cmp::max(opt[i + 1][j], opt[i][j + 1])
                        };
                        if j > 0 {
                            j -= 1
                        } else {
                            break;
                        }
                    }
                }
                if i > 0 {
                    i -= 1
                } else {
                    break;
                }
            }
        }

        // Create the patches.
        let mut i = 1;
        let mut j = 0;
        let mut oi = None;
        let mut oj = None;
        let mut last_alive_context = leading_equals;

        // The following variable, `pending_change`, is used to group
        // together consecutive additions/deletions (patches can be
        // self-conflicting if we don't do this).
        let mut pending_change = Pending::None;

        while i < opt.rows - 1 && j < opt.cols - 1 {
            debug!("i={}, j={}", i, j);
            let contents_a_i = diff.contents_a[leading_equals + i].clone();
            let contents_b_j: Value<'a, A> = Value::from_slice(b[leading_equals + j]);

            if contents_a_i.eq(contents_b_j) {
                // Two lines are equal. If we were collecting lines to
                // add or delete, we must stop here (in order to get
                // the smallest possible patch).
                debug!("eq: {:?} {:?}", i, j);
                if let Some(i0) = oi.take() {
                    // If we were collecting lines to delete (from i0, inclusive).
                    let i0 = leading_equals + i0;
                    let i = leading_equals + i;
                    debug!("deleting from {} to {} / {}", i0, i, diff.lines_a.len());
                    if i0 < i {
                        let dels = self.delete_lines(branch, &diff, i0, i);
                        if let Pending::Addition(pending) = pending_change.take() {
                            if let Some(del) = dels.del {
                                actions.push(Record::Replace {
                                    dels: del,
                                    adds: pending,
                                    file: file.clone(),
                                    conflict_reordering: dels.conflict_ordering,
                                })
                            } else {
                                actions.push(Record::Change {
                                    change: pending,
                                    file: file.clone(),
                                    conflict_reordering: dels.conflict_ordering,
                                })
                            }
                        } else if let Some(del) = dels.del {
                            actions.push(Record::Change {
                                change: del,
                                file: file.clone(),
                                conflict_reordering: dels.conflict_ordering,
                            })
                        } else {
                            for reord in dels.conflict_ordering {
                                actions.push(Record::Change {
                                    change: reord,
                                    file: file.clone(),
                                    conflict_reordering: Vec::new(),
                                })
                            }
                        }
                    }
                } else if let Some(j0) = oj.take() {
                    // Else, if we were collecting lines to add (from j0, inclusive).
                    let j0 = leading_equals + j0;
                    let j = leading_equals + j;
                    let i = leading_equals + i;
                    debug!("adding from {} to {} / {}, context {}", j0, j, b.len(), last_alive_context);
                    if j0 < j {
                        let adds = self.add_lines(last_alive_context, diff, line_num,
                                                  &diff.lines_a[last_alive_context], // up context
                                                  &diff.lines_a[i..i + 1], // down context
                                                  &b[j0..j]);
                        if let Pending::Deletion(pending) = pending_change.take() {
                            if let Some(del) = pending.del {
                                actions.push(Record::Replace {
                                    dels: del,
                                    adds: adds,
                                    file: file.clone(),
                                    conflict_reordering: pending.conflict_ordering,
                                });
                            } else {
                                actions.push(Record::Change {
                                    change: adds,
                                    file: file.clone(),
                                    conflict_reordering: pending.conflict_ordering,
                                })
                            }
                        } else {
                            actions.push(Record::Change {
                                change: adds,
                                file: file.clone(),
                                conflict_reordering: Vec::new(),
                            })
                        }
                    }
                }
                // "Confirm" line i / j, if it is a zombie line.
                self.confirm_zombie(branch, file.clone(), actions, &diff.lines_a[leading_equals + i]);

                // Move on to the next step.
                last_alive_context = leading_equals + i;
                i += 1;
                j += 1;
            } else {
                // Else, the current lines on each side are not equal:
                debug!("not eq");
                if opt[i + 1][j] >= opt[i][j + 1] {
                    // We will delete things starting from i (included).
                    // If we are currently adding stuff, finish that.
                    if let Some(j0) = oj.take() {
                        let j0 = leading_equals + j0;
                        let j = leading_equals + j;
                        let i = leading_equals + i;
                        debug!("adding from {} to {} / {}, context {}",
                               j0, j, b.len(), last_alive_context);

                        if j0 < j {
                            let adds = self.add_lines(last_alive_context, diff, line_num,
                                                      &diff.lines_a[last_alive_context], // up context
                                                      &diff.lines_a[i..i + 1], // down context
                                                      &b[j0..j]);

                            // Since we either always choose deletions
                            // or always additions, we can't have two
                            // consecutive replacements, hence there
                            // should be no pending change.
                            assert!(pending_change.is_none());
                            pending_change = Pending::Addition(adds)
                        }
                    }
                    if oi.is_none() {
                        oi = Some(i)
                    }
                    i += 1
                } else {
                    // We will add things starting from j.
                    // Are we currently deleting stuff?
                    if let Some(i0) = oi.take() {
                        let i0 = leading_equals + i0;
                        let i = leading_equals + i;
                        if i0 < i {
                            let dels = self.delete_lines(branch, &diff, i0, i);
                            // See comment about consecutive
                            // replacements in the previous case.
                            // assert!(pending_change.is_none());
                            pending_change = Pending::Deletion(dels)
                        }
                        last_alive_context = i0 - 1;
                    }
                    if oj.is_none() {
                        oj = Some(j)
                    }
                    j += 1
                }
            }
        }
        // Alright, we're at the end of either the original file, or the new version.
        debug!("i = {:?}, j = {:?}, line_a {:?}, b {:?}", i, j, diff.lines_a, b);
        if i < opt.rows - 1 {
            // There are remaining deletions, i.e. things from the
            // original file are not in the new version.
            let i = leading_equals + i;
            let j = leading_equals + j;
            if let Some(j0) = oj {
                // Before stopping, we were adding lines.
                let j0 = leading_equals + j0;
                if j0 < j {
                    debug!("line {}, adding remaining lines before the last deletion i={} j={} j0={}", line!(), i, j0, j);
                    let adds = self.add_lines(i - 1, diff, line_num,
                                              &diff.lines_a[i - 1], // up context
                                              &diff.lines_a[i..i + 1], // down context
                                              &b[j0..j]);
                    // We were doing an addition.
                    assert!(pending_change.is_none());
                    pending_change = Pending::Addition(adds)
                }
            }

            let dels = self.delete_lines(branch, &diff, i, diff.lines_a.len() - trailing_equals);
            if let Pending::Addition(pending) = pending_change.take() {
                if let Some(del) = dels.del {
                    actions.push(Record::Replace {
                        dels: del,
                        adds: pending,
                        file: file.clone(),
                        conflict_reordering: dels.conflict_ordering,
                    });
                } else {
                    actions.push(Record::Change {
                        change: pending,
                        file: file.clone(),
                        conflict_reordering: Vec::new(),
                    })
                }
            } else if let Some(del) = dels.del {
                actions.push(Record::Change {
                    change: del,
                    file: file.clone(),
                    conflict_reordering: dels.conflict_ordering,
                })
            }

        } else if j < opt.cols - 1 {
            // There's a pending block to add at the end of the file.
            let j = leading_equals + j;
            let mut i = leading_equals + i;
            if let Some(i0) = oi {
                // We were doing a deletion when we stopped.
                let i0 = leading_equals + i0;
                if i0 < i {
                    let dels = self.delete_lines(branch, &diff, i0, i);
                    // assert!(pending_change.is_none());
                    pending_change = Pending::Deletion(dels)
                }
                i = i0;
                debug!("line {}, adding lines after trailing equals: {:?} {:?}", line!(), diff.lines_a.len(), trailing_equals);
            }

            let adds = self.add_lines(
                i - 1,
                diff,
                line_num,
                // i is after the end of the non-equal section.
                &diff.lines_a[i - 1],
                if trailing_equals > 0 {
                    &diff.lines_a[diff.lines_a.len() - trailing_equals..
                                  diff.lines_a.len() - trailing_equals + 1]
                } else {
                    &[]
                },
                &b[j..b.len() - trailing_equals]
            );

            if let Pending::Deletion(pending) = pending_change.take() {
                if let Some(del) = pending.del {
                    actions.push(Record::Replace {
                        dels: del,
                        adds: adds,
                        file: file.clone(),
                        conflict_reordering: pending.conflict_ordering
                    });
                } else {
                    actions.push(Record::Change {
                        change: adds,
                        file: file.clone(),
                        conflict_reordering: pending.conflict_ordering
                    });
                }
            } else {
                actions.push(Record::Change {
                    change: adds,
                    file: file.clone(),
                    conflict_reordering: Vec::new(),
                })
            }
        }
    }

    pub fn diff<'a>(&'a self,
                    branch: &'a Branch,
                    path: &Rc<PathBuf>,
                    line_num: &mut LineId,
                    actions: &mut Vec<Record>,
                    redundant: &mut Vec<(Key<PatchId>, Edge)>,
                    a: &mut Graph,
                    lines_b: &[&[u8]])
                    -> Result<(), Error> {

        let mut d = Diff {
            lines_a: Vec::new(),
            contents_a: Vec::new(),
            conflicts_ancestors: HashMap::new(),
            current_conflict_ancestor: None,
            conflicts_descendants: HashMap::new(),
        };
        try!(self.output_file(&mut d, a, redundant));
        self.local_diff(branch,
                        path,
                        actions,
                        line_num,
                        &d,
                        &lines_b);
        Ok(())
    }
}

// buf_b should initially contain the whole file.
pub fn read_lines(buf_b: &[u8]) -> Vec<&[u8]> {
    let mut lines_b = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while j < buf_b.len() {
        if buf_b[j] == 0xa {
            lines_b.push(&buf_b[i..j + 1]);
            i = j + 1
        }
        j += 1;
    }
    if i < j {
        lines_b.push(&buf_b[i..j])
    }
    lines_b
}
