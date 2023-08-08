//! Implements the cursor for the persistent trace.
//!
//! The cursor is a wrapper around the SplinterDB iterator and some custom logic
//! that ensures it behaves the same as our other cursors.

use std::sync::Arc;

use bincode::decode_from_slice;
use splinterdb_rs::splinter_cf::SplinterColumnFamily;
use splinterdb_rs::SplinterCursor;

use super::{ReusableEncodeBuffer, SplinterKey, SplinterMeta, SplinterValue, BINCODE_CONFIG};
use crate::algebra::PartialOrder;
use crate::trace::{Batch, Cursor};

#[derive(PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

/// The unpacked contents of a SplinterDB iterator
#[derive(Clone)]
struct IteratorContents<B: Batch> {
    key: B::Key,
    value: B::Val,
    timestamp: B::Time,
    weight: B::R,
}

/// One option for how to handle values.
/// Or use a flag.
enum CursorValue<B: Batch> {
    BEGIN,
    VALID(B::Val),
    END,
}

/// Multiple IteratorContents combined that all
/// share the same key and value
struct CursorContents<B: Batch> {
    key: B::Key,
    value: B::Val,
    tw_pairs: Vec<(B::Time, B::R)>,
}

/// The cursor for the persistent trace.
pub struct PersistentTraceCursor<'s, B: Batch + 's> {
    /// Iterator of the underlying SplinterDB column family instance.
    db_iter: SplinterCursor<'s>,

    /// Contents of the current SplinterDB key/value pair. Completely unpacked.
    ///
    /// Once we reached the end (or seeked to the end) will be set to None.
    cur_contents: Option<CursorContents<B>>,

    /// Lower key bound: the cursor must not expose keys below this bound.
    /// TODO: Do we need this??
    lower_key_bound: &'s Option<B::Key>,

    /// Temporary storage serializing seeked keys.
    tmp_key: ReusableEncodeBuffer,
}

impl<'s, B> PersistentTraceCursor<'s, B>
where
    B: Batch + 's,
{
    /// Loads, parses, and unpacks the SplinterDB iterator contents.
    ///
    /// # Returns
    /// - The key and its values.
    /// - `None` if the iterator is invalid.
    #[allow(clippy::type_complexity)]
    fn read_iterator_contents(
        iter: &SplinterCursor<'s>,
        lower_key_bound: &'s Option<B::Key>,
    ) -> Option<IteratorContents<B>> {
        let (splinter_key, splinter_val): (
            Option<SplinterKey<B::Key, B::Val, B::Time>>, Option<SplinterValue<B::R>>
        ) = match iter.get_curr() {
            None => (None, None),
            Some(result) => {
                let (k, _) = decode_from_slice(
                    result.key, BINCODE_CONFIG).expect("Can't decode current key"
                );
                let (v, _) = decode_from_slice(
                    result.value, BINCODE_CONFIG).expect("Can't decode current value"
                );
                (k, v)
            },
        };

        match splinter_key {
            None => return None,
            Some(spd_key) => {
                assert_eq!(true, splinter_val.is_some());
                if lower_key_bound.is_some() && &spd_key.key < lower_key_bound.as_ref().unwrap() {
                    return None;
                } else {
                    match spd_key.meta {
                        SplinterMeta::Empty => panic!(),
                        SplinterMeta::Value(_) => panic!(),
                        SplinterMeta::ValueTimestamp(v, t) => {
                            return Some(IteratorContents{
                                key: spd_key.key,
                                value: v,
                                timestamp: t,
                                weight: splinter_val.unwrap().weight,
                            });
                        }
                    }
                }
            }
        }
    }

    /// Given a current key and value, call next/prev until a new key or value is encountered
    /// recording timestamps and weights as we go
    /// Precondition: 
    ///    If direction is Forward:  Iterator points at the first SplinterKey with this key and value
    ///    If direction is Backward: Iterator points at the last SplinterKey with this key and value
    fn build_cursor_contents(
        iter: &mut SplinterCursor<'s>, 
        lower_key_bound: &'s Option<B::Key>,
        direction: Direction,
    ) -> Option<CursorContents<B>> {
        let start = Self::read_iterator_contents(iter, lower_key_bound)?;
        let mut ptr = start.clone();
        let mut ret = CursorContents {
            key: start.key.clone(),
            value: Some(start.value.clone()),
            tw_pairs: Vec::new(),
        };

        while start.key == ptr.key && start.value == ptr.value {
            ret.tw_pairs.push((ptr.timestamp, ptr.weight));

            if direction == Direction::Forward {
                iter.next();
            }
            else {
                iter.prev();
            }
            ptr = match Self::read_iterator_contents(iter, lower_key_bound) {
                None => break,
                Some(c) => c,
            }
        }
        Some(ret)
    }
}

impl<'s, B: Batch> PersistentTraceCursor<'s, B> {
    /// Creates a new [`PersistentTraceCursor`], requires to pass a handle to
    /// the column family of the trace.
    pub(super) fn new(cf: &'s Arc<SplinterColumnFamily>, lower_key_bound: &'s Option<B::Key>) -> Self {
        let mut tmp_key = ReusableEncodeBuffer(Vec::new());
        let mut db_iter = match lower_key_bound {
            None => cf.range(None).unwrap(),
            Some(k) => {
                let splinter_key = SplinterKey {
                    key: lower_key_bound,
                    meta: SplinterMeta::<B::Val, B::Time>::Empty,
                };
                let encoded_key = tmp_key.encode(&splinter_key).expect("Can't encode `key`");
                cf.range(Some(encoded_key)).unwrap()
            }
        };

        PersistentTraceCursor {
            cur_contents: Self::build_cursor_contents(&mut db_iter, lower_key_bound, Direction::Forward),
            db_iter: db_iter,
            lower_key_bound: lower_key_bound,
            tmp_key: tmp_key,
        }
    }
}

impl<'s, B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for PersistentTraceCursor<'s, B> {
    fn key_valid(&self) -> bool {
        self.cur_contents.is_some()
    }

    fn val_valid(&self) -> bool {
        self.cur_contents.is_some() && self.cur_contents.as_ref().unwrap().value.is_some()
    }

    fn key(&self) -> &B::Key {
        if !self.key_valid() {
            panic!();
        }
        &self.cur_contents.as_ref().unwrap().key
    }

    fn val(&self) -> &B::Val {
        if !self.val_valid() {
            panic!();
        }
        &self.cur_contents.as_ref().unwrap().value.unwrap()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        if self.key_valid() && self.val_valid() {
            self.cur_contents.as_ref().unwrap().tw_pairs
                .iter()
                .fold(init, |init, (time, val)| fold(init, time, val))
        }
        else {
            init
        }
    }

    fn fold_times_through<F, U>(&mut self, upper: &B::Time, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        if self.key_valid() && self.val_valid() {
            self.cur_contents.as_ref().unwrap().tw_pairs
                .iter()
                .filter(|(time, _)| time.less_equal(upper))
                .fold(init, |init, (time, val)| fold(init, time, val))
        }
        else {
            init
        }
    }

    fn weight(&mut self) -> B::R
    where
        B::Time: PartialEq<()>,
    {
        if !self.key_valid() {
            panic!();
        }
        self.cur_contents.as_ref().unwrap().tw_pairs[0].1.clone()
    }

    fn step_key(&mut self) {
        // get current splinter iterator contents
        let mut contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);

        // ensure iterator is either after the end or in range
        if contents.is_none() {
            self.db_iter.next();
            contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);
        }

        if self.cur_contents.is_some() && contents.is_some() {
            let mut c = contents.unwrap();
            while self.cur_contents.as_ref().unwrap().key >= c.key {
                self.db_iter.next();
                c = match Self::read_iterator_contents(&self.db_iter, self.lower_key_bound) {
                    None => break,
                    Some(c) => c,
                };
            }
        }

        self.cur_contents = Self::build_cursor_contents(&mut self.db_iter, self.lower_key_bound, Direction::Forward);
    }

    fn step_key_reverse(&mut self) {
        // get current splinter iterator contents
        let mut contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);

        // ensure iterator is either before beginning or in range
        if contents.is_none() {
            self.db_iter.prev();
            contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);
        }

        if self.cur_contents.is_some() && contents.is_some() {
            let mut c = contents.unwrap();
            while self.cur_contents.as_ref().unwrap().key <= c.key {
                self.db_iter.prev();
                c = match Self::read_iterator_contents(&self.db_iter, self.lower_key_bound) {
                    None => break,
                    Some(c) => c,
                };
            }
        }

        self.cur_contents = Self::build_cursor_contents(&mut self.db_iter, self.lower_key_bound, Direction::Backward);
    }

    fn seek_key(&mut self, key: &B::Key) {
        todo!();
    }

    fn seek_key_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        unimplemented!()
    }

    fn seek_key_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        unimplemented!()
    }

    fn seek_key_reverse(&mut self, key: &B::Key) {
        todo!();
    }

    fn step_val(&mut self) {
        // get current splinter iterator contents
        let mut contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);

        // ensure iterator is either after the end or in range
        if contents.is_none() {
            self.db_iter.next();
            contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);
        }

        if self.cur_contents.is_some() && contents.is_some() {
            let mut c = contents.unwrap();
            while self.cur_contents.as_ref().unwrap().key >= c.key
                  && self.cur_contents.as_ref().unwrap().value >= c.value
            {
                self.db_iter.next();
                c = match Self::read_iterator_contents(&self.db_iter, self.lower_key_bound) {
                    None => break,
                    Some(c) => c,
                };
            }
            contents = Some(c);
        }

        if self.cur_contents.is_none() || self.cur_contents.as_ref().unwrap().key == contents.as_ref().unwrap().key {
            self.cur_contents = Self::build_cursor_contents(&mut self.db_iter, self.lower_key_bound, Direction::Forward);
        } else {
            self.cur_contents.as_ref().unwrap().value = None;
        }
    }

    fn step_val_reverse(&mut self) {
        // get current splinter iterator contents
        let mut contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);

        // ensure iterator is either before beginning or in range
        if contents.is_none() {
            self.db_iter.prev();
            contents = Self::read_iterator_contents(&self.db_iter, self.lower_key_bound);
        }

        if self.cur_contents.is_some() && contents.is_some() {
            let mut c = contents.unwrap();
            while self.cur_contents.as_ref().unwrap().key <= c.key 
                  && self.cur_contents.as_ref().unwrap().value.unwrap() <= c.value
            {
                self.db_iter.prev();
                c = match Self::read_iterator_contents(&self.db_iter, self.lower_key_bound) {
                    None => break,
                    Some(c) => c,
                };
            }
        }

        if self.cur_contents.is_none() || self.cur_contents.as_ref().unwrap().key == contents.as_ref().unwrap().key {
            self.cur_contents = Self::build_cursor_contents(&mut self.db_iter, self.lower_key_bound, Direction::Backward);
        } else {
            self.cur_contents.as_ref().unwrap().value = None;
        }
    }

    fn seek_val(&mut self, val: &B::Val) {
        todo!();
    }

    fn seek_val_reverse(&mut self, val: &B::Val) {
        todo!();
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool,
    {
        todo!();
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool,
    {
        todo!();
    }

    fn rewind_keys(&mut self) {
        todo!();
    }

    fn fast_forward_keys(&mut self) {
        todo!();
    }

    fn rewind_vals(&mut self) {
        todo!();
    }

    fn fast_forward_vals(&mut self) {
        todo!();
    }
}
