//! Implements the cursor for the persistent trace.
//!
//! The cursor is a wrapper around the RocksDB iterator and some custom logic
//! that ensures it behaves the same as our other cursors.

use std::sync::Arc;

use bincode::decode_from_slice;
use splinterdb_rs::RangeIterator;

use super::trace::PersistedValue;
use super::{ReusableEncodeBuffer, Values, BINCODE_CONFIG, ROCKS_DB_INSTANCE};
use crate::algebra::PartialOrder;
use crate::trace::{Batch, Cursor};

#[derive(PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

/// The cursor for the persistent trace.
pub struct PersistentTraceCursor<'s, B: Batch + 's> {
    
}

impl<'s, B> PersistentTraceCursor<'s, B>
where
    B: Batch + 's,
{
    /// Loads the current key and its values from RocksDB and stores them in the
    /// [`PersistentTraceCursor`] struct.
    ///
    /// # Panics
    /// - In case the `db_iter` is invalid.
    fn update_current_key_weight(&mut self, direction: Direction) {
        todo!();
    }

    /// Loads the current key and its values from RocksDB.
    ///
    /// # Returns
    /// - The key and its values.
    /// - `None` if the iterator is invalid.
    #[allow(clippy::type_complexity)]
    fn read_key_val_weights(
        iter: &mut DBRawIterator<'s>,
        direction: Direction,
    ) -> (Option<B::Key>, Option<Values<B::Val, B::Time, B::R>>) {
        todo!();
    }
}

impl<'s, B: Batch> PersistentTraceCursor<'s, B> {
    /// Creates a new [`PersistentTraceCursor`], requires to pass a handle to
    /// the column family of the trace.
    pub(super) fn new(cf: &Arc<BoundColumnFamily>, lower_key_bound: &'s Option<B::Key>) -> Self {
        todo!();
    }
}

impl<'s, B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for PersistentTraceCursor<'s, B> {
    fn key_valid(&self) -> bool {
        todo!();
    }

    fn val_valid(&self) -> bool {
        todo!();
    }

    fn key(&self) -> &B::Key {
        todo!();
    }

    fn val(&self) -> &B::Val {
        &self.cur_vals.as_ref().unwrap()[self.val_idx as usize].0
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        todo!();
    }

    fn fold_times_through<F, U>(&mut self, upper: &B::Time, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        todo!();
    }

    fn weight(&mut self) -> B::R
    where
        B::Time: PartialEq<()>,
    {
        todo!();
    }

    fn step_key(&mut self) {
        todo!();
    }

    fn step_key_reverse(&mut self) {
        todo!();
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
        todo!();
    }

    fn step_val_reverse(&mut self) {
        todo!();
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
