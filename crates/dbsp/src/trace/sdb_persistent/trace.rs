//! The implementation of the persistent trace.
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use bincode::{decode_from_slice, Decode, Encode};
use size_of::SizeOf;
use uuid::Uuid;

use splinterdb_rs::SplinterColumnFamily;
use crate::trace::sdb_persistent::sdb_interface::DbspSplinterFuncs;
use super::{PersistentTraceCursor, ReusableEncodeBuffer};
use super::{BINCODE_CONFIG, SPLINTER_DB_INSTANCE, DB_KEY_SIZE};
use crate::algebra::AddAssignByRef;
use crate::circuit::Activator;
use crate::time::{Antichain, Timestamp};
use crate::trace::cursor::Cursor;
use crate::trace::{
    AntichainRef, Batch, BatchReader, Builder, Consumer, DBData, DBTimestamp, DBWeight, HasZero,
    Trace, ValueConsumer,
};
use crate::NumEntries;

/// A persistent trace implementation.
///
/// - It mimics the (external) behavior of a `Spine`, but internally it uses a
///   SplinterDB ColumnFamily to store it's data.
///
/// - It also relies on merging and compaction of the SplinterDB key-value store
///   rather than controlling these aspects itself.
#[derive(SizeOf)]
pub struct PersistentTrace<B>
where
    B: Batch,
{
    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
    dirty: bool,
    approximate_len: usize,

    lower_key_bound: Option<B::Key>,
    lower_val_bound: Option<B::Val>,

    /// Where all the dataz is.
    #[size_of(skip)]
    cf: Arc<SplinterColumnFamily>,
    // #[size_of(skip)]
    // _cf_options: Options,

    _phantom: std::marker::PhantomData<B>,
}

impl<B> Default for PersistentTrace<B>
where
    B: Batch,
{
    fn default() -> Self {
        PersistentTrace::new(None)
    }
}

// impl<B> Drop for PersistentTrace<B>
// where
//     B: Batch,
// {
//     /// Deletes the RocksDB column family.
//     fn drop(&mut self) {
//         ROCKS_DB_INSTANCE
//             .drop_cf(&self.cf_name)
//             .expect("Can't delete CF?");
//     }
// }

impl<B> Debug for PersistentTrace<B>
where
    B: Batch,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: PersistentTraceCursor<B> = self.cursor();
        writeln!(f, "PersistentTrace:")?;
        while cursor.key_valid() {
            writeln!(f, "{:?}:", cursor.key())?;
            while cursor.val_valid() {
                writeln!(f, "{:?}:", cursor.val())?;
                cursor.map_times(|t, w| {
                    writeln!(
                        f,
                        "{}",
                        textwrap::indent(format!("{t:?} -> {w:?}").as_str(), "        ")
                    )
                    .expect("can't write out");
                });

                cursor.step_val();
            }
            cursor.step_key();
        }
        writeln!(f)?;
        Ok(())
    }
}

impl<B> Clone for PersistentTrace<B>
where
    B: Batch,
{
    fn clone(&self) -> Self {
        unimplemented!("PersistentTrace::clone")
    }
}

impl<B> NumEntries for PersistentTrace<B>
where
    B: Batch,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.key_count()
    }

    fn num_entries_deep(&self) -> usize {
        // Same as Spine implementation:
        self.num_entries_shallow()
    }
}

pub struct PersistentConsumer<B>
where
    B: Batch,
{
    __type: PhantomData<B>,
}

impl<B> Consumer<B::Key, B::Val, B::R, B::Time> for PersistentConsumer<B>
where
    B: Batch,
{
    type ValueConsumer<'a> = PersistentTraceValueConsumer<'a, B>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &B::Key {
        todo!()
    }

    fn next_key(&mut self) -> (B::Key, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &B::Key)
    where
        B::Key: Ord,
    {
        todo!()
    }
}

pub struct PersistentTraceValueConsumer<'a, B> {
    __type: PhantomData<&'a B>,
}

impl<'a, B> ValueConsumer<'a, B::Val, B::R, B::Time> for PersistentTraceValueConsumer<'a, B>
where
    B: Batch,
{
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (B::Val, B::R, B::Time) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}

impl<B> BatchReader for PersistentTrace<B>
where
    B: Batch,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor<'s> = PersistentTraceCursor<'s, B>;
    type Consumer = PersistentConsumer<B>;

    fn consumer(self) -> Self::Consumer {
        PersistentConsumer {
            __type: std::marker::PhantomData,
        }
    }

    /// The number of keys in the batch.
    ///
    /// This is an estimate as there is no way to get an exact count
    /// from SplinterDB.
    fn key_count(&self) -> usize {
    
        todo!();
    }

    /// The number of updates in the batch.
    ///
    /// This is an estimate, not an accurate count.
    fn len(&self) -> usize {
        self.approximate_len
    }

    fn lower(&self) -> AntichainRef<Self::Time> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<Self::Time> {
        self.upper.as_ref()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        PersistentTraceCursor::new(&self.cf, &self.lower_key_bound)
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let bound = if let Some(bound) = &self.lower_key_bound {
            max(bound, lower_bound).clone()
        } else {
            lower_bound.clone()
        };
        self.lower_key_bound = Some(bound);
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>) {
        todo!();
    }
}

impl<B> Trace for PersistentTrace<B>
where
    B: Batch + Clone + 'static,
    B::Time: DBTimestamp,
{
    type Batch = B;

    /// Create a new PersistentTrace.
    ///
    /// It works by creating a new column-family with a random name and
    /// configuring it with the right custom functions for comparison, merge,
    /// and compaction.
    ///
    /// # Arguments
    /// - `activator`: This is not used, None should be supplied.
    fn new(_activator: Option<Activator>) -> Self {
        let cf = SPLINTER_DB_INSTANCE
            .column_family_create::<DbspSplinterFuncs<B>>(DB_KEY_SIZE as u64)
            .expect("Can't create column family?");
        Self {
            lower: Antichain::from_elem(B::Time::minimum()),
            upper: Antichain::new(),
            approximate_len: 0,
            lower_key_bound: None,
            lower_val_bound: None,
            dirty: false,
            cf,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Recede to works by sending a `RecedeTo` command to every key in the
    /// trace.
    fn recede_to(&mut self, frontier: &B::Time) {
        // This is a no-op for the moment. We may implement it in the future.
    }

    fn exert(&mut self, _effort: &mut isize) {
        // This is a no-op for the persistent trace as RocksDB will decide when
        // to apply the merge / compaction operators etc.
    }

    fn consolidate(self) -> Option<Self::Batch> {
        // TODO: Not clear what the time of the batch should be here -- in Spine
        // the batch will not be `minimum` as it's created through merges of all
        // batches.
        //
        // In discussion with Leonid: We probably want to move consolidate out
        // of the trace trait.
        let mut builder = <Self::Batch as Batch>::Builder::new_builder(Self::Time::minimum());

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let v = cursor.val().clone();
                let mut w = B::R::zero();
                cursor.map_times(|_t, cur_w| {
                    w.add_assign_by_ref(cur_w);
                });
                let k = cursor.key().clone();

                builder.push((Self::Batch::item_from(k, v), w));
                cursor.step_val();
            }

            cursor.step_key();
        }

        Some(builder.done())
    }

    fn insert(&mut self, batch: Self::Batch) {
        assert!(batch.lower() != batch.upper());

        // Ignore empty batches.
        // Note: we may want to use empty batches to artificially force compaction.
        if batch.is_empty() {
            return;
        }

        self.dirty = true;
        self.lower = self.lower.as_ref().meet(batch.lower());
        self.upper = self.upper.as_ref().join(batch.upper());

        self.add_batch_to_cf(batch);
    }

    fn clear_dirty_flag(&mut self) {
        self.dirty = false;
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn truncate_values_below(&mut self, lower_bound: &Self::Val) {
        self.lower_val_bound = Some(if let Some(bound) = &self.lower_val_bound {
            max(bound, lower_bound).clone()
        } else {
            lower_bound.clone()
        });
    }

    fn lower_value_bound(&self) -> &Option<Self::Val> {
        &self.lower_val_bound
    }
}

impl<B> PersistentTrace<B>
where
    B: Batch,
{
    fn add_batch_to_cf(&mut self, batch: B) {
        use crate::trace::cursor::CursorDebug;

        let mut tmp_key = ReusableEncodeBuffer::default();
        let mut tmp_val = ReusableEncodeBuffer::default();

        let mut sstable = WriteBatch::default();
        let mut batch_cursor = batch.cursor();

        while batch_cursor.key_valid() {
            let key = batch_cursor.key();
            let vals: Values<B::Val, B::Time, B::R> = batch_cursor.val_to_vec();
            self.approximate_len += vals.len();
            for val in vals {
                for tw in val.1 {
                    let spd_key = SplinterKey {
                        key: key,
                        value: val.0,
                        timestamp: tw.0,
                    }
                    let spd_val = SplinterValue {
                        weight: tw.1,
                    }

                    let encoded_key = tmp_key.encode(&spd_key).expect("Can't encode `key`");
                    let encoded_val = tmp_val.encode(&spd_val).expect("Can't encode `vals`");
                    self.cf.insert(encoded_key, encoded_val).expect("Could not insert to");
                }
            }
            batch_cursor.step_key();
        }
    }
}
