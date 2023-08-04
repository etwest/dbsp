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
use super::{PersistentTraceCursor, ReusableEncodeBuffer, Values};
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
///   RocksDB ColumnFamily to store it's data.
///
/// - It also relies on merging and compaction of the RocksDB key-value store
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
    cf_name: String,
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
        writeln!(f, "    rocksdb column {}:", self.cf_name)?;
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
        todo!();
    }

    fn num_entries_deep(&self) -> usize {
        // Same as Spine implementation:
        todo!();
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
        todo!();
    }

    fn lower(&self) -> AntichainRef<Self::Time> {
        todo!();
    }

    fn upper(&self) -> AntichainRef<Self::Time> {
        todo!();
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        todo!();
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        todo!();
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>) {
        todo!();
    }
}

/// The data-type that is persisted as the value in RocksDB.
#[derive(Debug)]
pub(super) enum PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    /// Values with key-weight pairs.
    Values(Values<V, T, R>),
    /// A tombstone for a key which had its values deleted (during merges).
    ///
    /// It signifies that the key shouldn't exist anymore. See also
    /// [`tombstone_compaction`] which gets rid of Tombstones during compaction.
    Tombstone,
}

/// Decode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Decode for PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        todo!();
    }
}

/// Encode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Encode for PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        todo!();
    }
}

/// A merge-op is what [`PersistentTrace`] supplies to the RocksDB instance to
/// indicate how to update the values.
#[derive(Clone, Debug)]
pub enum MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    /// A recede-to command to reset times of values.
    RecedeTo(T),
    /// An insertion of a new value or update of an existing value.
    Insert(Values<V, T, R>),
}

/// Decode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Decode for MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        todo!();
    }
}

/// Encode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Encode for MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
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
        todo!();
    }

    /// Recede to works by sending a `RecedeTo` command to every key in the
    /// trace.
    fn recede_to(&mut self, frontier: &B::Time) {
        todo!();
    }

    fn exert(&mut self, _effort: &mut isize) {
        // This is a no-op for the persistent trace as RocksDB will decide when
        // to apply the merge / compaction operators etc.
    }

    fn consolidate(self) -> Option<Self::Batch> {
        todo!();
    }

    fn insert(&mut self, batch: Self::Batch) {
        todo!();
    }

    fn clear_dirty_flag(&mut self) {
        todo!();
    }

    fn dirty(&self) -> bool {
        todo!();
    }

    fn truncate_values_below(&mut self, lower_bound: &Self::Val) {
        todo!();
    }

    fn lower_value_bound(&self) -> &Option<Self::Val> {
        todo!();
    }
}

impl<B> PersistentTrace<B>
where
    B: Batch,
{
    fn add_batch_to_cf(&mut self, batch: B) {
        todo!();
    }
}
