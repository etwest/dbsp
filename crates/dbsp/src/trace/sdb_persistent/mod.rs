//! This module implements logic and datastructures to provide a trace that is
//! using on-disk storage with the help of RocksDB.

use std::cmp::Ordering;

use bincode::{
    config::{BigEndian, Fixint},
    decode_from_slice,
    enc::write::Writer,
    error::EncodeError,
    Decode, Encode,
};
use once_cell::sync::Lazy;
use splinterdb_rs::{DBConfig, SplinterDBWithColumnFamilies};
use uuid::Uuid;

mod sdb_interface;
mod cursor;
mod tests;
mod trace;

#[derive(Encode, Decode)]
enum SplinterMeta<V, T> {
    Empty,
    Value(V),
    ValueTimestamp(V, T),
}

#[derive(Encode, Decode)]
struct SplinterKey<K, V, T> {
    key: K,
    meta: SplinterMeta<V, T>,
}

#[derive(Encode, Decode)]
struct SplinterValue<R> {
    weight: R,
}

/// The cursor for the persistent trace.
pub use cursor::PersistentTraceCursor;
/// The persistent trace itself, it should be equivalent to the [`Spine`].
pub use trace::PersistentTrace;

/// DB in-memory cache size [bytes].
///
/// # TODO
/// Set to 1 GiB for now, in the future we should probably make this
/// configurable or determine it based on the system parameters.
const DB_DRAM_CACHE_SIZE: usize = 1024 * 1024 * 1024;

/// SplinterDB database maximum size [bytes].
/// # TODO
/// Set to 128 GiB for now, in the future we should probably make this
/// configurable or equal to some factor of device size
const DB_DISK_MAX_SIZE: usize = 128 * 1024 * 1024 * 1024;

/// Maximum key size for DBSP [bytes]
/// SplinterDB's true key size includes the key-space identifier (see below)
///
/// # TODO
/// Set to 60 bytes for now, in the future make this reactive to the
/// types in question
const DB_KEY_SIZE: usize = 60;

/// SplinterDB maximum value size [bytes]
///
/// # TODO
/// Set to 512 bytes for now, in the future make this reactive to the
/// types in question
const DB_VALUE_SIZE: usize = 512;

/// Path of the SplinterDB database file on disk.
///
/// # TODO
/// Eventually should be supplied as a command-line argument or provided in a
/// config file.
static DB_PATH: Lazy<String> = Lazy::new(|| format!("/tmp/{}.db", Uuid::new_v4()));

/// Config for the SplinterDB database
static DB_OPTS: Lazy<DBConfig> = Lazy::new(|| {
    DBConfig {
        cache_size_bytes: DB_DRAM_CACHE_SIZE,
        disk_size_bytes: DB_DISK_MAX_SIZE,
        max_key_size: DB_KEY_SIZE,
        max_value_size: DB_VALUE_SIZE,
        ..Default::default()
    }
});

/// The SplinterDB instance that holds all traces.
/// The traces are held in distinct key-spaces
///
/// # TODO
/// Is it okay that this will overwrite the existing database?
static SPLINTER_DB_INSTANCE: Lazy<SplinterDBWithColumnFamilies> = Lazy::new(|| {
    // Open the database (or create it if it doesn't exist
    let mut sdb = SplinterDBWithColumnFamilies::new();
    sdb.db_create(&DB_PATH.clone(), &DB_OPTS);
    sdb
});

/// Configuration we use for encodings/decodings to/from RocksDB data.
static BINCODE_CONFIG: bincode::config::Configuration<BigEndian, Fixint> =
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_big_endian();

/// A buffer that holds an encoded value.
///
/// Useful to keep around in code where serialization happens repeatedly as it
/// can avoid repeated [`Vec`] allocations.
#[derive(Default)]
struct ReusableEncodeBuffer(Vec<u8>);

impl ReusableEncodeBuffer {
    /// Creates a buffer with initial capacity of `cap` bytes.
    fn with_capacity(cap: usize) -> Self {
        ReusableEncodeBuffer(Vec::with_capacity(cap))
    }

    /// Encodes `val` into the buffer owned by this struct.
    ///
    /// # Returns
    /// - An error if encoding failed.
    /// - A reference to the buffer where `val` was encoded into. Makes sure the
    ///   buffer won't change until the reference out-of-scope again.
    fn encode<T: Encode>(&mut self, val: &T) -> Result<&[u8], EncodeError> {
        self.0.clear();
        bincode::encode_into_writer(val, &mut *self, BINCODE_CONFIG)?;
        Ok(&self.0)
    }
}

/// We can get the internal storage if we don't need the ReusableEncodeBuffer
/// anymore with the `From` trait.
impl From<ReusableEncodeBuffer> for Vec<u8> {
    fn from(r: ReusableEncodeBuffer) -> Vec<u8> {
        r.0
    }
}

impl Writer for &mut ReusableEncodeBuffer {
    /// Allows bincode to write into the buffer.
    ///
    /// # Note
    /// Client needs to ensure that the buffer is cleared in advance if we store
    /// something new. When possible use the [`Self::encode`] method instead
    /// which takes care of that.
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.0.extend(bytes);
        Ok(())
    }
}
