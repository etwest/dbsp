/// Implements the SplinterDB functions that we need
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Result;
use bincode::decode_from_slice;
use splinterdb_rs::{SdbRustDataFuncs, SdbMessage, SdbMessageType};
use crate::trace::Batch;
use super::{BINCODE_CONFIG, ReusableEncodeBuffer};
use crate::algebra::{AddAssignByRef, HasZero};
use crate::algebra::{PartialOrder, Lattice};

/// Implement the SplinterDB data functions for a specific Batch type
/// this will then be associated with a column family for implementing
/// the PersistedTrace struct
pub struct DbspSplinterFuncs<B: Batch> {
    _phantom: std::marker::PhantomData<B>,
}
impl<B: Batch> SdbRustDataFuncs for DbspSplinterFuncs<B> {
    /// TODO!
    /// Do we need to implement the key_hash, str_key, or str_msg?

    /// Custom key comparison function to compare based upon decoded keys
    ///
    /// TODO
    /// Is this actually necessary? 
    /// Perhaps the concern is that the encoding doesn't preserve the sort?
    fn key_comp(bytes1: &[u8], bytes2: &[u8]) -> Ordering {
        let (key_a, _) = decode_from_slice::<B::Key, _>(bytes1, BINCODE_CONFIG).expect("Can't decode_from_slice");
        let (key_b, _) = decode_from_slice::<B::Key, _>(bytes2, BINCODE_CONFIG).expect("Can't decode_from_slice");
        key_a.cmp(&key_b)
    }

    /// Custom merge 
    /// New message of type update
    /// old message of type insert or update
    fn merge(_key_bytes: &[u8], old_msg: SdbMessage, new_msg: SdbMessage) -> Result<SdbMessage> {
        let old_bytes = old_msg.data;
        let new_bytes = new_msg.data;

        let old_val: SplinterValue = decode_from_slice(&old_bytes, BINCODE_CONFIG).expect("Can't decode current value");
        let mut new_val: SplinterValue = decode_from_slice(&new_bytes, BINCODE_CONFIG).expect("Can't decode new value");
        new_val.weight = new_val.weight + old_val.weight;

        let mut tmp_buf = ReusableEncodeBuffer(Vec::new());
        return Ok(SdbMessage{
            msg_type: old_msg.msg_type,
            data: tmp_buf.encode(&new_val).expect("Can't encode updated value");
        });
    }

    /// Custom merge_final
    fn merge_final(key: &[u8], oldest_msg: SdbMessage) -> Result<SdbMessage> {
        let bytes = oldest_msg.data;
        let spd_value: SplinterValue = decode_from_slice(&old_bytes, BINCODE_CONFIG).expect("Can't decode current value");

        if spd_value.weight == 0 {
            oldest_msg.msg_type = SdbMessageType::DELETE;
        } else {
            oldest_msg.msg_type = SdbMessageType::INSERT;
        }
        return Ok(oldest_msg);
    }
}
