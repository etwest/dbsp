/// Implements the SplinterDB functions that we need
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Result;
use bincode::decode_from_slice;
use splinterdb_rs::{SdbRustDataFuncs, SdbMessage, SdbMessageType};
use super::trace::{PersistedValue, MergeOp};
use crate::trace::Batch;
use super::{BINCODE_CONFIG, Values, ReusableEncodeBuffer};
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
    fn merge(key_bytes: &[u8], old_msg: SdbMessage, new_msg: SdbMessage) -> Result<SdbMessage> {
        let old_bytes = old_msg.data;
        let new_bytes = new_msg.data;

        let (_key, _) = decode_from_slice::<B::Key, _>(key_bytes, BINCODE_CONFIG).expect("Can't decode_from_slice!");
        let mut vals: Values<B::Val, B::Time, B::R> = {
            let (decoded_val, _): (PersistedValue<B::Val, B::Time, B::R>, usize) =
                decode_from_slice(&old_bytes, BINCODE_CONFIG).expect("Can't decode current value");
            match decoded_val {
                PersistedValue::Values(vals) => vals,
                PersistedValue::Tombstone => Vec::new(),
            }
        };

        let (decoded_update, _): (MergeOp<B::Val, B::Time, B::R>, usize) = 
            decode_from_slice(&new_bytes, BINCODE_CONFIG).expect("Can't decode update!");
        
        match decoded_update {
            MergeOp::Insert(new_vals) => {
                for (v, tws) in new_vals {
                    let mut found_v = false;
                    let mut found_t = false;
                    let mut delete_zero_w = false;

                    for (existing_v, ref mut existing_tw) in vals.iter_mut() {
                        if existing_v == &v {
                            for (t, w) in &tws {
                                for (existing_t, ref mut existing_w) in existing_tw.iter_mut() {
                                    if existing_t == t {
                                        existing_w.add_assign_by_ref(w);
                                        found_t = true;
                                        if existing_w.is_zero() {
                                            delete_zero_w = true;
                                        }
                                    }
                                }
                                if !found_t {
                                    existing_tw.push((t.clone(), w.clone()));
                                    existing_tw.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                                    break;
                                } else if delete_zero_w {
                                    existing_tw.retain(|(_, w)| !w.is_zero());
                                }
                            }
                            found_v = true;
                            break;
                        }
                    }

                    if !found_v {
                        vals.push((v, tws));
                    } else {
                        // Delete values which ended up with zero weights
                        vals.retain(|(_ret_v, ret_tws)| {
                            ret_tws
                                .iter()
                                .filter(|(_ret_t, ret_w)| !ret_w.is_zero())
                                .count()
                                != 0
                        });
                    }
                }
            }
            MergeOp::RecedeTo(frontier) => {
                for (_existing_v, ref mut existing_tw) in vals.iter_mut() {
                    let mut modified_t = false;
                    for (ref mut existing_t, _existing_w) in existing_tw.iter_mut() {
                        // I think due to this being sorted by Ord we have to
                        // walk all of them (see also `map_batches_through`):
                        if !existing_t.less_equal(&frontier) {
                            // example: times [1,2,3,4], frontier 3 -> [1,2,3,3]
                            *existing_t = existing_t.meet(&frontier);
                            modified_t = true;
                        }
                    }

                    if modified_t {
                        // I think due to this being sorted by `Ord` we can't
                        // rely on `recede_to(x)` having only affected
                        // consecutive elements, so we create a new (t, w)
                        // vector with a hashmap that we sort again.
                        let mut new_tw = HashMap::with_capacity(existing_tw.len());
                        for (cur_t, cur_w) in &*existing_tw {
                            new_tw
                                .entry(cur_t.clone())
                                .and_modify(|w: &mut B::R| w.add_assign_by_ref(cur_w))
                                .or_insert_with(|| cur_w.clone());
                        }
                        let new_tw_vec: Vec<(B::Time, B::R)> = new_tw.into_iter().collect();
                        *existing_tw = new_tw_vec;
                    }
                    existing_tw.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                }

                // Delete values which ended up with zero weights
                vals.retain(|(_ret_v, ret_tws)| {
                    ret_tws
                        .iter()
                        .filter(|(_ret_t, ret_w)| !ret_w.is_zero())
                        .count()
                        != 0
                });
            }
        }

        // TODO: We can probably avoid re-sorting in some cases (see if found_v above)?
        vals.sort_unstable_by(|v1, v2| v1.0.cmp(&v2.0));

        let vals = if !vals.is_empty() {
            PersistedValue::Values(vals)
        } else {
            PersistedValue::Tombstone
        };

        // TODO: What is the right capacity here!?!?
        let mut buf = ReusableEncodeBuffer::with_capacity(new_bytes.len());
        buf.encode(&vals).expect("Can't encode `vals`");
        Ok(SdbMessage{
            msg_type: SdbMessageType::UPDATE,
            data: buf.into(),
        })
    }

    /// Custom merge_final
    fn merge_final(key: &[u8], oldest_msg: SdbMessage) -> Result<SdbMessage> {
        todo!();
    }
}
