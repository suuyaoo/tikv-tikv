// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;

use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
use tikv_util::codec::bytes;
use tikv_util::{box_err, box_try, error, warn};

use super::{AdminObserver, Coprocessor, ObserverContext, Result as CopResult};
use crate::store::util;
use crate::Error;

fn strip_timestamp_if_exists(mut key: Vec<u8>) -> Vec<u8> {
    let mut slice = key.as_slice();
    let strip_len = match bytes::decode_bytes(&mut slice, false) {
        // It is an encoded key and the slice points to the remaining unparsable
        // part which most likely is timestamp. Note that the key can be a raw key
        // in valid encoded form, but treat it as a encoded key anyway.
        Ok(_) => slice.len(),
        // It must be a raw key so no need to strip.
        Err(_) => 0,
    };
    key.truncate(key.len() - strip_len);
    key
}

fn is_valid_split_key(key: &[u8], index: usize, region: &Region) -> bool {
    if key.is_empty() {
        warn!(
            "skip invalid split key: key is empty";
            "region_id" => region.get_id(),
            "index" => index,
        );
        return false;
    }

    if let Err(Error::KeyNotInRegion(..)) = util::check_key_in_region_exclusive(key, region) {
        warn!(
            "skip invalid split key: key is not in region";
            "key" => log_wrappers::Value::key(key),
            "region_id" => region.get_id(),
            "start_key" => log_wrappers::Value::key(region.get_start_key()),
            "end_key" => log_wrappers::Value::key(region.get_end_key()),
            "index" => index,
        );
        return false;
    }

    true
}

/// `SplitObserver` adjusts the split key so that it won't separate
/// multiple MVCC versions of a key into two regions.
#[derive(Clone)]
pub struct SplitObserver;

impl SplitObserver {
    fn on_split(
        &self,
        ctx: &mut ObserverContext<'_>,
        splits: &mut Vec<SplitRequest>,
    ) -> Result<(), String> {
        let ajusted_splits = std::mem::take(splits)
            .into_iter()
            .enumerate()
            .filter_map(|(i, mut split)| {
                let key = split.take_split_key();
                let key = strip_timestamp_if_exists(key);
                if is_valid_split_key(&key, i, ctx.region) {
                    split.split_key = key;
                    Some(split)
                } else {
                    None
                }
            })
            .coalesce(|prev, curr| {
                // Make sure that the split keys are sorted and unique.
                if prev.split_key < curr.split_key {
                    Err((prev, curr))
                } else {
                    warn!(
                        "skip invalid split key: key should not be larger than the previous.";
                        "region_id" => ctx.region.id,
                        "key" => log_wrappers::Value::key(&curr.split_key),
                        "previous" => log_wrappers::Value::key(&prev.split_key),
                    );
                    Ok(prev)
                }
            })
            .collect::<Vec<_>>();

        if ajusted_splits.is_empty() {
            Err("no valid key found for split.".to_owned())
        } else {
            // Rewrite the splits.
            *splits = ajusted_splits;
            Ok(())
        }
    }
}

impl Coprocessor for SplitObserver {}

impl AdminObserver for SplitObserver {
    fn pre_propose_admin(
        &self,
        ctx: &mut ObserverContext<'_>,
        req: &mut AdminRequest,
    ) -> CopResult<()> {
        match req.get_cmd_type() {
            AdminCmdType::Split => {
                if !req.has_split() {
                    box_try!(Err(
                        "cmd_type is Split but it doesn't have split request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut request = vec![req.take_split()];
                if let Err(e) = self.on_split(ctx, &mut request) {
                    error!(
                        "failed to handle split req";
                        "region_id" => ctx.region().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                // self.on_split() makes sure request is not empty, or it will return error.
                // so directly unwrap here.
                req.set_split(request.pop().unwrap());
            }
            AdminCmdType::BatchSplit => {
                if !req.has_splits() {
                    return Err(box_err!(
                        "cmd_type is BatchSplit but it doesn't have splits request, message maybe \
                         corrupted!"
                            .to_owned()
                    ));
                }
                let mut requests = req.mut_splits().take_requests().into();
                if let Err(e) = self.on_split(ctx, &mut requests) {
                    error!(
                        "failed to handle split req";
                        "region_id" => ctx.region().get_id(),
                        "err" => ?e,
                    );
                    return Err(box_err!(e));
                }
                req.mut_splits().set_requests(requests.into());
            }
            _ => return Ok(()),
        }
        Ok(())
    }
}

