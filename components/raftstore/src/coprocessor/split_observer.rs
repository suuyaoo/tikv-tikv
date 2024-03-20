// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::{AdminObserver, Coprocessor, ObserverContext, Result as CopResult};
use tikv_util::codec::bytes::{self, encode_bytes};

use crate::store::util;
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
use std::result::Result as StdResult;

/// `SplitObserver` adjusts the split key so that it won't separate
/// the data of a row into two region. It adjusts the key according
/// to the key format of `TiDB`.
#[derive(Clone)]
pub struct SplitObserver;

type Result<T> = StdResult<T, String>;

impl SplitObserver {
    fn adjust_key(&self, region: &Region, key: Vec<u8>) -> Result<Vec<u8>> {
        if key.is_empty() {
            return Err("key is empty".to_owned());
        }

        let key = match bytes::decode_bytes(&mut key.as_slice(), false) {
            Ok(x) => x,
            // It's a raw key, skip it.
            Err(_) => return Ok(key),
        };

        let key = encode_bytes(&key);
        match util::check_key_in_region_exclusive(&key, region) {
            Ok(()) => Ok(key),
            Err(_) => Err(format!(
                "key {} should be in ({}, {})",
                log_wrappers::Value::key(&key),
                log_wrappers::Value::key(&region.get_start_key()),
                log_wrappers::Value::key(&region.get_end_key()),
            )),
        }
    }

    fn on_split(
        &self,
        ctx: &mut ObserverContext<'_>,
        splits: &mut Vec<SplitRequest>,
    ) -> Result<()> {
        let (mut i, mut j) = (0, 0);
        let mut last_valid_key: Option<Vec<u8>> = None;
        let region_id = ctx.region().get_id();
        while i < splits.len() {
            let k = i;
            i += 1;
            {
                let split = &mut splits[k];
                let key = split.take_split_key();
                match self.adjust_key(ctx.region(), key) {
                    Ok(key) => {
                        if last_valid_key.as_ref().map_or(false, |k| *k >= key) {
                            warn!(
                                "key is not larger than previous, skip.";
                                "region_id" => region_id,
                                "key" => log_wrappers::Value::key(&key),
                                "previous" => log_wrappers::Value::key(last_valid_key.as_ref().unwrap()),
                                "index" => k,
                            );
                            continue;
                        }
                        last_valid_key = Some(key.clone());
                        split.set_split_key(key)
                    }
                    Err(e) => {
                        warn!(
                            "invalid key, skip";
                            "region_id" => region_id,
                            "index" => k,
                            "err" => ?e,
                        );
                        continue;
                    }
                }
            }
            if k != j {
                splits.swap(k, j);
            }
            j += 1;
        }
        if j == 0 {
            return Err("no valid key found for split.".to_owned());
        }
        splits.truncate(j);
        Ok(())
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
