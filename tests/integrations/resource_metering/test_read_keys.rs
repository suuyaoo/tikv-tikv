// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::resource_metering::test_suite::MockReceiverServer;

use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::*;
use kvproto::resource_usage_agent::ResourceUsageRecord;
use kvproto::tikvpb::*;
use test_raftstore::*;
use test_util::alloc_port;
use tikv_util::config::ReadableDuration;
use tikv_util::HandyRwLock;

#[test]
pub fn test_read_keys() {
    // Create & start receiver server.
    let (tx, rx) = unbounded();
    let mut server = MockReceiverServer::new(tx);
    let port = alloc_port();
    let env = Arc::new(Environment::new(1));
    server.start_server(port, env.clone());

    // Create cluster.
    let (_cluster, client, mut ctx) = new_cluster(port, env);

    // Set resource group tag for enable resource metering.
    ctx.set_resource_group_tag("TEST-TAG".into());

    let mut ts = 0;

    // Write 10 key-value pairs.
    for n in 0..10 {
        let n = n.to_string().into_bytes();
        let (k, v) = (n.clone(), n);

        // Prewrite.
        ts += 1;
        let prewrite_start_version = ts;
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(k.clone());
        mutation.set_value(v.clone());
        must_kv_prewrite(
            &client,
            ctx.clone(),
            vec![mutation],
            k.clone(),
            prewrite_start_version,
        );

        // Commit.
        ts += 1;
        let commit_version = ts;
        must_kv_commit(
            &client,
            ctx.clone(),
            vec![k.clone()],
            prewrite_start_version,
            commit_version,
            commit_version,
        );
    }

    // PointGet
    ts += 1;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.set_key(b"0".to_vec());
    get_req.set_version(ts);
    let _ = client.kv_get(&get_req).unwrap(); // trigger thread register
    std::thread::sleep(Duration::from_secs(2));
    recv_read_keys(&rx);
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(!get_resp.has_error());
    let scan_detail_v2 = get_resp.get_exec_details_v2().get_scan_detail_v2();
    assert_eq!(scan_detail_v2.get_total_versions(), 1);
    assert_eq!(scan_detail_v2.get_processed_versions(), 1);
    assert!(scan_detail_v2.get_processed_versions_size() > 0);
    assert_eq!(get_resp.value, b"0".to_vec());

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 1);

    // Scan 0 ~ 4.
    ts += 1;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.set_start_key(b"0".to_vec());
    scan_req.set_limit(5);
    scan_req.set_version(ts);
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.pairs.len(), 5);

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 5);

    // Scan 0 ~ 9.
    ts += 1;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.set_start_key(b"0".to_vec());
    scan_req.set_limit(100);
    scan_req.set_version(ts);
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.pairs.len(), 10);

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 10);

    // Shutdown receiver server.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        server.shutdown_server().await;
    });
}

fn new_cluster(port: u16, env: Arc<Environment>) -> (Cluster<ServerCluster>, TikvClient, Context) {
    let (cluster, leader, ctx) = must_new_and_configure_cluster(|cluster| {
        cluster.cfg.resource_metering.receiver_address = format!("127.0.0.1:{}", port);
        cluster.cfg.resource_metering.precision = ReadableDuration::millis(100);
        cluster.cfg.resource_metering.report_receiver_interval = ReadableDuration::millis(400);
    });
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);
    (cluster, client, ctx)
}

fn must_recv_read_keys(rx: &Receiver<Vec<ResourceUsageRecord>>) -> u32 {
    const MAX_WAIT_SECS: u32 = 30;
    let duration = Duration::from_secs(1);
    for _ in 0..MAX_WAIT_SECS {
        std::thread::sleep(duration);
        let read_keys = recv_read_keys(rx);
        if read_keys > 0 {
            return read_keys;
        }
    }
    panic!("no read_keys");
}

fn recv_read_keys(rx: &Receiver<Vec<ResourceUsageRecord>>) -> u32 {
    let mut total = 0;
    while let Ok(records) = rx.try_recv() {
        for r in &records {
            total += r
                .get_record()
                .get_items()
                .iter()
                .map(|item| item.read_keys)
                .sum::<u32>();
        }
    }
    total
}
