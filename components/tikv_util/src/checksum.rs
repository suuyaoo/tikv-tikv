// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub fn checksum_crc64_xor(
    checksum: u64,
    mut digest: crc64fast::Digest,
    k_suffix: &[u8],
    v: &[u8],
) -> u64 {
    digest.write(k_suffix);
    digest.write(v);
    checksum ^ digest.sum64()
}
