// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! DO NOT MOVE THIS FILE. IT WILL BE PARSED BY `fuzz/cli.rs`. SEE `discover_fuzz_targets()`.

mod util;

use std::io::Cursor;

use anyhow::Result;

use self::util::ReadLiteralExt;

#[inline(always)]
pub fn fuzz_codec_bytes(data: &[u8]) -> Result<()> {
    let _ = tikv_util::codec::bytes::encode_bytes(data);
    let _ = tikv_util::codec::bytes::encode_bytes_desc(data);
    let _ = tikv_util::codec::bytes::encoded_bytes_len(data, true);
    let _ = tikv_util::codec::bytes::encoded_bytes_len(data, false);
    Ok(())
}

#[inline(always)]
pub fn fuzz_codec_number(data: &[u8]) -> Result<()> {
    use tikv_util::codec::number::NumberEncoder;
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u64()?;
        let mut buf = vec![];
        let _ = buf.encode_u64(n);
        let _ = buf.encode_u64_le(n);
        let _ = buf.encode_u64_desc(n);
        let _ = buf.encode_var_u64(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_i64()?;
        let mut buf = vec![];
        let _ = buf.encode_i64(n);
        let _ = buf.encode_i64_le(n);
        let _ = buf.encode_i64_desc(n);
        let _ = buf.encode_var_i64(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_f64()?;
        let mut buf = vec![];
        let _ = buf.encode_f64(n);
        let _ = buf.encode_f64_le(n);
        let _ = buf.encode_f64_desc(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u32()?;
        let mut buf = vec![];
        let _ = buf.encode_u32(n);
        let _ = buf.encode_u32_le(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_i32()?;
        let mut buf = vec![];
        let _ = buf.encode_i32_le(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u16()?;
        let mut buf = vec![];
        let _ = buf.encode_u16(n);
        let _ = buf.encode_u16_le(n);
    }
    {
        let buf = data.to_owned();
        let _ = tikv_util::codec::number::decode_u64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u32(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u32_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i32_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u16(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u16_le(&mut buf.as_slice());
    }
    Ok(())
}
