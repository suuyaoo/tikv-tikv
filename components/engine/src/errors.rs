// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error, result};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // RocksDb uses plain string as the error.
        RocksDb(msg: String) {
            from()
            display("RocksDb {}", msg)
        }
        // FIXME: It should not know Region.
        NotInRange( key: Vec<u8>, region_id: u64, start: Vec<u8>, end: Vec<u8>) {
            display(
                "Key {} is out of [region {}] [{}, {})",
                &log_wrappers::Value::key(&key), region_id, &log_wrappers::Value::key(&start), &log_wrappers::Value::key(&end)
            )
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            display("Protobuf {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstDecode(err: prost::DecodeError) {
            cause(err)
            display("Prost Decode {}", err)
        }
        #[cfg(feature = "prost-codec")]
        ProstEncode(err: prost::EncodeError) {
            cause(err)
            display("Prost Encode {}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            display("Io {}", err)
        }

        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        raft::Error::Store(raft::StorageError::Other(err.into()))
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Error {
        Error::ProstEncode(err.into())
    }
}

#[cfg(feature = "prost-codec")]
impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Error {
        Error::ProstDecode(err.into())
    }
}

impl From<Error> for kvproto::errorpb::Error {
    fn from(err: Error) -> kvproto::errorpb::Error {
        let mut errorpb = kvproto::errorpb::Error::default();
        errorpb.set_message(format!("{}", err));

        if let Error::NotInRange(key, region_id, start_key, end_key) = err {
            errorpb.mut_key_not_in_region().set_key(key);
            errorpb.mut_key_not_in_region().set_region_id(region_id);
            errorpb.mut_key_not_in_region().set_start_key(start_key);
            errorpb.mut_key_not_in_region().set_end_key(end_key);
        }

        errorpb
    }
}
