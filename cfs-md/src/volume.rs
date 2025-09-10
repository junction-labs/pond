#![allow(unused)]

use bytes::Bytes;

use crate::volume::generated::VolumeRootArgs;

#[path = "./volume.fbs.rs"]
#[allow(unused_imports, dead_code, mismatched_lifetime_syntaxes)]
#[allow(
    unsafe_op_in_unsafe_fn,
    reason = "https://github.com/google/flatbuffers/pull/8638"
)]
#[rustfmt::skip]
mod generated;


#[derive(Clone)]
pub struct Volume {
    buf: Bytes,
}

impl Volume {
    pub fn new(name: &str, version: &str, s3_url: &str) -> Self {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(16);
        let name = builder.create_string(name);
        let version = builder.create_string(version);
        let s3_url = builder.create_string(s3_url);
        let volume_root = generated::VolumeRoot::create(
            &mut builder,
            &VolumeRootArgs {
                name: Some(name),
                version: Some(version),
                s3_url: Some(s3_url),
            },
        );

        builder.finish(volume_root, None);
        let (buf, n) = builder.collapse();
        let buf = Bytes::from(buf).slice(n..);
        Self { buf }
    }

    pub fn as_bytes(&self) -> Bytes {
        self.buf.clone()
    }

    pub fn name(&self) -> Option<&str> {
        self.as_volume_root().name()
    }

    pub fn version(&self) -> Option<&str> {
        self.as_volume_root().version()
    }

    pub fn s3_path(&self) -> Option<&str> {
        self.as_volume_root().s3_url()
    }

    fn as_volume_root(&self) -> generated::VolumeRoot<'_> {
        unsafe { generated::root_as_volume_root_unchecked(&self.buf) }
    }
}
