#![allow(unused)]

use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;


#[path = "./volume_info.fbs.rs"]
#[allow(unused_imports, dead_code, mismatched_lifetime_syntaxes)]
#[allow(
    unsafe_op_in_unsafe_fn,
    reason = "https://github.com/google/flatbuffers/pull/8638"
)]
#[rustfmt::skip]
mod generated;

#[derive(Clone)]
pub struct VolumeInfo {
    buf: Bytes,
}

impl VolumeInfo {
    pub fn names_from_iter<S: AsRef<str>>(names: impl IntoIterator<Item = S>) -> Self {
        let mut builder = FlatBufferBuilder::new();

        let names: Vec<_> = names
            .into_iter()
            .map(|n| builder.create_string(n.as_ref()))
            .collect();
        let names = builder.create_vector(&names);

        let info = generated::VolumeInfo::create(
            &mut builder,
            &generated::VolumeInfoArgs {
                names: Some(names),
                versions: None,
            },
        );

        builder.finish_minimal(info);
        let (buf, n) = builder.collapse();
        let buf = Bytes::from(buf).slice(n..);
        Self { buf }
    }

    pub fn versions_from_iter<S: AsRef<str>>(versions: impl IntoIterator<Item = S>) -> Self {
        let mut builder = FlatBufferBuilder::new();

        let versions: Vec<_> = versions
            .into_iter()
            .map(|v| builder.create_string(v.as_ref()))
            .collect();
        let versions = builder.create_vector(&versions);

        let info = generated::VolumeInfo::create(
            &mut builder,
            &generated::VolumeInfoArgs {
                names: None,
                versions: Some(versions),
            },
        );

        builder.finish_minimal(info);
        let (buf, n) = builder.collapse();
        let buf = Bytes::from(buf).slice(n..);
        Self { buf }
    }

    pub fn as_bytes(&self) -> Bytes {
        self.buf.clone()
    }

    pub fn from_bytes(buf: &Bytes) -> Result<Self, flatbuffers::InvalidFlatbuffer> {
        static VERIFY: flatbuffers::VerifierOptions = flatbuffers::VerifierOptions {
            max_depth: 1,
            max_tables: 1,
            max_apparent_size: 4 * 1024 * 1024, /* arbitrary! think about this */
            ignore_missing_null_terminator: true,
        };

        generated::root_as_volume_info_with_opts(&VERIFY, buf)?;
        Ok(Self { buf: buf.clone() })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.as_root().names().unwrap_or_default().into_iter()
    }

    pub fn versions(&self) -> impl Iterator<Item = &str> {
        self.as_root().versions().unwrap_or_default().into_iter()
    }

    fn as_root(&self) -> generated::VolumeInfo<'_> {
        unsafe { generated::root_as_volume_info_unchecked(&self.buf) }
    }
}
