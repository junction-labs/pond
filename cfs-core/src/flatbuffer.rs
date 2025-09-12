#[allow(dead_code)]
pub(crate) trait IntoFlatBuffer {
    /// Destroys Self and converts it into its FlatBuffer-variant, returning the FlatBuffer-encoded
    /// buffer.
    fn into_flatbuffer(self) -> Vec<u8>;
}
