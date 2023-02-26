use super::metadata::Metadata;
use std::io;
use std::path::Path;

#[derive(Debug)]
pub(crate) struct Db721File {
    pub(crate) mmap: memmap2::Mmap,
    pub(crate) metadata: Metadata,
}

pub(crate) fn parse_file(path: impl AsRef<Path>) -> io::Result<Db721File> {
    let f = std::fs::File::open(path)?;
    let mmap = unsafe { memmap2::Mmap::map(&f) }?;
    let metadata_len_start = mmap.len() - 4;
    let metadata_len = {
        let mut buf = [0; 4];
        buf.copy_from_slice(&mmap[metadata_len_start..]);
        u32::from_le_bytes(buf)
    };
    let metadata_start = metadata_len_start - metadata_len as usize;
    log::trace!("metadatalen: {metadata_len}bytes");
    let buf = &mmap[metadata_start..metadata_len_start];
    log::trace!("metadata_raw: {}", std::str::from_utf8(buf).unwrap());
    let metadata = Metadata::from_slice(buf).unwrap();
    log::trace!("metadata: {metadata:?}");

    log::trace!(
        "metadata print: {}",
        serde_json::to_string_pretty(&metadata).unwrap()
    );
    Ok(Db721File { mmap, metadata })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .init();
        let data_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../data/data-farms.db721");
        parse_file(data_path).unwrap();
    }
}
