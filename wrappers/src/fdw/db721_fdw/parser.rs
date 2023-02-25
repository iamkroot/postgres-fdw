use super::metadata::Metadata;
use std::io::{self, Read, Seek};
use std::path::Path;

#[derive(Debug)]
pub(crate) struct Db721File {
    pub(crate) file: std::fs::File,
    pub(crate) metadata: Metadata,
}

pub(crate) fn parse_file(path: impl AsRef<Path>) -> io::Result<Db721File> {
    let mut f = std::fs::File::open(path)?;
    let pos = f.seek(std::io::SeekFrom::End(-4))?;
    let metadata_len = {
        let mut buf = [0; 4];
        f.read_exact(&mut buf)?;
        u32::from_le_bytes(buf)
    };
    log::trace!("metadatalen: {metadata_len}bytes");
    f.seek(io::SeekFrom::Start(pos - metadata_len as u64))?;
    let mut buf = vec![0; metadata_len as usize];
    f.read_exact(&mut buf)?;
    log::trace!("metadata_raw: {}", std::str::from_utf8(&buf).unwrap());
    f.seek(io::SeekFrom::Start(0))?;
    let metadata = Metadata::from_slice(&buf).unwrap();
    log::trace!("metadata: {metadata:?}");

    log::trace!(
        "metadata print: {}",
        serde_json::to_string_pretty(&metadata).unwrap()
    );
    Ok(Db721File { file: f, metadata })
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
