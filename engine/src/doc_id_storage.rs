use std::iter::Cloned;
use thiserror::Error;

pub trait DocumentIdStorage<'a> {
    type Iter: Iterator<Item = u64> + 'a;

    fn get(&'a self, key: u64) -> Option<Self::Iter>;
}

pub trait DocumentIdStorageBuilder {
    type Output: for<'a> DocumentIdStorage<'a>;
    type Error;

    fn add_entry(&mut self, doc_ids: Vec<u64>) -> Result<u64, Self::Error>;
    fn build(self) -> Result<Self::Output, Self::Error>;
}

pub enum DuplicatesIterator<I>
where
    I: Iterator<Item = u64>,
{
    Duplicates(I),
    NoDuplicates(Option<u64>),
}

impl<I> DuplicatesIterator<I>
where
    I: Iterator<Item = u64>,
{
    fn duplicates(iter: I) -> Self {
        Self::Duplicates(iter)
    }

    fn no_duplicates(key: u64) -> Self {
        Self::NoDuplicates(Some(key))
    }
}

impl<I> Iterator for DuplicatesIterator<I>
where
    I: Iterator<Item = u64>,
{
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Duplicates(iter) => iter.next(),
            Self::NoDuplicates(fst_id) => fst_id.take(),
        }
    }
}

const DUPES_TAG: u64 = 1 << 63;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct VecStorage(Vec<Vec<u64>>);

impl<'a> DocumentIdStorage<'a> for VecStorage {
    type Iter = DuplicatesIterator<Cloned<std::slice::Iter<'a, u64>>>;

    fn get(&'a self, key: u64) -> Option<Self::Iter> {
        Some(if key & DUPES_TAG != 0 {
            DuplicatesIterator::duplicates(self.0[(key ^ DUPES_TAG) as usize].iter().cloned())
        } else {
            DuplicatesIterator::no_duplicates(key)
        })
    }
}

#[cfg(feature = "rkyv")]
impl<'a> DocumentIdStorage<'a> for ArchivedVecStorage {
    type Iter = DuplicatesIterator<Cloned<std::slice::Iter<'a, u64>>>;

    fn get(&'a self, key: u64) -> Option<Self::Iter> {
        Some(if key & DUPES_TAG != 0 {
            DuplicatesIterator::duplicates(self.0[(key ^ DUPES_TAG) as usize].iter().cloned())
        } else {
            DuplicatesIterator::no_duplicates(key)
        })
    }
}

#[derive(Error, Debug)]
pub enum VecStorageBuilderError {
    #[error("document IDs with the 64th bit set to 1 aren't allowed")]
    DupesTagSet,
}

#[derive(Default)]
pub struct VecStorageBuilder(Vec<Vec<u64>>);

impl DocumentIdStorageBuilder for VecStorageBuilder {
    type Output = VecStorage;
    type Error = VecStorageBuilderError;

    fn add_entry(&mut self, doc_ids: Vec<u64>) -> Result<u64, Self::Error> {
        let fst_id = if doc_ids.len() > 1 {
            // check for dupes tag
            if doc_ids.iter().any(|doc_id| doc_id & DUPES_TAG != 0) {
                return Err(VecStorageBuilderError::DupesTagSet);
            }

            self.0.push(doc_ids);
            ((self.0.len() - 1) as u64) | DUPES_TAG
        } else {
            if doc_ids[0] & DUPES_TAG != 0 {
                return Err(VecStorageBuilderError::DupesTagSet);
            }

            doc_ids[0]
        };

        Ok(fst_id)
    }

    fn build(self) -> Result<Self::Output, Self::Error> {
        Ok(VecStorage(self.0))
    }
}

#[cfg(feature = "compression")]
pub mod compression {
    use super::*;
    use q_compress::errors::QCompressError;
    pub use q_compress::CompressorConfig;
    use q_compress::{BitWriter, Compressor};

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(
        feature = "rkyv",
        derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
    )]
    pub struct CompressedStorage {
        chunk_offsets: Vec<usize>,
        compressed_indices: Vec<u8>,
    }

    impl<'a> DocumentIdStorage<'a> for CompressedStorage {
        type Iter = DuplicatesIterator<std::vec::IntoIter<u64>>;

        fn get(&'a self, key: u64) -> Option<Self::Iter> {
            Some(if key & DUPES_TAG != 0 {
                let offset = (key ^ DUPES_TAG) as usize;
                let mut reader = q_compress::BitReader::from(&self.compressed_indices);
                let decompressor = q_compress::Decompressor::<u64>::default();
                let flags = decompressor.header(&mut reader).unwrap();
                reader.seek(self.chunk_offsets[offset] * 8);
                let chunk = decompressor.chunk(&mut reader, &flags).unwrap().unwrap();
                DuplicatesIterator::duplicates(chunk.nums.into_iter())
            } else {
                DuplicatesIterator::no_duplicates(key)
            })
        }
    }

    #[cfg(feature = "rkyv")]
    impl<'a> DocumentIdStorage<'a> for ArchivedCompressedStorage {
        type Iter = DuplicatesIterator<std::vec::IntoIter<u64>>;

        fn get(&'a self, key: u64) -> Option<Self::Iter> {
            Some(if key & DUPES_TAG != 0 {
                let offset = (key ^ DUPES_TAG) as usize;
                let mut reader = q_compress::BitReader::from(&self.compressed_indices);
                let decompressor = q_compress::Decompressor::<u64>::default();
                let flags = decompressor.header(&mut reader).unwrap();
                reader.seek((self.chunk_offsets[offset] as usize) * 8);
                let chunk = decompressor.chunk(&mut reader, &flags).unwrap().unwrap();
                DuplicatesIterator::duplicates(chunk.nums.into_iter())
            } else {
                DuplicatesIterator::no_duplicates(key)
            })
        }
    }

    pub struct CompressedStorageBuilder {
        bit_writer: BitWriter,
        compressor: Compressor<u64>,
        header_offset: usize,
        chunk_offsets: Vec<usize>,
    }

    impl CompressedStorageBuilder {
        pub fn new(config: CompressorConfig) -> Result<Self, QCompressError> {
            let mut bit_writer = BitWriter::default();
            let compressor = Compressor::from_config(config);

            compressor.header(&mut bit_writer)?;
            let header_offset = bit_writer.byte_size();

            Ok(Self {
                bit_writer,
                compressor,
                header_offset,
                chunk_offsets: Vec::new(),
            })
        }
    }

    #[derive(Error, Debug)]
    pub enum CompressedStorageBuilderError {
        #[error("document IDs with the 64th bit set to 1 aren't allowed")]
        DupesTagSet,
        #[error("could not compress document IDs: {0}")]
        CouldNotCompress(#[from] QCompressError),
    }

    impl DocumentIdStorageBuilder for CompressedStorageBuilder {
        type Output = CompressedStorage;
        type Error = CompressedStorageBuilderError;

        fn add_entry(&mut self, doc_ids: Vec<u64>) -> Result<u64, Self::Error> {
            let fst_id = if doc_ids.len() > 1 {
                // check for dupes tag
                if doc_ids.iter().any(|doc_id| doc_id & DUPES_TAG != 0) {
                    return Err(CompressedStorageBuilderError::DupesTagSet);
                }

                let offset = self.bit_writer.byte_size();
                self.compressor
                    .chunk(doc_ids.as_slice(), &mut self.bit_writer)?;
                self.chunk_offsets.push(offset - self.header_offset);

                ((self.chunk_offsets.len() - 1) as u64) | DUPES_TAG
            } else {
                if doc_ids[0] & DUPES_TAG != 0 {
                    return Err(CompressedStorageBuilderError::DupesTagSet);
                }

                doc_ids[0]
            };

            Ok(fst_id)
        }

        fn build(mut self) -> Result<Self::Output, Self::Error> {
            self.compressor.footer(&mut self.bit_writer)?;

            Ok(CompressedStorage {
                chunk_offsets: self.chunk_offsets,
                compressed_indices: self.bit_writer.pop(),
            })
        }
    }
}

pub type DefaultStorageBuilder = VecStorageBuilder;
pub type DefaultStorage = <DefaultStorageBuilder as DocumentIdStorageBuilder>::Output;

#[cfg(feature = "compression")]
pub use compression::CompressedStorage;
