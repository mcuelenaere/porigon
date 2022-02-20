use crate::doc_id_storage::DocumentIdStorage;
use crate::text::normalizer::TextNormalizer;
use crate::Searchable;
use fst::Map;
use rkyv::{Archive, Archived};
use std::marker::PhantomData;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LoadError {
    #[error("could not read FST: {0}")]
    ReadFst(#[from] fst::Error),
}

/// Structure that contains all underlying data needed to construct a `Searchable`.
///
/// NOTE: this struct is serializable
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct Storage<D, TN>
where
    D: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer,
{
    fst_data: Vec<u8>,
    doc_ids: D,
    _phantom: PhantomData<TN>,
}

impl<D, TN> Storage<D, TN>
where
    D: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer,
{
    pub(crate) fn new(fst_data: Vec<u8>, doc_ids: D) -> Self {
        Self {
            fst_data,
            doc_ids,
            _phantom: PhantomData,
        }
    }
}

impl<D, TN> Storage<D, TN>
where
    D: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer + Default,
{
    pub fn to_searchable(&self) -> Result<Searchable<'_, D, TN>, LoadError> {
        Ok(Searchable {
            map: Map::new(self.fst_data.as_slice())?,
            doc_ids: &self.doc_ids,
            text_normalizer: TN::default(),
        })
    }
}

#[cfg(feature = "rkyv")]
impl<D, TN> ArchivedStorage<D, TN>
where
    D: Archive + for<'a> DocumentIdStorage<'a>,
    Archived<D>: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer + Default,
{
    pub fn to_searchable(&self) -> Result<Searchable<'_, Archived<D>, TN>, LoadError> {
        Ok(Searchable {
            map: Map::new(self.fst_data.as_slice())?,
            doc_ids: &self.doc_ids,
            text_normalizer: TN::default(),
        })
    }
}
