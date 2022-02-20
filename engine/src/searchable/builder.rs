use crate::doc_id_storage;
use crate::doc_id_storage::{DefaultStorageBuilder, DocumentIdStorageBuilder};
use crate::searchable::storage::Storage;
use crate::text::{normalizer::TextNormalizer, DefaultTextNormalizer};
use fst::Map;
use itertools::{process_results, Itertools};
use std::error::Error as StdError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BuildError<D> {
    #[error("could not build FST: {0}: {}", .0.source().unwrap())]
    BuildFst(#[from] fst::Error),
    #[error("could not add entry '{0}' to document ID storage: {1}")]
    AddEntryToDocIdStorage(String, #[source] D),
    #[error("could not build document ID storage: {0}")]
    BuildDocIdStorage(#[source] D),
}

pub struct Builder<D, TN>
where
    D: DocumentIdStorageBuilder,
    TN: TextNormalizer,
{
    doc_id_storage_builder: D,
    text_normalizer: TN,
}

impl Default for Builder<DefaultStorageBuilder, DefaultTextNormalizer> {
    fn default() -> Self {
        Builder {
            doc_id_storage_builder: DefaultStorageBuilder::default(),
            text_normalizer: DefaultTextNormalizer::default(),
        }
    }
}

impl<D, TN> Builder<D, TN>
where
    D: DocumentIdStorageBuilder,
    TN: TextNormalizer,
{
    #[cfg(feature = "compression")]
    pub fn with_document_id_compression(
        self,
        config: doc_id_storage::compression::CompressorConfig,
    ) -> Result<
        Builder<doc_id_storage::compression::CompressedStorageBuilder, TN>,
        q_compress::errors::QCompressError,
    > {
        Ok(Builder {
            doc_id_storage_builder: doc_id_storage::compression::CompressedStorageBuilder::new(
                config,
            )?,
            text_normalizer: self.text_normalizer,
        })
    }

    pub fn with_text_normalizer<T: TextNormalizer>(self, text_normalizer: T) -> Builder<D, T> {
        Builder {
            doc_id_storage_builder: self.doc_id_storage_builder,
            text_normalizer,
        }
    }

    /// Construct a Searchable from an `Iterator<Item=(AsRef<str>, u64)>`.
    ///
    /// This method will normalize the item and sort them. This method can handle duplicate keys.
    ///
    /// # Example
    /// ```
    /// use porigon::SearchableBuilder;
    ///
    /// let searchable = SearchableBuilder::default().build(vec!(
    ///     ("FOO", 2),
    ///     ("Bar", 1),
    /// )).unwrap();
    /// ```
    pub fn build<'a, I, S>(self, entries: I) -> Result<Storage<D::Output, TN>, BuildError<D::Error>>
    where
        S: AsRef<str> + 'a,
        I: IntoIterator<Item = (S, u64)>,
    {
        let entries = entries
            .into_iter()
            .map(|(text, doc_id)| (self.text_normalizer.normalize(text.as_ref()), doc_id))
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect_vec();

        self.build_without_preprocessing(entries.iter().map(|e| (e.0.as_str(), e.1)))
    }

    /// Construct a Searchable from an `Iterator<Item=(AsRef<str>, u64)>`.
    ///
    /// This expects the items to be pre-normalized and pre-sorted in lexicographic order. If not,
    /// an error will be returned.
    ///
    /// This method can handle duplicate keys.
    ///
    /// # Example
    /// ```
    /// use porigon::SearchableBuilder;
    ///
    /// let searchable = SearchableBuilder::default().build_without_preprocessing(vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    /// )).unwrap();
    /// ```
    pub fn build_without_preprocessing<'a, I>(
        mut self,
        entries: I,
    ) -> Result<Storage<D::Output, TN>, BuildError<D::Error>>
    where
        I: IntoIterator<Item = (&'a str, u64)>,
    {
        // group items by normalized text and build FST + doc storage
        let grouped_entries = entries.into_iter().group_by(|(text, _)| *text);
        let iter = grouped_entries.into_iter().map(|(text, group)| {
            let mut doc_ids = group.map(|(_, doc_id)| doc_id).collect_vec();
            doc_ids.sort();

            let fst_id = self
                .doc_id_storage_builder
                .add_entry(doc_ids)
                .map_err(|err| BuildError::AddEntryToDocIdStorage(text.to_string(), err))?;

            Ok::<_, BuildError<D::Error>>((text.as_bytes(), fst_id))
        });
        let map = process_results(iter, |iter| Map::from_iter(iter))??;

        let doc_ids = self
            .doc_id_storage_builder
            .build()
            .map_err(BuildError::BuildDocIdStorage)?;
        Ok(Storage::new(map.into_fst().into_inner(), doc_ids))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc_id_storage::compression::CompressorConfig;

    #[test]
    fn test_build() -> Result<(), Box<dyn std::error::Error>> {
        let items = vec![("bar", 1), ("foo", 0)];
        let storage = Builder::default().build(items)?;
        let results = storage
            .to_searchable()?
            .map
            .as_ref()
            .stream()
            .into_str_vec()?;
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec![("bar".to_string(), 1), ("foo".to_string(), 0)]
        );
        Ok(())
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_build_compression() -> Result<(), Box<dyn std::error::Error>> {
        let items = vec![("bar", 1), ("foo", 0)];
        let storage = Builder::default()
            .with_document_id_compression(CompressorConfig {
                compression_level: 6,
                delta_encoding_order: 1,
            })?
            .build(items)?;
        let results = storage
            .to_searchable()?
            .map
            .as_ref()
            .stream()
            .into_str_vec()?;
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec![("bar".to_string(), 1), ("foo".to_string(), 0)]
        );
        Ok(())
    }
}
