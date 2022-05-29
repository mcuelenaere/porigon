use crate::streams::{DeduplicatedStream, SearchStream};
use crate::Score;
use fst::automaton::{Automaton, Str, Subsequence};
use fst::{IntoStreamer, Map, Streamer};
use itertools::{process_results, Itertools};
use levenshtein_automata::{Distance as LevenshteinDistance, LevenshteinAutomatonBuilder};
use maybe_owned::MaybeOwned;
use q_compress::errors::QCompressError;
use std::collections::HashMap;
use thiserror::Error;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct Duplicates {
    offsets_map: HashMap<u64, usize>,
    compressed_indices: Vec<u8>,
}

pub trait DuplicatesLookup {
    type Iter: Iterator<Item = u64>;

    fn get(&self, key: u64) -> Option<Self::Iter>;
}

impl DuplicatesLookup for Duplicates {
    type Iter = std::vec::IntoIter<u64>;

    fn get(&self, key: u64) -> Option<Self::Iter> {
        self.offsets_map.get(&key).map(|offset| {
            let words = q_compress::BitWords::from(&self.compressed_indices);
            let mut reader = q_compress::BitReader::from(&words);
            let decompressor = q_compress::Decompressor::<u64>::default();
            let flags = decompressor.header(&mut reader).unwrap();
            reader.seek(*offset * 8);
            let chunk = decompressor.chunk(&mut reader, &flags).unwrap().unwrap();
            chunk.nums.into_iter()
        })
    }
}

#[cfg(feature = "rkyv")]
impl DuplicatesLookup for ArchivedDuplicates {
    type Iter = std::vec::IntoIter<u64>;

    fn get(&self, key: u64) -> Option<Self::Iter> {
        self.offsets_map.get(&key).map(|offset| {
            let words = q_compress::BitWords::from(&self.compressed_indices);
            let mut reader = q_compress::BitReader::from(&words);
            let decompressor = q_compress::Decompressor::<u64>::default();
            let flags = decompressor.header(&mut reader).unwrap();
            reader.seek(*offset as usize * 8);
            let chunk = decompressor.chunk(&mut reader, &flags).unwrap().unwrap();
            chunk.nums.into_iter()
        })
    }
}

/// Structure that contains all underlying data needed to construct a `Searchable`.
///
/// NOTE: this struct is serializable
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
pub struct SearchableStorage {
    fst_data: Vec<u8>,
    duplicates: Duplicates,
}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("could not build FST: {0}")]
    BuildFst(#[from] fst::Error),
    #[error("could not compress indices: {0}")]
    CompressIndices(#[from] QCompressError),
}

#[derive(Error, Debug)]
pub enum LoadError {
    #[error("could not read FST: {0}")]
    ReadFst(#[from] fst::Error),
}

#[cfg(feature = "rkyv")]
impl ArchivedSearchableStorage
where
    SearchableStorage: rkyv::Archive,
{
    pub fn to_searchable(&self) -> Result<ArchivedSearchable, LoadError> {
        Ok(ArchivedSearchable {
            map: Map::new(self.fst_data.as_slice())?,
            duplicates: &self.duplicates,
        })
    }
}

impl SearchableStorage {
    pub fn to_searchable(&self) -> Result<Searchable, LoadError> {
        Ok(Searchable {
            map: Map::new(self.fst_data.as_slice())?,
            duplicates: &self.duplicates,
        })
    }

    /// Construct a Searchable from an `Iterator<Item=(&[u8], u64)>`.
    ///
    /// This expects the items to be pre-sorted in lexicographic order. If not, an error will be
    /// returned.
    ///
    /// This method can handle duplicate keys.
    ///
    /// # Example
    /// ```
    /// use porigon::SearchableStorage;
    ///
    /// let searchable = SearchableStorage::build_from_iter(vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    /// )).unwrap();
    /// ```
    pub fn build_from_iter<'a, I>(iter: I) -> Result<Self, BuildError>
    where
        I: IntoIterator<Item = (&'a str, u64)>,
    {
        let mut bit_writer = q_compress::BitWriter::default();
        let compressor = q_compress::Compressor::<u64>::from_config(q_compress::CompressorConfig {
            delta_encoding_order: 1,
            compression_level: 6,
        });
        compressor.header(&mut bit_writer)?;
        let header_offset = bit_writer.byte_size();

        // group items by key and build map
        let mut offsets_map = HashMap::new();
        let iter = iter.into_iter().group_by(|(key, _)| *key);
        let iter = iter.into_iter().map(|(key, mut group)| {
            let (_, first) = group.next().unwrap();
            if let Some((_, second)) = group.next() {
                let mut indices = vec![first, second];
                indices.extend(group.map(|(_, next)| next));
                indices.sort();

                let offset = bit_writer.byte_size();
                compressor.chunk(indices.as_slice(), &mut bit_writer)?;
                offsets_map.insert(first, offset - header_offset);
            }

            Ok::<_, QCompressError>((key, first))
        });
        let map = process_results(iter, |iter| Map::from_iter(iter))??;

        compressor.footer(&mut bit_writer)?;

        Ok(Self {
            fst_data: map.into_fst().into_inner(),
            duplicates: Duplicates {
                offsets_map,
                compressed_indices: bit_writer.bytes(),
            },
        })
    }
}

/// Main entry point to querying FSTs.
///
/// This is a thin wrapper around `fst::Map`, providing easy access to querying it in
/// various ways (exact_match, starts_with, levenshtein, ...).
pub struct SearchableInner<'a, D: DuplicatesLookup> {
    map: Map<&'a [u8]>,
    duplicates: &'a D,
}

impl<'s, D: DuplicatesLookup> SearchableInner<'s, D> {
    fn create_stream<'a, A: 'a>(&'a self, automaton: A) -> impl SearchStream + 'a
    where
        A: Automaton,
    {
        struct Adapter<'m, A>(fst::map::Stream<'m, A>)
        where
            A: Automaton;

        impl<'m, A> SearchStream for Adapter<'m, A>
        where
            A: Automaton,
        {
            fn next(&mut self) -> Option<(&str, u64, Score)> {
                self.0.next().map(|(key, index)| {
                    // SAFETY: we have built this FST containing only valid strings, so this should
                    // always hold.
                    let key = unsafe { std::str::from_utf8_unchecked(key) };
                    (key, index, 0)
                })
            }
        }

        let stream = self.map.search(automaton).into_stream();
        DeduplicatedStream::new(Adapter(stream), self.duplicates)
    }

    /// Creates a `SearchStream` from a `StartsWith` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    ///     ("foo_bar", 3)
    /// );
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    ///
    /// let mut strm = searchable.starts_with("foo");
    /// assert_eq!(strm.next(), Some(("foo", 2, 0)));
    /// assert_eq!(strm.next(), Some(("foo_bar", 3, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn starts_with<'a>(&'a self, query: &'a str) -> impl SearchStream + 'a {
        let automaton = Str::new(query).starts_with();
        self.create_stream(automaton)
    }

    /// Creates a `SearchStream` from an `ExactMatch` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    ///     ("foo_bar", 3)
    /// );
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    ///
    /// let mut strm = searchable.exact_match("foo");
    /// assert_eq!(strm.next(), Some(("foo", 2, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn exact_match<'a>(&'a self, query: &'a str) -> impl SearchStream + 'a {
        let automaton = Str::new(query);
        self.create_stream(automaton)
    }

    /// Creates a `SearchStream` from a `levenshtein_automata::DFA` matcher.
    ///
    /// This method supports both moving the DFA or passing a reference to it.
    ///
    /// # Example
    /// ```
    /// use porigon::levenshtein::LevenshteinAutomatonBuilder;
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("fob", 2),
    ///     ("foo", 3),
    ///     ("foo_bar", 4)
    /// );
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let levenshtein_builder = LevenshteinAutomatonBuilder::new(1, false);
    ///
    /// let dfa = levenshtein_builder.build_dfa("foo");
    /// let mut strm = searchable.levenshtein(&dfa);
    /// assert_eq!(strm.next(), Some(("fob", 2, 0)));
    /// assert_eq!(strm.next(), Some(("foo", 3, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn levenshtein<'a, DFA>(&'a self, dfa: DFA) -> impl SearchStream + 'a
    where
        DFA: Into<MaybeOwned<'a, levenshtein_automata::DFA>>,
    {
        struct Adapter<'a>(MaybeOwned<'a, levenshtein_automata::DFA>);

        impl<'a> fst::Automaton for Adapter<'a> {
            type State = u32;

            fn start(&self) -> u32 {
                self.0.initial_state()
            }

            fn is_match(&self, state: &u32) -> bool {
                match self.0.distance(*state) {
                    LevenshteinDistance::Exact(_) => true,
                    LevenshteinDistance::AtLeast(_) => false,
                }
            }

            fn can_match(&self, state: &u32) -> bool {
                *state != levenshtein_automata::SINK_STATE
            }

            fn accept(&self, state: &u32, byte: u8) -> u32 {
                self.0.transition(*state, byte)
            }
        }

        self.create_stream(Adapter(dfa.into()))
    }

    /// Creates a `SearchStream` for a `LevenshteinAutomatonBuilder` and the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::levenshtein::LevenshteinAutomatonBuilder;
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("fob", 2),
    ///     ("foo", 3),
    ///     ("foo_bar", 4)
    /// );
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let levenshtein_builder = LevenshteinAutomatonBuilder::new(1, false);
    ///
    /// let dfa = levenshtein_builder.build_dfa("foo");
    /// let mut strm = searchable.levenshtein_exact_match(&levenshtein_builder, "foo");
    /// assert_eq!(strm.next(), Some(("fob", 2, 0)));
    /// assert_eq!(strm.next(), Some(("foo", 3, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn levenshtein_exact_match<'a>(
        &'a self,
        builder: &LevenshteinAutomatonBuilder,
        query: &'a str,
    ) -> impl SearchStream + 'a {
        self.levenshtein(builder.build_dfa(query))
    }

    /// Creates a `SearchStream` for a `LevenshteinAutomatonBuilder` and the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::levenshtein::LevenshteinAutomatonBuilder;
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("fob", 2),
    ///     ("foo", 3),
    ///     ("foo_bar", 4)
    /// );
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let levenshtein_builder = LevenshteinAutomatonBuilder::new(1, false);
    ///
    /// let dfa = levenshtein_builder.build_dfa("foo");
    /// let mut strm = searchable.levenshtein_starts_with(&levenshtein_builder, "foo");
    /// assert_eq!(strm.next(), Some(("fob", 2, 0)));
    /// assert_eq!(strm.next(), Some(("foo", 3, 0)));
    /// assert_eq!(strm.next(), Some(("foo_bar", 4, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn levenshtein_starts_with<'a>(
        &'a self,
        builder: &LevenshteinAutomatonBuilder,
        query: &'a str,
    ) -> impl SearchStream + 'a {
        self.levenshtein(builder.build_prefix_dfa(query))
    }

    /// Creates a `SearchStream` from a `Subsequence` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableStorage, SearchStream};
    ///
    /// let items = vec!(("bar_foo", 2), ("foo", 0), ("foo_bar", 1));
    /// let storage = SearchableStorage::build_from_iter(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    ///
    /// let mut strm = searchable.subsequence("fb");
    /// assert_eq!(strm.next(), Some(("foo_bar", 1, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn subsequence<'a>(&'a self, query: &'a str) -> impl SearchStream + 'a {
        let automaton = Subsequence::new(query);
        self.create_stream(automaton)
    }
}

pub type Searchable<'a> = SearchableInner<'a, Duplicates>;
#[cfg(feature = "rkyv")]
pub type ArchivedSearchable<'a> = SearchableInner<'a, ArchivedDuplicates>;

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    trait SearchStreamIntoVec {
        fn into_vec(self) -> Vec<(String, u64, Score)>;
    }

    impl<S: SearchStream> SearchStreamIntoVec for S {
        fn into_vec(mut self) -> Vec<(String, u64, Score)> {
            let mut items = Vec::new();
            while let Some((key, index, score)) = self.next() {
                items.push((key.to_string(), index, score));
            }
            items
        }
    }

    #[test]
    fn test_build() -> TestResult {
        let items = vec![("bar", 1), ("foo", 0)];
        let storage = SearchableStorage::build_from_iter(items)?;
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

    #[test]
    fn test_searchable_exact_match() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.exact_match("bar").into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.exact_match("foo").into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0)));

        Ok(())
    }

    #[test]
    fn test_searchable_starts_with() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.starts_with("b").into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.starts_with("foo").into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("foo".to_string(), 0, 0), ("foobar".to_string(), 2, 0))
        );

        Ok(())
    }

    #[test]
    fn test_searchable_subsequence() -> TestResult {
        let items = vec![("bar_foo", 2), ("foo", 0), ("foo_bar", 1)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.subsequence("m").into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.subsequence("fb").into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo_bar".to_string(), 1, 0)));

        // other positive match
        let results = searchable.subsequence("bf").into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("bar_foo".to_string(), 2, 0)));

        Ok(())
    }

    #[test]
    fn test_scored_stream() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // use key
        let results = searchable
            .starts_with("foo")
            .rescore(|key, _, _| key.len() as u64)
            .into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("foo".to_string(), 0, 3), ("foobar".to_string(), 2, 6))
        );

        // use index
        let results = searchable
            .starts_with("foo")
            .rescore(|_, idx, _| (idx * 2) as u64)
            .into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("foo".to_string(), 0, 0), ("foobar".to_string(), 2, 4))
        );

        Ok(())
    }

    #[test]
    fn test_filter() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable
            .starts_with("foo")
            .filter(|key, _, _| key != "foobar")
            .into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0)));

        let results = searchable
            .starts_with("foo")
            .filter(|key, _, _| key == "foobar")
            .into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foobar".to_string(), 2, 0)));

        Ok(())
    }

    #[test]
    fn test_map() -> TestResult {
        let items = vec![("fo", 1), ("foo", 2), ("foobar", 3)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable
            .starts_with("foo")
            .map(|key, index, score| (key, index * 2, score))
            .into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("foo".to_string(), 4, 0), ("foobar".to_string(), 6, 0))
        );

        Ok(())
    }

    #[test]
    fn test_duplicates() -> TestResult {
        let items = vec![("foo", 0), ("foo", 1), ("foobar", 2)];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable.exact_match("foo").into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("foo".to_string(), 0, 0), ("foo".to_string(), 1, 0))
        );

        Ok(())
    }

    #[test]
    fn test_levenshtein() -> TestResult {
        let items = vec![
            ("bar", 0),
            ("baz", 1),
            ("boz", 2),
            ("coz", 3),
            ("fob", 4),
            ("foo", 5),
            ("foobar", 6),
            ("something else", 7),
        ];
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;
        let dfa_builder_1 = LevenshteinAutomatonBuilder::new(1, false);
        let dfa_builder_2 = LevenshteinAutomatonBuilder::new(2, false);

        let results = searchable
            .levenshtein_exact_match(&dfa_builder_1, "foo")
            .into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results,
            vec!(("fob".to_string(), 4, 0), ("foo".to_string(), 5, 0))
        );

        let results = searchable
            .levenshtein_starts_with(&dfa_builder_1, "foo")
            .into_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(
            results,
            vec!(
                ("fob".to_string(), 4, 0),
                ("foo".to_string(), 5, 0),
                ("foobar".to_string(), 6, 0)
            )
        );

        let results = searchable
            .levenshtein_exact_match(&dfa_builder_2, "bar")
            .into_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(
            results,
            vec!(
                ("bar".to_string(), 0, 0),
                ("baz".to_string(), 1, 0),
                ("boz".to_string(), 2, 0)
            )
        );

        let dfa = dfa_builder_2.build_prefix_dfa("bar");
        let results = searchable
            .levenshtein(&dfa)
            .rescore(|key, _, _| match dfa.eval(key) {
                LevenshteinDistance::Exact(d) => d as u64,
                LevenshteinDistance::AtLeast(_) => 0,
            })
            .into_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(
            results,
            vec!(
                ("bar".to_string(), 0, 0),
                ("baz".to_string(), 1, 1),
                ("boz".to_string(), 2, 2)
            )
        );

        Ok(())
    }
}
