use crate::SearchResult;
use fst::{IntoStreamer, Map, Streamer};
use fst::automaton::{Automaton, Str, Subsequence};
use levenshtein_automata::{LevenshteinAutomatonBuilder, Distance as LevenshteinDistance};
use maybe_owned::MaybeOwned;
use std::collections::HashMap;
use itertools::Itertools;

/// Structure that contains all underlying data needed to construct a `Searchable`.
///
/// NOTE: this struct is serializable
#[cfg_attr(feature = "serde_support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "rkyv_support", derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize))]
pub struct SearchableStorage {
    fst_data: Vec<u8>,
    duplicates: HashMap<u64, Vec<u64>>,
}

#[cfg(feature = "rkyv_support")]
impl ArchivedSearchableStorage
    where SearchableStorage: rkyv::Archive
{
    pub fn to_searchable(&self) -> Result<Searchable<rkyv::collections::ArchivedHashMap<u64, rkyv::vec::ArchivedVec<u64>>>, fst::Error> {
        Ok(Searchable {
            map: Map::new(self.fst_data.as_slice())?,
            duplicates: &self.duplicates,
        })
    }
}

impl SearchableStorage {
    pub fn to_searchable(&self) -> Result<Searchable<HashMap<u64, Vec<u64>>>, fst::Error> {
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
    pub fn build_from_iter<'a, I>(iter: I) -> Result<Self, fst::Error> where I: IntoIterator<Item=(&'a str, u64)> {
        // group items by key and build map
        let mut duplicates = HashMap::new();
        let map = Map::from_iter(
            iter
            .into_iter()
            .group_by(|(key, _)| *key)
            .into_iter()
            .map(|(key, mut group)| {
                let (_, first) = group.next().unwrap();
                if let Some((_, second)) = group.next() {
                    let mut indices = vec![first, second];
                    indices.extend(group.map(|(_, next)| next));
                    duplicates.insert(first, indices);
                }

                (key, first)
            })
        )?;

        Ok(Self {
            fst_data: map.into_fst().into_inner(),
            duplicates,
        })
    }
}

pub trait DuplicatesLookup {
    fn get(&self, key: u64) -> Option<&[u64]>;
}

impl DuplicatesLookup for HashMap<u64, Vec<u64>> {
    fn get(&self, key: u64) -> Option<&[u64]> {
        self.get(&key).map(|values| values.as_slice())
    }
}

#[cfg(feature = "rkyv_support")]
impl DuplicatesLookup for rkyv::collections::ArchivedHashMap<u64, rkyv::vec::ArchivedVec<u64>> {
    fn get(&self, key: u64) -> Option<&[u64]> {
        self.get(&key).map(|values| values.as_slice())
    }
}

struct Searcher<'a, D, S> {
    cur_state: Option<((*const u8, usize), std::slice::Iter<'a, u64>)>,
    duplicates: &'a D,
    streamer: S,
}

impl<'a, D, S> Iterator for Searcher<'a, D, S>
    where S: 'a + for<'b> Streamer<'b, Item=(&'b [u8], u64)>,
          D: DuplicatesLookup
{
    type Item = SearchResult<'a>;

    fn next(&mut self) -> Option<SearchResult<'a>> {
        if let Some((key, iter)) = &mut self.cur_state {
            match iter.next() {
                Some(index) => {
                    let key = unsafe { std::slice::from_raw_parts((*key).0, (*key).1) };
                    // SAFETY: we populate the FST with `&str` only, so it is safe to convert this to a `&str`
                    let key = unsafe { std::str::from_utf8_unchecked(key) };
                    return Some((key, *index, 0));
                },
                None => {
                    self.cur_state = None;
                }
            }
        }

        self.streamer.next().map(|(key, index)| {
            let key = (key.as_ptr(), key.len());

            let index = match self.duplicates.get(index) {
                Some(dupes) => {
                    let mut iter = dupes.iter();
                    let index = iter.next().unwrap();
                    self.cur_state = Some((key, iter));

                    *index
                },
                None => index,
            };

            let key = unsafe { std::slice::from_raw_parts(key.0, key.1) };
            // SAFETY: we populate the FST with `&str` only, so it is safe to convert this to a `&str`
            let key = unsafe { std::str::from_utf8_unchecked(key) };
            (key, index, 0)
        })
    }
}

/// Main entry point to querying FSTs.
///
/// This is a thin wrapper around `fst::Map`, providing easy access to querying it in
/// various ways (exact_match, starts_with, levenshtein, ...).
pub struct Searchable<'a, D: DuplicatesLookup> {
    map: Map<&'a [u8]>,
    duplicates: &'a D,
}

impl<'s, D: DuplicatesLookup> Searchable<'s, D> {
    fn create_stream<'a: 's, A: 'a>(&'a self, automaton: A) -> impl Iterator<Item=SearchResult<'s>> + 'a
        where A: Automaton
    {
        Searcher {
            cur_state: None,
            streamer: self.map.search(automaton).into_stream(),
            duplicates: self.duplicates,
        }
    }

    /// Creates a `SearchStream` from a `StartsWith` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use fst::Streamer;
    /// use porigon::SearchableStorage;
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
    pub fn starts_with<'a: 's>(&'a self, query: &'a str) -> impl Iterator<Item=SearchResult<'s>> + 'a {
        let automaton = Str::new(query).starts_with();
        self.create_stream(automaton)
    }

    /// Creates a `SearchStream` from an `ExactMatch` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use fst::Streamer;
    /// use porigon::SearchableStorage;
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
    pub fn exact_match<'a: 's>(&'a self, query: &'a str) -> impl Iterator<Item=SearchResult<'s>> + 'a {
        let automaton = Str::new(query);
        self.create_stream(automaton)
    }

    /// Creates a `SearchStream` from a `levenshtein_automata::DFA` matcher.
    ///
    /// This method supports both moving the DFA or passing a reference to it.
    ///
    /// # Example
    /// ```
    /// use fst::Streamer;
    /// use porigon::{LevenshteinAutomatonBuilder, SearchableStorage};
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
    pub fn levenshtein<'a: 's, DFA>(&'a self, dfa: DFA) -> impl Iterator<Item=SearchResult<'s>> + 'a
        where DFA: Into<MaybeOwned<'a, levenshtein_automata::DFA>>
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
    /// use fst::Streamer;
    /// use porigon::{LevenshteinAutomatonBuilder, SearchableStorage};
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
    pub fn levenshtein_exact_match<'a: 's>(&'a self, builder: &LevenshteinAutomatonBuilder, query: &'a str) -> impl Iterator<Item=SearchResult<'s>> + 'a {
        self.levenshtein(builder.build_dfa(query))
    }

    /// Creates a `SearchStream` for a `LevenshteinAutomatonBuilder` and the given `query`.
    ///
    /// # Example
    /// ```
    /// use fst::Streamer;
    /// use porigon::{LevenshteinAutomatonBuilder, SearchableStorage};
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
    pub fn levenshtein_starts_with<'a: 's>(&'a self, builder: &LevenshteinAutomatonBuilder, query: &'a str) -> impl Iterator<Item=SearchResult<'s>> + 'a {
        self.levenshtein(builder.build_prefix_dfa(query))
    }

    /// Creates a `SearchStream` from a `Subsequence` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use fst::Streamer;
    /// use porigon::SearchableStorage;
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
    pub fn subsequence<'a: 's>(&'a self, query: &'a str) -> impl Iterator<Item=SearchResult<'s>> + 'a {
        let automaton = Subsequence::new(query);
        self.create_stream(automaton)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_build() -> TestResult {
        let items = vec!(("bar", 1), ("foo", 0));
        let storage = SearchableStorage::build_from_iter(items)?;
        let results = storage.to_searchable()?.map.as_ref().stream().into_str_vec()?;
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec![("bar".to_string(), 1), ("foo".to_string(), 0)]);
        Ok(())
    }

    #[test]
    fn test_searchable_exact_match() -> TestResult {
        let items = vec!(("fo", 1), ("foo", 0), ("foobar", 2));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.exact_match("bar").collect_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.exact_match("foo").collect_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo", 0, 0)));

        Ok(())
    }

    #[test]
    fn test_searchable_starts_with() -> TestResult {
        let items = vec!(("fo", 1), ("foo", 0), ("foobar", 2));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.starts_with("b").collect_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.starts_with("foo").collect_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo", 0, 0), ("foobar", 2, 0)));

        Ok(())
    }

    #[test]
    fn test_searchable_subsequence() -> TestResult {
        let items = vec!(("bar_foo", 2), ("foo", 0), ("foo_bar", 1));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        // negative match
        let results = searchable.subsequence("m").collect_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.subsequence("fb").collect_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo_bar", 1, 0)));

        // other positive match
        let results = searchable.subsequence("bf").collect_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("bar_foo", 2, 0)));

        Ok(())
    }

    #[test]
    fn test_filter() -> TestResult {
        let items = vec!(("fo", 1), ("foo", 0), ("foobar", 2));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable
            .starts_with("foo")
            .filter(|(key, _, _)| *key != "foobar")
            .collect_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo", 0, 0)));

        let results = searchable
            .starts_with("foo")
            .filter(|(key, _, _)| *key == "foobar")
            .collect_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foobar", 2, 0)));

        Ok(())
    }

    #[test]
    fn test_map() -> TestResult {
        let items = vec!(("fo", 1), ("foo", 2), ("foobar", 3));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable
            .starts_with("foo")
            .map(|(key, index, score)| (key, index * 2, score))
            .collect_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo", 4, 0), ("foobar", 6, 0)));

        Ok(())
    }

    #[test]
    fn test_duplicates() -> TestResult {
        let items = vec!(("foo", 0), ("foo", 1), ("foobar", 2));
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;

        let results = searchable.exact_match("foo").collect_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo", 0, 0), ("foo", 1, 0)));

        Ok(())
    }

    #[test]
    fn test_levenshtein() -> TestResult {
        let items = vec!(
            ("bar", 0),
            ("baz", 1),
            ("boz", 2),
            ("coz", 3),
            ("fob", 4),
            ("foo", 5),
            ("foobar", 6),
            ("something else", 7),
        );
        let storage = SearchableStorage::build_from_iter(items)?;
        let searchable = storage.to_searchable()?;
        let dfa_builder_1 = LevenshteinAutomatonBuilder::new(1, false);
        let dfa_builder_2 = LevenshteinAutomatonBuilder::new(2, false);

        let results = searchable.levenshtein_exact_match(&dfa_builder_1, "foo").collect_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("fob", 4, 0), ("foo", 5, 0)));

        let results = searchable.levenshtein_starts_with(&dfa_builder_1, "foo").collect_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results, vec!(("fob", 4, 0), ("foo", 5, 0), ("foobar", 6, 0)));

        let results = searchable.levenshtein_exact_match(&dfa_builder_2, "bar").collect_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results, vec!(("bar", 0, 0), ("baz", 1, 0), ("boz", 2, 0)));

        let dfa = dfa_builder_2.build_prefix_dfa("bar");
        let results = searchable
            .levenshtein(&dfa)
            .map(
                |(key, index, _)| {
                    let score = match dfa.eval(key) {
                        LevenshteinDistance::Exact(d) => d as u64,
                        LevenshteinDistance::AtLeast(_) => 0,
                    };
                    (key, index, score)
                }
            )
            .collect_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results, vec!(("bar", 0, 0), ("baz", 1, 1), ("boz", 2, 2)));

        Ok(())
    }
}