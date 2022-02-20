use crate::streams::SearchStream;
use crate::text::normalizer::TextNormalizer;
use crate::Score;
use fst::automaton::Automaton;
use fst::{IntoStreamer, Map, Streamer};

pub mod builder;
pub mod storage;

use crate::doc_id_storage::DocumentIdStorage;
pub use builder::Builder as SearchableBuilder;
pub use levenshtein_automata::{Distance as LevenshteinDistance, LevenshteinAutomatonBuilder};
pub use storage::Storage as SearchableStorage;

struct SearchableStream<'a, S, D>
where
    S: 'a + for<'s> Streamer<'s, Item = (&'s [u8], u64)>,
    D: DocumentIdStorage<'a>,
{
    cur_key: String,
    cur_iter: Option<D::Iter>,
    doc_ids: &'a D,
    streamer: S,
}

impl<'a, S, D> SearchableStream<'a, S, D>
where
    S: 'a + for<'s> Streamer<'s, Item = (&'s [u8], u64)>,
    D: DocumentIdStorage<'a>,
{
    fn new(streamer: S, doc_ids: &'a D) -> Self {
        Self {
            cur_key: String::new(),
            cur_iter: None,
            doc_ids,
            streamer,
        }
    }
}

impl<'a, S, D> SearchStream for SearchableStream<'a, S, D>
where
    S: 'a + for<'s> Streamer<'s, Item = (&'s [u8], u64)>,
    D: DocumentIdStorage<'a>,
{
    fn next(&mut self) -> Option<(&str, u64, Score)> {
        loop {
            match &mut self.cur_iter {
                Some(iter) => match iter.next() {
                    Some(index) => {
                        return Some((self.cur_key.as_str(), index, 0));
                    }
                    None => {
                        self.cur_key.clear();
                        self.cur_iter = None;
                    }
                },
                None => match self.streamer.next() {
                    Some((key, fst_id)) => {
                        // SAFETY: we control what is being used to build this FST, so we know that
                        // all the bytes are valid UTF-8 strings.
                        let key = unsafe { std::str::from_utf8_unchecked(key) };

                        self.cur_key.clear();
                        self.cur_key.push_str(key);
                        self.cur_iter = Some(
                            self.doc_ids
                                .get(fst_id)
                                .expect("could not find doc_ids for given FST id"),
                        );
                    }
                    None => return None,
                },
            }
        }
    }
}

struct LevenshteinAutomaton(levenshtein_automata::DFA);

impl Automaton for LevenshteinAutomaton {
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

struct StringAutomaton<S: AsRef<str>>(S);

// this is a copy-paste of fst::automaton::Str
impl<S: AsRef<str>> Automaton for StringAutomaton<S> {
    type State = Option<usize>;

    #[inline]
    fn start(&self) -> Option<usize> {
        Some(0)
    }

    #[inline]
    fn is_match(&self, pos: &Option<usize>) -> bool {
        *pos == Some(self.0.as_ref().len())
    }

    #[inline]
    fn can_match(&self, pos: &Option<usize>) -> bool {
        pos.is_some()
    }

    #[inline]
    fn accept(&self, pos: &Option<usize>, byte: u8) -> Option<usize> {
        if let Some(pos) = *pos {
            if self.0.as_ref().as_bytes().get(pos).cloned() == Some(byte) {
                return Some(pos + 1);
            }
        }
        None
    }
}

/// Main entry point to querying FSTs.
///
/// This is a thin wrapper around `fst::Map`, providing easy access to querying it in
/// various ways (exact_match, starts_with, levenshtein, ...).
pub struct Searchable<'s, D, TN>
where
    D: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer,
{
    map: Map<&'s [u8]>,
    doc_ids: &'s D,
    text_normalizer: TN,
}

impl<'s, D, TN> Searchable<'s, D, TN>
where
    D: for<'a> DocumentIdStorage<'a>,
    TN: TextNormalizer,
{
    fn create_stream<'a, A, F>(
        &'a self,
        query: &str,
        automaton_factory: F,
    ) -> impl SearchStream + 'a
    where
        A: Automaton + 'a,
        F: FnOnce(String) -> A,
    {
        let query = self.text_normalizer.normalize(query);
        let automaton = automaton_factory(query);
        let stream = self.map.search(automaton).into_stream();
        SearchableStream::new(stream, self.doc_ids)
    }

    /// Creates a `SearchStream` from a `StartsWith` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    ///     ("foo_bar", 3)
    /// );
    /// let storage = SearchableBuilder::default().build(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    ///
    /// let mut strm = searchable.starts_with("foo");
    /// assert_eq!(strm.next(), Some(("foo", 2, 0)));
    /// assert_eq!(strm.next(), Some(("foo_bar", 3, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn starts_with(&self, query: &str) -> impl SearchStream + '_ {
        self.create_stream(query, |query| StringAutomaton(query).starts_with())
    }

    /// Creates a `SearchStream` from an `ExactMatch` matcher for the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("foo", 2),
    ///     ("foo_bar", 3)
    /// );
    /// let storage = SearchableBuilder::default().build(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    ///
    /// let mut strm = searchable.exact_match("foo");
    /// assert_eq!(strm.next(), Some(("foo", 2, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    pub fn exact_match(&self, query: &str) -> impl SearchStream + '_ {
        self.create_stream(query, |query| StringAutomaton(query))
    }

    /// Creates a `SearchStream` for a `LevenshteinAutomatonBuilder` and the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::levenshtein::LevenshteinAutomatonBuilder;
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("fob", 2),
    ///     ("foo", 3),
    ///     ("foo_bar", 4)
    /// );
    /// let storage = SearchableBuilder::default().build(items).unwrap();
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
    pub fn levenshtein_exact_match<'a, 'b: 'a>(
        &'a self,
        builder: &'b LevenshteinAutomatonBuilder,
        query: &str,
    ) -> impl SearchStream + '_ {
        self.create_stream(query, |query| {
            LevenshteinAutomaton(builder.build_dfa(query.as_ref()))
        })
    }

    /// Creates a `SearchStream` for a `LevenshteinAutomatonBuilder` and the given `query`.
    ///
    /// # Example
    /// ```
    /// use porigon::levenshtein::LevenshteinAutomatonBuilder;
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let items = vec!(
    ///     ("bar", 1),
    ///     ("fob", 2),
    ///     ("foo", 3),
    ///     ("foo_bar", 4)
    /// );
    /// let storage = SearchableBuilder::default().build(items).unwrap();
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
    pub fn levenshtein_starts_with<'a, 'b: 'a>(
        &'a self,
        builder: &'b LevenshteinAutomatonBuilder,
        query: &str,
    ) -> impl SearchStream + '_ {
        self.create_stream(query, |query| {
            LevenshteinAutomaton(builder.build_prefix_dfa(query.as_ref()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::searchable::builder::Builder;

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
    fn test_searchable_exact_match() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = Builder::default().build(items)?;
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
        let storage = Builder::default().build(items)?;
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
    fn test_scored_stream() -> TestResult {
        let items = vec![("fo", 1), ("foo", 0), ("foobar", 2)];
        let storage = Builder::default().build(items)?;
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
        let storage = Builder::default().build(items)?;
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
        let storage = Builder::default().build(items)?;
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
        let storage = Builder::default().build(items)?;
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
        let storage = Builder::default().build(items)?;
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

        Ok(())
    }
}
