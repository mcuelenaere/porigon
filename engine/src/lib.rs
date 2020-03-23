use fst::{IntoStreamer, Streamer, Map};
use fst::automaton::{Automaton, Str, Subsequence};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

pub use self::collectors::TopScoreCollector;

mod collectors;
mod serialization;
mod streams;

type Score = usize;

type BoxedStream<'f> = Box<dyn for<'a> Streamer<'a, Item = (&'a [u8], u64, Score)> + 'f>;
pub struct SearchStream<'s>(BoxedStream<'s>);

impl<'s> SearchStream<'s> {
    pub fn rescore(self, func: &'s streams::ScorerFn<'s>) -> Self {
        SearchStream(Box::new(streams::ScoredStream::new(self, func)))
    }
}

impl<'a, 's> Streamer<'a> for SearchStream<'s> {
    type Item = (&'a [u8], u64, Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Searchable {
    map: serialization::SerializableMap,
    duplicates: HashMap<u64, Vec<u64>>,
}

impl Searchable {
    fn create_stream<'a, A: 'a>(&'a self, automaton: A) -> SearchStream<'a>
        where A: Automaton
    {
        struct Adapter<'m, A>(fst::map::Stream<'m, A>)
            where A: Automaton;

        impl<'a, 'm, A> Streamer<'a> for Adapter<'m, A>
            where A: Automaton
        {
            type Item = (&'a [u8], u64, Score);

            fn next(&'a mut self) -> Option<Self::Item> {
                self.0.next().map(|(key, index)| (key, index, 0))
            }
        }

        let stream = self.map.as_ref().search(automaton).into_stream();
        let deduped_stream = streams::DeduplicatedStream::new(Adapter(stream), &self.duplicates);
        SearchStream(Box::new(deduped_stream))
    }

    pub fn starts_with<'a>(&'a self, query: &'a str) -> SearchStream<'a> {
        let automaton = Str::new(query).starts_with();
        self.create_stream(automaton)
    }

    pub fn exact_match<'a>(&'a self, query: &'a str) -> SearchStream<'a> {
        let automaton = Str::new(query);
        self.create_stream(automaton)
    }

    /*pub fn fuzzy<'a>(&'a self, query: &'a str, distance: u32) -> Result<SearchStream<'a, fst::automaton::Levenshtein>, fst::automaton::LevenshteinError> {
        let automaton = fst::automaton::Levenshtein::new(query, distance)?;
        Ok(self.create_stream(automaton))
    }*/

    pub fn subsequence<'a>(&'a self, query: &'a str) -> SearchStream<'a> {
        let automaton = Subsequence::new(query);
        self.create_stream(automaton)
    }

    pub fn build_from_iter<'a, I>(iter: I) -> Result<Searchable, fst::Error> where I: IntoIterator<Item=(&'a [u8], u64)> {
        // group items by key
        let (deduped_iter, duplicates) = streams::dedupe_from_iter(iter);

        // build map
        let map = Map::from_iter(deduped_iter)?;

        Ok(Self {
            map: map.into(),
            duplicates,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    impl<'a> SearchStream<'a>
    {
        fn into_vec(mut self) -> Vec<(String, u64, Score)> {
            let mut items = Vec::new();
            while let Some((key, index, score)) = self.next() {
                items.push((String::from_utf8(key.to_vec()).unwrap(), index, score));
            }
            items
        }
    }

    #[test]
    fn test_build() -> TestResult {
        let items = vec!(("bar".as_bytes(), 1), ("foo".as_bytes(), 0));
        let searchable = Searchable::build_from_iter(items)?;
        let results = searchable.map.as_ref().stream().into_str_vec()?;
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec![("bar".to_string(), 1), ("foo".to_string(), 0)]);
        Ok(())
    }

    #[test]
    fn test_searchable_exact_match() -> TestResult {
        let items = vec!(("fo".as_bytes(), 1), ("foo".as_bytes(), 0), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

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
        let items = vec!(("fo".as_bytes(), 1), ("foo".as_bytes(), 0), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // negative match
        let results = searchable.starts_with("b").into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.starts_with("foo").into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0), ("foobar".to_string(), 2, 0)));

        Ok(())
    }

    #[test]
    fn test_searchable_subsequence() -> TestResult {
        let items = vec!(("bar_foo".as_bytes(), 2), ("foo".as_bytes(), 0), ("foo_bar".as_bytes(), 1));
        let searchable = Searchable::build_from_iter(items)?;

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
        let items = vec!(("fo".as_bytes(), 1), ("foo".as_bytes(), 0), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // use key
        let results = searchable.starts_with("foo").rescore(&|key, _| key.len()).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 3), ("foobar".to_string(), 2, 6)));

        // use index
        let results = searchable.starts_with("foo").rescore(&|_, idx| (idx * 2) as usize).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0), ("foobar".to_string(), 2, 4)));

        Ok(())
    }

    #[test]
    fn test_duplicates() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("foo".as_bytes(), 1), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        let results = searchable.exact_match("foo").into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0), ("foo".to_string(), 1, 0)));

        Ok(())
    }
}