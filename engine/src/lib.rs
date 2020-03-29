use fst::{IntoStreamer, Streamer, Map};
use fst::automaton::{Automaton, Str, Subsequence};
pub use levenshtein_automata::LevenshteinAutomatonBuilder;
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
    pub fn rescore<F: 's + Fn(&[u8], u64, crate::Score) -> crate::Score>(self, func: F) -> Self {
        SearchStream(Box::new(streams::ScoredStream::new(self, func)))
    }

    pub fn filter<F: 's + Fn(&[u8], u64, crate::Score) -> bool>(self, func: F) -> Self {
        SearchStream(Box::new(streams::FilteredStream::new(self, func)))
    }

    pub fn map<F: 's + Fn(&[u8], u64, crate::Score) -> (&[u8], u64, crate::Score)>(self, func: F) -> Self {
        SearchStream(Box::new(streams::MappedStream::new(self, func)))
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

    fn levenshtein<'a>(&'a self, builder: &LevenshteinAutomatonBuilder, is_prefix: bool, query: &'a str) -> SearchStream<'a> {
        let dfa = if is_prefix { builder.build_prefix_dfa(query) } else { builder.build_dfa(query) };

        struct Adapter(levenshtein_automata::DFA);

        impl fst::Automaton for Adapter {
            type State = u32;

            fn start(&self) -> u32 {
                self.0.initial_state()
            }

            fn is_match(&self, state: &u32) -> bool {
                match self.0.distance(*state) {
                    levenshtein_automata::Distance::Exact(_) => true,
                    levenshtein_automata::Distance::AtLeast(_) => false,
                }
            }

            fn can_match(&self, state: &u32) -> bool {
                *state != levenshtein_automata::SINK_STATE
            }

            fn accept(&self, state: &u32, byte: u8) -> u32 {
                self.0.transition(*state, byte)
            }
        }

        self.create_stream(Adapter(dfa))
    }

    pub fn levenshtein_exact_match<'a>(&'a self, builder: &LevenshteinAutomatonBuilder, query: &'a str) -> SearchStream<'a> {
        self.levenshtein(builder, false, query)
    }

    pub fn levenshtein_starts_with<'a>(&'a self, builder: &LevenshteinAutomatonBuilder, query: &'a str) -> SearchStream<'a> {
        self.levenshtein(builder, true, query)
    }

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
        let results = searchable.starts_with("foo").rescore(|key, _, _| key.len()).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 3), ("foobar".to_string(), 2, 6)));

        // use index
        let results = searchable.starts_with("foo").rescore(|_, idx, _| (idx * 2) as usize).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0), ("foobar".to_string(), 2, 4)));

        Ok(())
    }

    #[test]
    fn test_filter() -> TestResult {
        let items = vec!(("fo".as_bytes(), 1), ("foo".as_bytes(), 0), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        let results = searchable
            .starts_with("foo")
            .filter(|key, _, _| key != "foobar".as_bytes())
            .into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foo".to_string(), 0, 0)));

        let results = searchable
            .starts_with("foo")
            .filter(|key, _, _| key == "foobar".as_bytes())
            .into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!(("foobar".to_string(), 2, 0)));

        Ok(())
    }

    #[test]
    fn test_map() -> TestResult {
        let items = vec!(("fo".as_bytes(), 1), ("foo".as_bytes(), 2), ("foobar".as_bytes(), 3));
        let searchable = Searchable::build_from_iter(items)?;

        let results = searchable
            .starts_with("foo")
            .map(|key, index, score| (key, index * 2, score))
            .into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("foo".to_string(), 4, 0), ("foobar".to_string(), 6, 0)));

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

    #[test]
    fn test_levenshtein() -> TestResult {
        let items = vec!(
            ("bar".as_bytes(), 0),
            ("baz".as_bytes(), 1),
            ("boz".as_bytes(), 2),
            ("coz".as_bytes(), 3),
            ("fob".as_bytes(), 4),
            ("foo".as_bytes(), 5),
            ("foobar".as_bytes(), 6),
            ("something else".as_bytes(), 7),
        );
        let searchable = Searchable::build_from_iter(items)?;
        let dfa_builder_1 = LevenshteinAutomatonBuilder::new(1, false);
        let dfa_builder_2 = LevenshteinAutomatonBuilder::new(2, false);

        let results = searchable.levenshtein_exact_match(&dfa_builder_1, "foo").into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!(("fob".to_string(), 4, 0), ("foo".to_string(), 5, 0)));

        let results = searchable.levenshtein_starts_with(&dfa_builder_1, "foo").into_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results, vec!(("fob".to_string(), 4, 0), ("foo".to_string(), 5, 0), ("foobar".to_string(), 6, 0)));

        let results = searchable.levenshtein_exact_match(&dfa_builder_2, "bar").into_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results, vec!(("bar".to_string(), 0, 0), ("baz".to_string(), 1, 0), ("boz".to_string(), 2, 0)));

        Ok(())
    }
}