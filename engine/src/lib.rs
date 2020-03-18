use itertools::Itertools;
use fst::{IntoStreamer, Streamer, Map};
use fst::automaton::{Automaton, Str, Subsequence, StartsWith};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

pub use self::collectors::TopScoreCollector;

mod collectors;
mod serialization;

type ScorerFn<'a> = dyn Fn(&[u8], u64) -> collectors::DocScore + 'a;
pub struct ScoredStream<'m, A>(fst::map::Stream<'m, A>, &'m ScorerFn<'m>) where A: fst::Automaton;

impl<'a, 'm, A: fst::Automaton> Streamer<'a> for ScoredStream<'m, A> {
    type Item = (collectors::DocScore, u64);

    fn next(&'a mut self) -> Option<Self::Item> {
        let scorer_fn = &self.1;
        self.0.next().map(|(key, index)| (scorer_fn(key, index), index))
    }
}

const DUPES_TAG: u64 = (1 << 63);

pub struct DeduplicatorStream<'a, S: for<'b> Streamer<'b, Item=(collectors::DocScore, u64)>> {
    cur_iter: Option<(std::slice::Iter<'a, u64>, collectors::DocScore)>,
    duplicates: &'a HashMap<u64, Vec<u64>>,
    wrapped: S,
}

impl<'a, 'b, S: for<'c> Streamer<'c, Item=(collectors::DocScore, u64)>> Streamer<'a> for DeduplicatorStream<'b, S> {
    type Item = (collectors::DocScore, u64);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some((iter, score)) = &mut self.cur_iter {
            match iter.next() {
                Some(index) => return Some((*score, *index)),
                None => self.cur_iter = None
            }
        }

        match self.wrapped.next() {
            Some((score, index)) => {
                if index & DUPES_TAG != 0 {
                    let dupes = match self.duplicates.get(&(index ^ DUPES_TAG)) {
                        Some(x) => x,
                        None => return None,
                    };

                    let mut iter = dupes.iter();
                    match iter.next() {
                        Some(index) => {
                            self.cur_iter = Some((iter, score));
                            Some((score, *index))
                        }
                        None => None,
                    }
                } else {
                    Some((score, index))
                }
            },
            None => None,
        }
    }
}

impl<'a, S: for<'b> Streamer<'b, Item=(collectors::DocScore, u64)>> DeduplicatorStream<'a, S> {
    pub fn new(streamer: S, duplicates: &'a HashMap<u64, Vec<u64>>) -> Self {
        Self {
            cur_iter: None,
            duplicates,
            wrapped: streamer,
        }
    }
}

pub struct SearchStream<'s, A: Automaton> {
    duplicates: &'s HashMap<u64, Vec<u64>>,
    stream: fst::map::Stream<'s, A>,
}

impl<'s, A: Automaton + 's> SearchStream<'s, A> {
    pub fn with_score(self, func: &'s ScorerFn<'s>) -> DeduplicatorStream<'s, ScoredStream<'s, A>> {
        DeduplicatorStream::new(ScoredStream(self.stream, func), self.duplicates)
    }
    pub fn without_score(self) -> DeduplicatorStream<'s, ScoredStream<'s, A>> {
        DeduplicatorStream::new(ScoredStream(self.stream, &|_, _| 0), self.duplicates)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Searchable {
    map: serialization::SerializableMap,
    duplicates: HashMap<u64, Vec<u64>>,
}

impl Searchable {
    fn create_stream<A: fst::automaton::Automaton>(&self, automaton: A) -> SearchStream<A> {
        let stream = self.map.as_ref().search(automaton).into_stream();
        SearchStream {
            duplicates: &self.duplicates,
            stream
        }
    }

    pub fn starts_with<'a>(&'a self, query: &'a str) -> SearchStream<'a, StartsWith<Str>> {
        let automaton = Str::new(query).starts_with();
        self.create_stream(automaton)
    }

    pub fn exact_match<'a>(&'a self, query: &'a str) -> SearchStream<'a, Str> {
        let automaton = Str::new(query);
        self.create_stream(automaton)
    }

    /*pub fn fuzzy<'a>(&'a self, query: &'a str, distance: u32) -> Result<SearchStream<'a, fst::automaton::Levenshtein>, fst::automaton::LevenshteinError> {
        let automaton = fst::automaton::Levenshtein::new(query, distance)?;
        Ok(self.create_stream(automaton))
    }*/

    pub fn subsequence<'a>(&'a self, query: &'a str) -> SearchStream<'a, Subsequence> {
        let automaton = Subsequence::new(query);
        self.create_stream(automaton)
    }

    pub fn build_from_iter<'a, I>(iter: I) -> Result<Searchable, fst::Error> where I: IntoIterator<Item=(&'a [u8], u64)> {
        // sort items
        let mut copy: Vec<(&'a [u8], u64)> = iter.into_iter().collect();
        copy.sort_by_key(|entry| entry.0);

        // group items by key
        let mut duplicates = HashMap::new();
        let mut counter: u64 = 1;
        copy = copy.iter()
            .group_by(|(key, _)| *key)
            .into_iter()
            .map(|(key, mut group)| {
                let (_, first) = group.next().unwrap();
                if let Some((_, second)) = group.next() {
                    let mut indices = vec!(*first, *second);
                    while let Some((_, next)) = group.next() {
                        indices.push(*next);
                    }
                    duplicates.insert(counter, indices);
                    let dup_index = counter | DUPES_TAG;
                    counter += 1;
                    (key, dup_index)
                } else {
                    (key, *first)
                }
            })
            .collect();

        // build map
        let map = Map::from_iter(copy)?;
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

    impl<'a, S: for<'b> Streamer<'b, Item=(collectors::DocScore, u64)>> DeduplicatorStream<'a, S> {
        pub fn into_vec(mut self) -> Vec<(collectors::DocScore, u64)> {
            let mut items = Vec::new();
            while let Some((score, idx)) = self.next() {
                items.push((score, idx));
            }
            items
        }
    }

    #[test]
    fn test_build() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("bar".as_bytes(), 1));
        let searchable = Searchable::build_from_iter(items)?;
        let results = searchable.map.as_ref().stream().into_str_vec()?;
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec![("bar".to_string(), 1), ("foo".to_string(), 0)]);
        Ok(())
    }

    #[test]
    fn test_searchable_exact_match() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("fo".as_bytes(), 1), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // negative match
        let results = searchable.exact_match("bar").without_score().into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.exact_match("foo").without_score().into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!((0, 0)));

        Ok(())
    }

    #[test]
    fn test_searchable_starts_with() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("fo".as_bytes(), 1), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // negative match
        let results = searchable.starts_with("b").without_score().into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.starts_with("foo").without_score().into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!((0, 0), (0, 2)));

        Ok(())
    }

    #[test]
    fn test_searchable_subsequence() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("foo_bar".as_bytes(), 1), ("bar_foo".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // negative match
        let results = searchable.subsequence("m").without_score().into_vec();
        assert_eq!(results.len(), 0);

        // positive match
        let results = searchable.subsequence("fb").without_score().into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!((0, 1)));

        // other positive match
        let results = searchable.subsequence("bf").without_score().into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results, vec!((0, 2)));

        Ok(())
    }

    #[test]
    fn test_scored_stream() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("fo".as_bytes(), 1), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        // use key
        let results = searchable.starts_with("foo").with_score(&|key, _| key.len()).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!((3, 0), (6, 2)));

        // use index
        let results = searchable.starts_with("foo").with_score(&|_, idx| (idx * 2) as usize).into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!((0, 0), (4, 2)));

        Ok(())
    }

    #[test]
    fn test_duplicates() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("foo".as_bytes(), 1), ("foobar".as_bytes(), 2));
        let searchable = Searchable::build_from_iter(items)?;

        let results = searchable.exact_match("foo").without_score().into_vec();
        assert_eq!(results.len(), 2);
        assert_eq!(results, vec!((0, 0), (0, 1)));

        Ok(())
    }
}