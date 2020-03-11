use fst::{IntoStreamer, Streamer, Map};
use fst::automaton::{Automaton, Str, Subsequence, StartsWith};
use serde::{Serialize, Deserialize};

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

pub struct SearchStream<'s, A>(fst::map::Stream<'s, A>) where A: fst::automaton::Automaton;

impl<'s, A: fst::automaton::Automaton> SearchStream<'s, A> {
    pub fn with_score(self, func: &'s ScorerFn<'s>) -> ScoredStream<'s, A> {
        ScoredStream(self.0, func)
    }
    pub fn without_score(self) -> ScoredStream<'s, A> {
        ScoredStream(self.0, &|_, _| 0)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Searchable(serialization::SerializableMap);

impl Searchable {
    fn create_stream<A: fst::automaton::Automaton>(&self, automaton: A) -> SearchStream<A> {
        let stream = self.0.as_ref().search(automaton).into_stream();
        SearchStream(stream)
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

        // build map, using sorted keys
        let map = Map::from_iter(copy)?;
        Ok(map.into())
    }
}

impl From<Map<Vec<u8>>> for Searchable {
    fn from(m: Map<Vec<u8>>) -> Self {
        Self(m.into())
    }
}