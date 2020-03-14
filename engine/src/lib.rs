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

impl<'m, A: Automaton> ScoredStream<'m, A> {
    #[cfg(test)]
    pub fn into_vec(mut self) -> Vec<(collectors::DocScore, u64)> {
        let mut items = vec![];
        while let Some((score, idx)) = self.next() {
            items.push((score, idx));
        }
        items
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

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_build() -> TestResult {
        let items = vec!(("foo".as_bytes(), 0), ("bar".as_bytes(), 1));
        let searchable = Searchable::build_from_iter(items)?;
        let results = searchable.0.as_ref().stream().into_str_vec()?;
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
}