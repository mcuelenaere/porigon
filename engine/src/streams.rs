use fst::Streamer;
use itertools::Itertools;
use std::collections::HashMap;

pub type ScorerFn<'a> = dyn Fn(&[u8], u64) -> crate::Score + 'a;
pub struct ScoredStream<'a, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>
{
    scorer: &'a ScorerFn<'a>,
    wrapped: S,
}

impl<'a, S> ScoredStream<'a, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>
{
    pub fn new(streamer: S, scorer: &'a ScorerFn<'a>) -> Self {
        Self {
            scorer,
            wrapped: streamer,
        }
    }
}

impl<'a, 'b, S> Streamer<'a> for ScoredStream<'b, S>
    where S: for<'c> Streamer<'c, Item=(&'c [u8], u64, crate::Score)>
{
    type Item = (&'a [u8], u64, crate::Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        let scorer_fn = &self.scorer;
        self.wrapped.next().map(|(key, index, _)| (key, index, scorer_fn(key, index)))
    }
}

const DUPES_TAG: u64 = (1 << 63);

pub struct DeduplicatedStream<'a, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>
{
    cur_key: Vec<u8>,
    cur_iter: Option<std::slice::Iter<'a, u64>>,
    cur_score: crate::Score,
    duplicates: &'a HashMap<u64, Vec<u64>>,
    wrapped: S,
}

impl<'a, S> DeduplicatedStream<'a, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>
{
    pub fn new(streamer: S, duplicates: &'a HashMap<u64, Vec<u64>>) -> Self {
        Self {
            cur_key: Vec::new(),
            cur_iter: None,
            cur_score: 0,
            duplicates,
            wrapped: streamer,
        }
    }
}

impl<'a, 'b, S> Streamer<'a> for DeduplicatedStream<'b, S>
    where S: for<'c> Streamer<'c, Item=(&'c [u8], u64, crate::Score)>
{
    type Item = (&'a [u8], u64, crate::Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.cur_iter {
            match iter.next() {
                Some(index) => return Some((self.cur_key.as_slice(), *index, self.cur_score)),
                None => {
                    self.cur_iter = None;
                    self.cur_key.clear();
                    self.cur_score = 0;
                }
            }
        }

        match self.wrapped.next() {
            Some((key, index, score)) => {
                if index & DUPES_TAG != 0 {
                    let dupes = match self.duplicates.get(&(index ^ DUPES_TAG)) {
                        Some(x) => x,
                        None => return None,
                    };

                    let mut iter = dupes.iter();
                    match iter.next() {
                        Some(index) => {
                            self.cur_key.clear();
                            self.cur_key.extend_from_slice(key);
                            self.cur_iter = Some(iter);
                            self.cur_score = score;
                            Some((key, *index, score))
                        }
                        None => None,
                    }
                } else {
                    Some((key, index, score))
                }
            },
            None => None,
        }
    }
}

pub fn dedupe_from_iter<'a, I>(iter: I) -> (Vec<(&'a [u8], u64)>, HashMap<u64, Vec<u64>>)
    where I: IntoIterator<Item=(&'a [u8], u64)>,
{
    let mut duplicates = HashMap::new();
    let mut counter: u64 = 1;
    let deduped_iter = iter.into_iter()
        .group_by(|(key, _)| *key)
        .into_iter()
        .map(|(key, mut group)| {
            let (_, first) = group.next().unwrap();
            if let Some((_, second)) = group.next() {
                let mut indices = vec!(first, second);
                while let Some((_, next)) = group.next() {
                    indices.push(next);
                }
                duplicates.insert(counter, indices);
                let dup_index = counter | DUPES_TAG;
                counter += 1;
                (key, dup_index)
            } else {
                (key, first)
            }
        })
        .collect(); // TODO: should return an iterator instead of collecting here
    (deduped_iter, duplicates)
}