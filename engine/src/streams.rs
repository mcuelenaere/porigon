use fst::Streamer;
use itertools::Itertools;
use std::collections::HashMap;

pub struct FilteredStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> bool,
{
    cur_key: Vec<u8>,
    filter: F,
    wrapped: S,
}

impl<F, S> FilteredStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> bool,
{
    pub fn new(streamer: S, filter: F) -> Self {
        Self {
            cur_key: Vec::new(),
            filter,
            wrapped: streamer,
        }
    }
}

impl<'a, F, S> Streamer<'a> for FilteredStream<F, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> bool,
{
    type Item = (&'a [u8], u64, crate::Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        let filter_fn = &self.filter;
        while let Some((key, index, score)) = self.wrapped.next() {
            if !filter_fn(key, index, score) {
                continue;
            }

            // borrow checker workaround: we can't seem to pass the key as-is, so we make
            // an (useless) copy and return that instead
            self.cur_key.clear();
            self.cur_key.extend_from_slice(key);
            return Some((self.cur_key.as_slice(), index, score));
        }

        None
    }
}

pub struct MappedStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> (&[u8], u64, crate::Score),
{
    mapper: F,
    wrapped: S,
}

impl<F, S> MappedStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> (&[u8], u64, crate::Score),
{
    pub fn new(streamer: S, mapper: F) -> Self {
        Self {
            mapper,
            wrapped: streamer,
        }
    }
}

impl<'a, F, S> Streamer<'a> for MappedStream<F, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> (&[u8], u64, crate::Score),
{
    type Item = (&'a [u8], u64, crate::Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        let mapper_fn = &self.mapper;
        self.wrapped.next().map(|(key, index, score)| mapper_fn(key, index, score))
    }
}

pub struct RescoredStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> crate::Score,
{
    scorer: F,
    wrapped: S,
}

impl<F, S> RescoredStream<F, S>
    where S: for<'a> Streamer<'a, Item=(&'a [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> crate::Score,
{
    pub fn new(streamer: S, scorer: F) -> Self {
        Self {
            scorer,
            wrapped: streamer,
        }
    }
}

impl<'a, F, S> Streamer<'a> for RescoredStream<F, S>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>,
          F: Fn(&[u8], u64, crate::Score) -> crate::Score,
{
    type Item = (&'a [u8], u64, crate::Score);

    fn next(&'a mut self) -> Option<Self::Item> {
        let scorer_fn = &self.scorer;
        self.wrapped.next().map(|(key, index, score)| (key, index, scorer_fn(key, index, score)))
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
impl DuplicatesLookup for rkyv::std_impl::chd::ArchivedHashMap<u64, rkyv::std_impl::ArchivedVec<u64>> {
    fn get(&self, key: u64) -> Option<&[u64]> {
        self.get(&key).map(|values| values.as_slice())
    }
}

const DUPES_TAG: u64 = 1 << 63;

pub struct DeduplicatedStream<'a, S, D>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>,
          D: DuplicatesLookup
{
    cur_key: Vec<u8>,
    cur_iter: Option<std::slice::Iter<'a, u64>>,
    cur_score: crate::Score,
    duplicates: &'a D,
    wrapped: S,
}

impl<'a, S, D> DeduplicatedStream<'a, S, D>
    where S: for<'b> Streamer<'b, Item=(&'b [u8], u64, crate::Score)>,
          D: DuplicatesLookup
{
    pub fn new(streamer: S, duplicates: &'a D) -> Self {
        Self {
            cur_key: Vec::new(),
            cur_iter: None,
            cur_score: 0,
            duplicates,
            wrapped: streamer,
        }
    }
}

impl<'a, 'b, S, D> Streamer<'a> for DeduplicatedStream<'b, S, D>
    where S: for<'c> Streamer<'c, Item=(&'c [u8], u64, crate::Score)>,
          D: DuplicatesLookup
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
                    let dupes = match self.duplicates.get(index ^ DUPES_TAG) {
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