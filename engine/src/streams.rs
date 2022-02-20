use crate::Score;

pub struct FilteredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> bool,
{
    cur_key: String,
    filter: F,
    wrapped: S,
}

impl<F, S> FilteredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> bool,
{
    pub fn new(streamer: S, filter: F) -> Self {
        Self {
            cur_key: String::new(),
            filter,
            wrapped: streamer,
        }
    }
}

impl<F, S> SearchStream for FilteredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> bool,
{
    fn next(&mut self) -> Option<(&str, u64, Score)> {
        let filter_fn = &self.filter;
        while let Some((key, index, score)) = self.wrapped.next() {
            if !filter_fn(key, index, score) {
                continue;
            }

            // borrow checker workaround: we can't seem to pass the key as-is, so we make
            // an (useless) copy and return that instead
            self.cur_key.clear();
            self.cur_key.push_str(key);
            return Some((self.cur_key.as_str(), index, score));
        }

        None
    }
}

pub struct MappedStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> (&str, u64, Score),
{
    mapper: F,
    wrapped: S,
}

impl<F, S> MappedStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> (&str, u64, Score),
{
    pub fn new(streamer: S, mapper: F) -> Self {
        Self {
            mapper,
            wrapped: streamer,
        }
    }
}

impl<F, S> SearchStream for MappedStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> (&str, u64, Score),
{
    fn next(&mut self) -> Option<(&str, u64, Score)> {
        let mapper_fn = &self.mapper;
        self.wrapped
            .next()
            .map(|(key, index, score)| mapper_fn(key, index, score))
    }
}

pub struct RescoredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> Score,
{
    scorer: F,
    wrapped: S,
}

impl<F, S> RescoredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> Score,
{
    pub fn new(streamer: S, scorer: F) -> Self {
        Self {
            scorer,
            wrapped: streamer,
        }
    }
}

impl<F, S> SearchStream for RescoredStream<F, S>
where
    S: SearchStream,
    F: Fn(&str, u64, Score) -> Score,
{
    fn next(&mut self) -> Option<(&str, u64, Score)> {
        let scorer_fn = &self.scorer;
        self.wrapped
            .next()
            .map(|(key, index, score)| (key, index, scorer_fn(key, index, score)))
    }
}

/// FST stream on which various operations can be chained.
pub trait SearchStream {
    /// Emits the next scored document in this stream, or `None` to indicate
    /// the stream has been exhausted.
    ///
    /// It is not specified what a stream does after `None` is emitted. In most
    /// cases, `None` should be emitted on every subsequent call.
    fn next(&mut self) -> Option<(&str, u64, Score)>;

    /// Scores a stream, using the given closure.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let storage = SearchableBuilder::default().build(vec!(
    ///     ("foo", 0),
    ///     ("foobar", 1))
    /// ).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let mut strm = searchable
    ///     .starts_with("foo")
    ///     .rescore(|key, _, _| key.len() as porigon::Score)
    /// ;
    /// assert_eq!(strm.next(), Some(("foo", 0, 3)));
    /// assert_eq!(strm.next(), Some(("foobar", 1, 6)));
    /// assert_eq!(strm.next(), None);
    /// ```
    ///
    /// You can also use this to build upon a previously set score:
    ///
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let storage = SearchableBuilder::default().build(vec!(
    ///     ("foo", 0),
    ///     ("foobar", 1))
    /// ).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let mut strm = searchable
    ///     .starts_with("foo")
    ///     .rescore(|key, _, _| key.len() as porigon::Score)
    ///     .rescore(|_, index, old_score| (old_score << 16) | index)
    /// ;
    /// assert_eq!(strm.next(), Some(("foo", 0, 3 << 16)));
    /// assert_eq!(strm.next(), Some(("foobar", 1, (6 << 16) | 1)));
    /// assert_eq!(strm.next(), None);
    /// ```
    fn rescore<F>(self, func: F) -> RescoredStream<F, Self>
    where
        F: Fn(&str, u64, Score) -> Score,
        Self: Sized,
    {
        RescoredStream::new(self, func)
    }

    /// Filters a stream, using the given closure.
    ///
    /// # Example
    ///
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let storage = SearchableBuilder::default().build(vec!(
    ///     ("foo", 0),
    ///     ("foobar", 1))
    /// ).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let mut strm = searchable
    ///     .starts_with("foo")
    ///     .filter(|key, _, _| key != "foobar")
    /// ;
    /// assert_eq!(strm.next(), Some(("foo", 0, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    fn filter<F>(self, func: F) -> FilteredStream<F, Self>
    where
        F: Fn(&str, u64, Score) -> bool,
        Self: Sized,
    {
        FilteredStream::new(self, func)
    }

    /// Maps over a stream, using the given closure.
    ///
    /// This more of an advanced method, used for changing the stream's key or index. Most probably
    /// you want to use [rescore()](#method.rescore) instead.
    ///
    /// # Example
    ///
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream};
    ///
    /// let mut items = vec!(
    ///     ("this is a bar", 15),
    ///     ("is a bar", (1 << 32) | 15),
    ///     ("a bar", (1 << 32) | 15),
    ///     ("bar", (1 << 32) | 15),
    ///     ("barfoo", 16)
    /// );
    /// items.sort_by_key(|(key, _)| *key);
    /// let storage = SearchableBuilder::default().build(items).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let mut strm = searchable
    ///     .starts_with("bar")
    ///     .map(|key, index, score| (key, index & !(1 << 32), score))
    /// ;
    /// assert_eq!(strm.next(), Some(("bar", 15, 0)));
    /// assert_eq!(strm.next(), Some(("barfoo", 16, 0)));
    /// assert_eq!(strm.next(), None);
    /// ```
    fn map<F>(self, func: F) -> MappedStream<F, Self>
    where
        F: Fn(&str, u64, Score) -> (&str, u64, Score),
        Self: Sized,
    {
        MappedStream::new(self, func)
    }
}
