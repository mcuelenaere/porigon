use crate::{Score, SearchStream};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

pub struct Document {
    pub index: u64,
    pub score: Score,
}

impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for Document {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.cmp(&other.score)
    }
}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        other.index == self.index
    }
}

impl Eq for Document {}

/// Documents collector, keeping track of the top N items (based on their score).
///
/// Internally, this uses a binary min-heap as an efficient manner of keeping track of the top N
/// documents.
pub struct TopScoreCollector {
    // heap should be a min-heap, so use Reverse to achieve this
    heap: BinaryHeap<Reverse<Document>>,
    sorted_docs: Vec<Document>,
    limit: usize,
}

impl TopScoreCollector {
    /// Constructs a new `TopScoreCollector` with a hardcoded `limit` of documents.
    pub fn new(limit: usize) -> Self {
        TopScoreCollector {
            limit,
            sorted_docs: Vec::with_capacity(limit),
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    /// Resets the internal state of the collector.
    pub fn reset(&mut self) {
        self.heap.clear();
    }

    /// Consumes a `SearchStream`, collecting the items and only keeping the top N items (based on
    /// their score).
    ///
    /// # Example
    /// ```
    /// use porigon::{SearchableBuilder, SearchStream, TopScoreCollector};
    ///
    /// let storage = SearchableBuilder::default().build(vec!(
    ///     ("foo", 1),
    ///     ("foobar", 2),
    /// )).unwrap();
    /// let searchable = storage.to_searchable().unwrap();
    /// let mut collector = TopScoreCollector::new(1);
    ///
    /// collector.consume_stream(
    ///     searchable
    ///         .starts_with("foo")
    ///         .rescore(|_, index, _| index * 2)
    /// );
    /// collector.consume_stream(
    ///     searchable
    ///         .exact_match("foobar")
    /// );
    /// assert_eq!(collector.top_documents()[0].index, 2);
    /// ```
    pub fn consume_stream<S>(&mut self, mut stream: S)
    where
        S: SearchStream,
    {
        while let Some((_, index, score)) = stream.next() {
            self.process_document(Document { score, index });
        }
    }

    /// Collects a single item
    pub fn collect_item(&mut self, index: u64, score: Score) {
        self.process_document(Document { score, index })
    }

    fn process_document(&mut self, doc: Document) {
        if let Some(Reverse(existing_doc)) = self.heap.iter().find(|other| other.0 == doc) {
            if doc.score > existing_doc.score {
                // The heap already contains this document, but it has a lower score than
                // the one we are trying to add now. Since BinaryHeap does not have a method
                // to remove an item, we'll manually pop all the items using sorted_docs as
                // a scratch space.
                self.sorted_docs.clear();
                while let Some(Reverse(item)) = self.heap.pop() {
                    if item == doc {
                        break;
                    }
                    self.sorted_docs.push(item);
                }
                self.heap.push(Reverse(doc));
                while let Some(item) = self.sorted_docs.pop() {
                    self.heap.push(Reverse(item));
                }
            }
            return;
        }

        if self.heap.len() < self.limit {
            self.heap.push(Reverse(doc));
        } else if let Some(mut head) = self.heap.peek_mut() {
            if head.0.score < doc.score {
                *head = Reverse(doc);
            }
        }
    }

    /// Returns a slice of the processed top documents, ordered by their score.
    pub fn top_documents(&mut self) -> &[Document] {
        self.sorted_docs.clear();
        while let Some(doc) = self.heap.pop() {
            self.sorted_docs.push(doc.0)
        }
        self.sorted_docs.reverse();
        self.sorted_docs.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector() {
        let mut collector = TopScoreCollector::new(5);
        for i in 0..10 {
            collector.process_document(Document {
                index: i,
                score: (i as Score),
            });
        }
        let top_docs = collector.top_documents();

        // check scores
        let scores: Vec<Score> = top_docs.iter().map(|doc| doc.score).collect();
        assert_eq!(scores, vec!(9, 8, 7, 6, 5));

        // check indices
        let indices: Vec<u64> = top_docs.iter().map(|doc| doc.index).collect();
        assert_eq!(indices, vec!(9, 8, 7, 6, 5));
    }

    #[test]
    fn test_collector_duplicates() {
        let mut collector = TopScoreCollector::new(7);
        for i in 0..10 {
            collector.process_document(Document {
                index: i,
                score: (i as Score),
            });
        }
        collector.process_document(Document {
            index: 3,
            score: 6 as Score,
        });
        collector.process_document(Document {
            index: 6,
            score: 2 as Score,
        });
        collector.process_document(Document {
            index: 2,
            score: 8 as Score,
        });
        let top_docs = collector.top_documents();

        // check scores
        let scores: Vec<Score> = top_docs.iter().map(|doc| doc.score).collect();
        assert_eq!(scores, vec!(9, 8, 8, 7, 6, 6, 5));

        // check indices
        let indices: Vec<u64> = top_docs.iter().map(|doc| doc.index).collect();
        assert_eq!(indices, vec!(9, 2, 8, 7, 6, 3, 5));
    }
}
