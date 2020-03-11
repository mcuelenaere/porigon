use fst::Streamer;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub type DocScore = usize;

pub struct Document {
    pub index: u64,
    pub score: DocScore,
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

pub struct TopScoreCollector {
    heap: BinaryHeap<Document>,
    sorted_docs: Vec<Document>,
    limit: usize,
}

impl TopScoreCollector {
    pub fn new(limit: usize) -> Self {
        TopScoreCollector {
            limit,
            sorted_docs: Vec::with_capacity(limit),
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    pub fn reset(&mut self) {
        self.heap.clear();
    }

    pub fn consume_stream<S>(&mut self, stream: &mut S) where S: for<'a> Streamer<'a, Item=(DocScore, u64)> {
        while let Some((score, index)) = stream.next() {
            self.process_document(Document { score, index });
        }
    }

    pub fn process_document(&mut self, doc: Document) {
        let already_contains_doc = self.heap.iter().any(|other| *other == doc);
        if already_contains_doc {
            return;
        }

        if self.heap.len() < self.limit {
            self.heap.push(doc);
        } else if let Some(mut head) = self.heap.peek_mut() {
            if *head < doc {
                *head = doc;
            }
        }
    }

    pub fn top_documents(&mut self) -> &[Document] {
        self.sorted_docs.clear();
        while let Some(doc) = self.heap.pop() {
            self.sorted_docs.push(doc)
        }
        self.sorted_docs.as_slice()
    }
}