pub mod collectors;
pub use levenshtein_automata as levenshtein;
pub mod searchable;
pub mod streams;

pub use crate::{
    collectors::TopScoreCollector,
    searchable::{Searchable, SearchableStorage},
    streams::SearchStream,
};

/// Score type, used for keeping a per-item score in `SearchStream`.
pub type Score = u64;
