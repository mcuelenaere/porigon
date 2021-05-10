mod collectors;
mod searchable;
mod streams;

pub use crate::{
    collectors::TopScoreCollector,
    searchable::{SearchableStorage, Searchable},
    streams::SearchStream
};
pub use levenshtein_automata::{LevenshteinAutomatonBuilder, Distance as LevenshteinDistance};

/// Score type, used for keeping a per-item score in `SearchStream`.
pub type Score = u64;