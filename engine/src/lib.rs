mod collectors;
mod searchable;

pub use crate::{
    collectors::TopScoreCollector,
    searchable::{SearchableStorage, Searchable}
};
pub use levenshtein_automata::{LevenshteinAutomatonBuilder, Distance as LevenshteinDistance};

/// Score type, used for keeping a per-item score in `SearchStream`.
pub type Score = u64;

/// Search result
pub type SearchResult<'a> = (&'a str, u64, Score);