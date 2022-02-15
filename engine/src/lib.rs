mod collectors;
mod searchable;
mod streams;

pub use crate::{
    collectors::TopScoreCollector,
    searchable::{ArchivedSearchable, Searchable, SearchableStorage},
    streams::SearchStream,
};
pub use levenshtein_automata::{Distance as LevenshteinDistance, LevenshteinAutomatonBuilder};

/// Score type, used for keeping a per-item score in `SearchStream`.
pub type Score = u64;
