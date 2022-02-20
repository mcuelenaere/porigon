pub mod collectors;
pub use levenshtein_automata as levenshtein;
pub mod doc_id_storage;
pub mod searchable;
pub mod streams;
pub mod text;

pub use crate::{
    collectors::TopScoreCollector,
    searchable::{Searchable, SearchableBuilder, SearchableStorage},
    streams::SearchStream,
};

/// Score type, used for keeping a per-item score in `SearchStream`.
pub type Score = u64;
