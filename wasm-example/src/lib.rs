use porigon::doc_id_storage::DefaultStorage;
use porigon::levenshtein::LevenshteinAutomatonBuilder;
use porigon::searchable::SearchableBuilder;
use porigon::text::DefaultTextNormalizer;
use porigon::{SearchStream, SearchableStorage, TopScoreCollector};
use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, Serializer},
    Archive, Serialize,
};
use stats_alloc::{StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(serde::Serialize)]
pub struct MemoryStats {
    pub allocations: usize,
    pub deallocations: usize,
    pub bytes_used: isize,
}

#[wasm_bindgen]
pub fn memory_stats() -> JsValue {
    let stats = GLOBAL.stats();
    let result = MemoryStats {
        allocations: stats.allocations + stats.reallocations,
        deallocations: stats.deallocations,
        bytes_used: stats.bytes_allocated as isize - stats.bytes_deallocated as isize,
    };

    JsValue::from_serde(&result).unwrap()
}

#[derive(Archive, Serialize)]
struct SearchData {
    titles: SearchableStorage<DefaultStorage, DefaultTextNormalizer>,
    ratings: HashMap<u64, u32>,
}

impl ArchivedSearchData {
    pub fn from_bytes(bytes: &Vec<u8>) -> &Self {
        unsafe { archived_root::<SearchData>(bytes.as_slice()) }
    }
}

impl SearchData {
    pub fn to_bytes(&self) -> Result<Vec<u8>, JsError> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(self)?;
        Ok(serializer.into_serializer().into_inner().into_vec())
    }
}

#[wasm_bindgen]
pub struct Searcher {
    data: Vec<u8>,
    collector: TopScoreCollector,
    levenshtein_builder_1: LevenshteinAutomatonBuilder,
    levenshtein_builder_2: LevenshteinAutomatonBuilder,
}

#[derive(serde::Serialize)]
pub struct SearchResult {
    pub index: u64,
    pub score: u64,
}

#[wasm_bindgen]
impl Searcher {
    #[wasm_bindgen(constructor)]
    pub fn new(data: Vec<u8>, limit: usize) -> Searcher {
        let collector = TopScoreCollector::new(limit);
        let levenshtein_builder_1 = LevenshteinAutomatonBuilder::new(1, false);
        let levenshtein_builder_2 = LevenshteinAutomatonBuilder::new(2, false);
        Searcher {
            data,
            collector,
            levenshtein_builder_1,
            levenshtein_builder_2,
        }
    }

    pub fn search(&mut self, query: &str) -> JsValue {
        let data = ArchivedSearchData::from_bytes(&self.data);
        let ratings = &data.ratings;
        let get_rating_for = move |index| *ratings.get(&index).unwrap_or(&0) as u64;

        let titles = data.titles.to_searchable().unwrap();

        self.collector.reset();

        self.collector.consume_stream(
            titles
                .exact_match(query)
                .rescore(move |_, index, _| 50000 + get_rating_for(index)),
        );
        self.collector.consume_stream(
            titles
                .starts_with(query)
                .rescore(move |_, index, _| 40000 + get_rating_for(index)),
        );

        if query.len() > 3 {
            // running the levenshtein matchers on short query strings is quite expensive, so don't do that
            self.collector.consume_stream(
                titles
                    .levenshtein_exact_match(&self.levenshtein_builder_1, query)
                    .rescore(move |_, index, _| 30000 + get_rating_for(index)),
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_starts_with(&self.levenshtein_builder_1, query)
                    .rescore(move |_, index, _| 20000 + get_rating_for(index)),
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_exact_match(&self.levenshtein_builder_2, query)
                    .rescore(move |_, index, _| 10000 + get_rating_for(index)),
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_starts_with(&self.levenshtein_builder_2, query)
                    .rescore(move |_, index, _| get_rating_for(index)),
            );
        }

        let results: Vec<SearchResult> = self
            .collector
            .top_documents()
            .iter()
            .map(|doc| SearchResult {
                score: doc.score,
                index: doc.index,
            })
            .collect();

        JsValue::from_serde(&results).unwrap()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BuildData {
    titles: Vec<(String, u64)>,
    ratings: HashMap<u64, u32>,
}

#[wasm_bindgen]
pub fn build(val: &JsValue) -> Result<Vec<u8>, JsError> {
    let data: BuildData = val.into_serde()?;
    let search_data = SearchData {
        titles: SearchableBuilder::default().build(data.titles)?,
        ratings: data.ratings,
    };
    Ok(search_data.to_bytes()?)
}
