extern crate stats_alloc;
extern crate wasm_bindgen;

use wasm_bindgen::prelude::*;
use porigon::{TopScoreCollector, LevenshteinAutomatonBuilder, SearchableStorage, SearchStream};
use rkyv::{Archive, Serialize, AlignedVec, archived_root, ser::{serializers::AlignedSerializer, Serializer}};
use std::collections::HashMap;
use stats_alloc::{StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

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

#[inline]
fn err_to_js<D: std::fmt::Display>(prefix: &'static str, displayable: D) -> JsValue {
    JsValue::from(format!("{}: {}", prefix, displayable))
}

#[derive(Archive, Serialize)]
struct SearchData {
    titles: SearchableStorage,
    ratings: HashMap<u64, u32>,
}

impl ArchivedSearchData {
    pub fn from_bytes(bytes: &Vec<u8>) -> &Self {
        unsafe { archived_root::<SearchData>(bytes.as_slice()) }
    }
}

impl SearchData {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut serializer = AlignedSerializer::new(AlignedVec::new());
        serializer.serialize_value(self).unwrap();
        serializer.into_inner().into_vec()
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
    pub fn new(data: Vec<u8>, limit: usize) -> Result<Searcher, JsValue> {
        let collector = TopScoreCollector::new(limit);
        let levenshtein_builder_1 = LevenshteinAutomatonBuilder::new(1, false);
        let levenshtein_builder_2 = LevenshteinAutomatonBuilder::new(2, false);
        Ok(Searcher { data, collector, levenshtein_builder_1, levenshtein_builder_2 })
    }

    pub fn search(&mut self, query: &str) -> JsValue {
        let data = ArchivedSearchData::from_bytes(&self.data);
        let ratings = &data.ratings;
        let get_rating_for = move |index| {
            *ratings.get(&index).unwrap_or(&0) as u64
        };

        let titles = data.titles.as_searchable().unwrap();

        self.collector.reset();

        self.collector.consume_stream(
            titles
                .exact_match(query)
                .rescore(move |_, index, _| 50000 + get_rating_for(index))
        );
        self.collector.consume_stream(
            titles
                .starts_with(query)
                .rescore(move |_, index, _| 40000 + get_rating_for(index))
        );

        if query.len() > 3 {
            // running the levenshtein matchers on short query strings is quite expensive, so don't do that
            self.collector.consume_stream(
                titles
                    .levenshtein_exact_match(&self.levenshtein_builder_1, query)
                    .rescore(move |_, index, _| 30000 + get_rating_for(index))
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_starts_with(&self.levenshtein_builder_1, query)
                    .rescore(move |_, index, _| 20000 + get_rating_for(index))
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_exact_match(&self.levenshtein_builder_2, query)
                    .rescore(move |_, index, _| 10000 + get_rating_for(index))
            );
            self.collector.consume_stream(
                titles
                    .levenshtein_starts_with(&self.levenshtein_builder_2, query)
                    .rescore(move |_, index, _| get_rating_for(index))
            );
        }

        let results: Vec<SearchResult> = self.collector.top_documents()
            .iter()
            .map(|doc| SearchResult { score: doc.score, index: doc.index })
            .collect();

        JsValue::from_serde(&results).unwrap()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BuildData {
    titles: Vec<(u64, String)>,
    ratings: HashMap<u64, u32>,
}

#[wasm_bindgen]
pub fn build(val: &JsValue) -> Result<Vec<u8>, JsValue> {
    let data: BuildData = val.into_serde().map_err(|err| err_to_js("failed to parse data", err))?;
    let build_searchable = |input: Vec<(u64, String)>| {
        let mut i: Vec<(&[u8], u64)> = input.iter().map(|(key, val)| (val.as_bytes(), *key)).collect();
        i.sort_by_key(|(key, _)| *key);
        SearchableStorage::build_from_iter(i).map_err(|err| err_to_js("could not build FST", err))
    };
    let search_data = SearchData {
        titles: build_searchable(data.titles)?,
        ratings: data.ratings,
    };
    Ok(search_data.to_bytes())
}