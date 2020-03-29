extern crate stats_alloc;
extern crate wasm_bindgen;

use wasm_bindgen::prelude::*;
use porigon::{Searchable, TopScoreCollector};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use stats_alloc::{StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Serialize)]
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

#[derive(Serialize, Deserialize)]
struct SearchData {
    titles: Searchable,
    ratings: HashMap<u64, u32>,
}

impl SearchData {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes.as_slice())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(&self)
    }
}

#[wasm_bindgen]
pub struct Searcher {
    data: SearchData,
    collector: TopScoreCollector,
}

#[derive(Serialize)]
pub struct SearchResult {
    pub index: u64,
    pub score: usize,
}

#[wasm_bindgen]
impl Searcher {
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: Vec<u8>, limit: usize) -> Result<Searcher, JsValue> {
        let data: SearchData = SearchData::from_bytes(bytes).map_err(|err| err_to_js("could not parse data", err))?;
        let collector = TopScoreCollector::new(limit);
        Ok(Searcher { data, collector })
    }

    pub fn search(&mut self, query: &str) -> JsValue {
        let ratings = &self.data.ratings;
        let for_fixed_score = |score: usize| {
            move |_: &[u8], index: u64, _: usize| score + (*ratings.get(&index).unwrap_or(&0) as usize)
        };

        let s1 = self.data.titles
            .exact_match(query)
            .rescore(for_fixed_score(10000));
        let s2 = self.data.titles
            .starts_with(query)
            .rescore(for_fixed_score(0));

        self.collector.reset();
        self.collector.consume_stream(s1);
        self.collector.consume_stream(s2);

        let results: Vec<SearchResult> = self.collector.top_documents()
            .iter()
            .map(|doc| SearchResult { score: doc.score, index: doc.index })
            .collect();

        JsValue::from_serde(&results).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
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
        Searchable::build_from_iter(i).map_err(|err| err_to_js("could not build FST", err))
    };
    let search_data = SearchData {
        titles: build_searchable(data.titles)?,
        ratings: data.ratings,
    };
    search_data.to_bytes().map_err(|err| err_to_js("could not serialize search data", err))
}