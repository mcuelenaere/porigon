extern crate wasm_bindgen;
extern crate wee_alloc;

use wasm_bindgen::prelude::*;
use porigon::{Searchable, TopScoreCollector};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[inline]
fn err_to_js<D: std::fmt::Display>(prefix: &'static str, displayable: D) -> JsValue {
    JsValue::from(format!("{}: {}", prefix, displayable))
}

#[derive(Serialize, Deserialize)]
struct SearchData {
    shortcodes: Searchable,
    names: Searchable,
    name_aliases: Searchable,
    item_ranks: HashMap<u64, u32>,
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

#[wasm_bindgen]
impl Searcher {
    pub fn new(bytes: Vec<u8>) -> Result<Searcher, JsValue> {
        let data: SearchData = SearchData::from_bytes(bytes).map_err(|err| err_to_js("could not parse data", err))?;
        let collector = TopScoreCollector::new(25);
        Ok(Searcher { data, collector })
    }

    pub fn search(&mut self, query: &str) -> Vec<u64> {
        let item_ranks = &self.data.item_ranks;
        let for_fixed_score = |score: usize| {
            move |_: &[u8], index: u64| (score << 16) | (*item_ranks.get(&index).unwrap_or(&0) as usize)
        };

        let exact_match_score = for_fixed_score(100);
        let starts_with_score = for_fixed_score(95);
        let alias_exact_match_score = for_fixed_score(90);
        let alias_starts_with_score = for_fixed_score(85);
        let mut s1 = self.data.shortcodes
            .exact_match(query)
            .with_score(&exact_match_score);
        let mut s2 = self.data.shortcodes
            .starts_with(query)
            .with_score(&starts_with_score);
        let mut s3 = self.data.names
            .exact_match(query)
            .with_score(&exact_match_score);
        let mut s4 = self.data.names
            .starts_with(query)
            .with_score(&starts_with_score);
        let mut s5 = self.data.name_aliases
            .exact_match(query)
            .with_score(&alias_exact_match_score);
        let mut s6 = self.data.name_aliases
            .starts_with(query)
            .with_score(&alias_starts_with_score);

        self.collector.reset();
        self.collector.consume_stream(&mut s1);
        self.collector.consume_stream(&mut s2);
        self.collector.consume_stream(&mut s3);
        self.collector.consume_stream(&mut s4);
        self.collector.consume_stream(&mut s5);
        self.collector.consume_stream(&mut s6);

        self.collector.top_documents().iter().map(|doc| doc.index).collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct BuildData {
    shortcodes: Vec<(u64, String)>,
    names: Vec<(u64, String)>,
    name_aliases: Vec<(u64, String)>,
    item_ranks: HashMap<u64, u32>,
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
        shortcodes: build_searchable(data.shortcodes)?,
        names: build_searchable(data.names)?,
        name_aliases: build_searchable(data.name_aliases)?,
        item_ranks: data.item_ranks,
    };
    search_data.to_bytes().map_err(|err| err_to_js("could not serialize search data", err))
}