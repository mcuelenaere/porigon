porigon
===
Lightweight FST-based autocompleter library written in Rust, targeting WebAssembly and data stored in-memory

[![Build status](https://github.com/mcuelenaere/porigon/workflows/Rust/badge.svg)](https://github.com/mcuelenaere/porigon/actions)
[![](http://meritbadge.herokuapp.com/porigon)](https://crates.io/crates/porigon)

Licensed under MIT.

### Intended usecase

The idea of this library is to have a lightweight, yet idiomatic API around the [fst crate](https://github.com/BurntSushi/fst) that allows you to construct, serialize/deserialize and query FSTs in an WebAssembly environment. It's an ideal starting point for building an autocompleter service that can be used on the web, the edge (eg Cloudflare Worker) or the backend-side (node.js).

Existing solutions like eg [tantivy](https://github.com/tantivy-search/tantivy) are not fitting as they're too heavyweight (wasm binary size is over 1MB) or not compilable to WebAssembly. If you're looking for a more full fledged full-text search engine, take a look at the list of alternatives at the bottom.


### Documentation

https://docs.rs/porigon


### Installation

Simply add a corresponding entry to your `Cargo.toml` dependency list:

```toml,ignore
[dependencies]
porigon = "0.1.0"
```

### Example

This example demonstrates building a `Searchable` in memory, executing a `StartsWith` query
against it and collecting the top 3 documents with `TopScoreCollector`.

```rust
use porigon::{Searchable, TopScoreCollector};

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let items = vec!(
    ("bar".as_bytes(), 1),
    ("foo".as_bytes(), 2),
    ("foobar".as_bytes(), 3)
  );
  let searchable = Searchable::build_from_iter(items)?;

  let mut collector = TopScoreCollector::new(3);
  collector.consume_stream(
    searchable
      .starts_with("foo")
      .rescore(|_, index, _| index * 2)
  );

  let docs = collector.top_documents();
  assert_eq!(docs[0].index, 3);
  
  Ok(())
}
```

When use the library in browser, load the FST as follows:
```js
  let fst_data = await fetch("./data/fst.bin").then(response => response.arrayBuffer());
  fst_data  = new Uint8Array(fst_data);
```

Check out the documentation or `wasm-example` for a more examples.

### Alternatives

If you're looking for a more general-purpose full-text search engine, take a look at these alternatives:

 - https://github.com/tantivy-search/tantivy
 - https://github.com/meilisearch/MeiliSearch
 - https://github.com/toshi-search/Toshi
