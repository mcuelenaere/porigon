porigon
===
Lightweight FST-based autocompleter library written in Rust, targeting WebAssembly and data stored in-memory

[![Build status](https://github.com/mcuelenaere/porigon/workflows/Rust/badge.svg)](https://github.com/mcuelenaere/porigon/actions)
[![](http://meritbadge.herokuapp.com/porigon)](https://crates.io/crates/porigon)

Licensed under MIT.


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

Check out the documentation or `wasm-example` for a more examples.
