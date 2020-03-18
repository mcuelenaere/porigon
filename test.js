const fs = require('fs');
const { build, Searcher } = require('./wasm-example/pkg/porigon_wasm_example');

function transliterate(s) {
    // https://towardsdatascience.com/difference-between-nfd-nfc-nfkd-and-nfkc-explained-with-python-code-e2631f96ae6c
    // https://en.wikipedia.org/wiki/Combining_character#Unicode_ranges
    return s.normalize("NFKD").replace(/[\u0300-\u036F\u1AB0-\u1AFF\u1DC0-\u1DFF\u20D0-\u20FF\uFE20-\uFE2F]/g, "").toLowerCase();
}

function* generate_aliases(text) {
    const parts = text.split(/\s+/);
    if (parts.length <= 1) {
        return;
    }

    for (let i = 1; i < parts.length; i++) {
        yield parts.slice(i).join(' ');
    }
}

function readData() {
    const text = fs.readFileSync('./codes.csv', 'utf-8');
    const shortcodes = [], names = [], name_aliases = [], market_caps = [], lookup = {};
    for (const line of text.split('\n')) {
        if (!line) {
            continue;
        }

        let [id, code, marketCap, name] = line.split(',', 4);
        id = parseInt(id);
        code = transliterate(code);
        name = transliterate(name);
        marketCap = Math.trunc(parseFloat(marketCap));

        lookup[id] = code;
        shortcodes.push([id, code]);
        names.push([id, name]);
        for (const alias of generate_aliases(name)) {
            name_aliases.push([id, alias]);
        }
        market_caps.push([id, marketCap]);
    }

    // sort market caps
    market_caps.sort((a, b) => b[1] - a[1]);

    // build item weights
    // linearly scale market cap
    const item_ranks = {};
    let counter = 0;
    for (const [id, _] of market_caps) {
        item_ranks[id] = market_caps.length - counter;
        counter++;
    }

    return {
        lookup,
        shortcodes,
        names,
        name_aliases,
        item_ranks,
    };
}

if (process.argv.length !== 3) {
    console.error('Usage: node test.js <query>');
    process.exit(1);
}

const query = transliterate(process.argv[2]);
console.log('Searching for', query);

const t0 = process.hrtime.bigint();
const data = readData();
//const fst_data = fs.readFileSync('./codes.bin', null);
const t1 = process.hrtime.bigint();
const fst_data = build(data);
const t2 = process.hrtime.bigint();
fs.writeFileSync('codes.bin', fst_data);
const t3 = process.hrtime.bigint();
const searcher = Searcher.new(fst_data);
const t4 = process.hrtime.bigint();
const searchResults = searcher.search(query);
const t5 = process.hrtime.bigint();
console.log(Array.from(searchResults).map(id => data.lookup[id.toString()]));
const t6 = process.hrtime.bigint();
/*for (const code of Object.values(data.shortcodes)) {
    searcher.search(code);
}
const t7 = process.hrtime.bigint();*/

console.log('read:', (t1 - t0) / 1000000n);
console.log('build:', (t2 - t1) / 1000000n);
console.log('write:', (t3 - t2) / 1000000n);
console.log('init:', (t4 - t3) / 1000000n);
console.log('search:', (t5 - t4) / 1000000n);
console.log('display:', (t6 - t5) / 1000000n);
//console.log('search_all:', (t7 - t6) / 1000000n);
