const fs = require('fs');
const { build, Searcher } = require('./wasm-example/pkg/porigon_wasm_example');

function readData() {
    const text = fs.readFileSync('./codes.csv', 'utf-8');
    const shortcodes = {}, names = {}, name_aliases = {}, market_caps = [];
    const seenCodes = new Set();
    for (const line of text.split('\n')) {
        if (!line) {
            continue;
        }

        let [id, code, marketCap, name] = line.split(',', 4);
        id = parseInt(id);
        code = code.toLowerCase();
        name = name.toLowerCase();
        marketCap = Math.trunc(parseFloat(marketCap));

        // HACK
        if (seenCodes.has(code)) {
            continue;
        }

        shortcodes[id] = code;
        //names[id] = name;
        //name_aliases[id] = name; // TODO
        market_caps.push([id, marketCap]);
        seenCodes.add(code);
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

const query = process.argv[2];
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
console.log(Array.from(searchResults).map(id => data.shortcodes[id.toString()]));
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