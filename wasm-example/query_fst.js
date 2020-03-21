const fs = require('fs');
const readline = require('readline');
const { Searcher, memory_stats } = require('./pkg/porigon_wasm_example');

function transliterate(s) {
    // https://towardsdatascience.com/difference-between-nfd-nfc-nfkd-and-nfkc-explained-with-python-code-e2631f96ae6c
    // https://en.wikipedia.org/wiki/Combining_character#Unicode_ranges
    return s.normalize("NFKD").replace(/[\u0300-\u036F\u1AB0-\u1AFF\u1DC0-\u1DFF\u20D0-\u20FF\uFE20-\uFE2F]/g, "").toLowerCase();
}

function askUserForQuery(rl) {
    return new Promise((resolve) => {
        rl.question('Search query: ', resolve);
    })
}

async function main() {
    console.time('Reading lookup');
    const lookup = JSON.parse(fs.readFileSync('./data/lookup.json', 'utf8'));
    console.timeEnd('Reading lookup');
    console.time('Reading FST');
    const fst_data = fs.readFileSync('./data/fst.bin', null);
    console.timeEnd('Reading FST');
    console.time('Initing searcher');
    const searcher = new Searcher(fst_data, 10);
    console.timeEnd('Initing searcher');
    const memUsageInMb = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    console.log(`Process memory usage: ${memUsageInMb}MB`);

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      completer: (line) => {
        const results = searcher.search(transliterate(line));
        const completions = Array.from(results).map(({ index }) => lookup[index].primaryTitle);
        const dedupedCompletions = Array.from(new Set(completions));
        return [dedupedCompletions, line];
      }
    });

    let query;
    do {
        console.log('');
        query = transliterate(await askUserForQuery(rl));
        if (!query) {
            break;
        }

        console.time('Querying FST took');
        const results = searcher.search(query);
        console.timeEnd('Querying FST took');

        const stats = memory_stats();
        const bytesUsedInMb = Math.round(stats.bytes_used / 1024 / 1024 * 100) / 100;
        console.log(`WASM memory usage: ${bytesUsedInMb}MB`);

        for (const { index, score } of results) {
            const { primaryTitle, titleType, rating, startYear } = lookup[index];
            const imdbId = 'tt' + index.toString().padStart(7, '0');
            const formattedRating = (rating || 0).toString().padStart(3, ' ');
            console.log(` * {${formattedRating}} ${primaryTitle} [${startYear}] (https://imdb.com/title/${imdbId}/) [type=${titleType}, score=${score}]`);
        }
    } while (query);

    rl.close();
}

main()
    .catch(err => {
        console.error(err);
        process.exit(1);
    });

