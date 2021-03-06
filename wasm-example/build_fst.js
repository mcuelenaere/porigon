const fs = require('fs');
const readline = require('readline');
const wasmExample = require('./pkg/porigon_wasm_example');

function openFile(filename) {
  return readline.createInterface({
    input: fs.createReadStream(filename),
    crlfDelay: Infinity,
  });
}

function transliterate(s) {
    // https://towardsdatascience.com/difference-between-nfd-nfc-nfkd-and-nfkc-explained-with-python-code-e2631f96ae6c
    // https://en.wikipedia.org/wiki/Combining_character#Unicode_ranges
    return s.normalize("NFKD").replace(/[\u0300-\u036F\u1AB0-\u1AFF\u1DC0-\u1DFF\u20D0-\u20FF\uFE20-\uFE2F]/g, "").toLowerCase();
}

const IGNORED_TYPES = ['short', 'video', 'tvEpisode', 'tvShort', 'tvSpecial'];

async function readFromTitles() {
    const strm = openFile('./data/titles.tsv');
    const titles = [], lookup = {};
    for await (const line of strm) {
        const [tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres] = line.split('\t', 9);
        if (tconst === 'tconst') {
            // header line, skip it
            continue;
        }

        if (IGNORED_TYPES.includes(titleType) || startYear === '\\N') {
            continue;
        }

        const id = parseInt(tconst.replace(/^tt0*/, ''));
        const transliteratedPrimaryTitle = transliterate(primaryTitle);
        const transliteratedOriginalTitle = transliterate(originalTitle);

        titles.push([id, transliteratedPrimaryTitle]);
        if (transliteratedPrimaryTitle !== transliteratedOriginalTitle) {
            titles.push([id, transliteratedOriginalTitle]);
        }
        lookup[id] = {
            titleType,
            primaryTitle,
            startYear,
        };
    }

    return { titles, lookup };
}

async function readFromRatings(lookup) {
    const strm = openFile('./data/ratings.tsv');
    const ratings = {};
    for await (const line of strm) {
        const [tconst, averageRating, numVotes] = line.split('\t', 9);
        if (tconst === 'tconst') {
            // header line, skip it
            continue;
        }

        const id = parseInt(tconst.replace(/^tt0*/, ''));
        if (!(id in lookup)) {
            continue;
        }

        const rating = parseFloat(averageRating);
        ratings[id] = parseInt(rating * 100);
        lookup[id].rating = rating;
    }

    return ratings;
}

async function buildFst(titles, ratings) {
    return wasmExample.build({ titles, ratings });
}

async function writeLookupToDisk(lookup) {
    fs.writeFileSync('./data/lookup.json', JSON.stringify(lookup), 'utf8');
}

async function writeFstToDisk(fst) {
    fs.writeFileSync('./data/fst.bin', fst);
}

async function timeIt(label, fn) {
    console.log(label + '...');
    console.time(label + ' took');
    const ret = await fn();
    console.timeEnd(label + ' took');
    const memUsageInMb = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    console.log(`Current memory usage: ${memUsageInMb} MB`);

    return ret;
}

async function main() {
    const { titles, lookup } = await timeIt('Reading from titles dataset', readFromTitles);
    const ratings = await timeIt('Reading from ratings dataset', () => readFromRatings(lookup));
    const fst = await timeIt('Building FST', () => buildFst(titles, ratings));
    await timeIt('Writing FST to disk', () => writeFstToDisk(fst));
    await timeIt('Writing lookup to disk', () => writeLookupToDisk(lookup));
}

main()
    .catch(err => {
        console.error(err);
        process.exit(1);
    });