#!/usr/bin/env node
import 'dotenv/config';
import { promises as fs } from 'fs';
import path from 'path';
import { parse as parseCSV } from 'csv-parse/sync';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

// If you're on Node 16, also: npm i undici  and uncomment the next line:
// import { fetch } from 'undici';

const argv = yargs(hideBin(process.argv))
  .option('start',   { type: 'string', demandOption: true, desc: 'YYYY-MM (inclusive)' })
  .option('end',     { type: 'string', demandOption: true, desc: 'YYYY-MM (inclusive)' })
  .option('bbox',    { type: 'string', demandOption: true, desc: 'west,south,east,north (deg)' })
  .option('out',     { type: 'string', default: './data/firms' })
  .option('sources', {
    type: 'string',
    default: 'MODIS_SP,VIIRS_SNPP_SP,VIIRS_NOAA20_SP,VIIRS_NOAA21_NRT',
    desc: 'Comma list (e.g., MODIS_SP only to cut calls while testing)'
  })
  .option('throttle', {
    type: 'number',
    default: Number(process.env.FIRMS_THROTTLE_MS || 800),
    desc: 'Delay (ms) between slice requests to avoid 403s'
  })
  .option('maxRetries', {
    type: 'number',
    default: Number(process.env.FIRMS_MAX_RETRIES || 4),
    desc: 'Retries for 403/429 with backoff'
  })
  .option('retryEmpty', {
    type: 'boolean',
    default: true,
    desc: 'If an existing month file has 0 features, rebuild it'
  })
  .option('months', {
    type: 'string',
    desc: 'Optional CSV list of specific months to run (e.g., "2001-04,2001-06")'
  })
  .help().argv;

const MAP_KEY = process.env.FIRMS_MAP_KEY;
if (!MAP_KEY) {
  console.error('❌ Missing FIRMS_MAP_KEY in .env');
  process.exit(1);
}

const DAY_MAX = 10; // 1..10 per API
const THROTTLE_MS = argv.throttle;
const MAX_RETRIES = argv.maxRetries;

// Earliest availability per source; months before this are skipped
const SOURCE_START = {
  MODIS_SP:        '2000-11-01',
  MODIS_NRT:       '2000-11-01',
  VIIRS_SNPP_SP:   '2012-01-20',
  VIIRS_SNPP_NRT:  '2012-01-20',
  VIIRS_NOAA20_SP: '2020-01-01',
  VIIRS_NOAA20_NRT:'2020-01-01',
  VIIRS_NOAA21_NRT:'2024-01-17'
};

const SOURCES = argv.sources.split(',').map(s => s.trim()).filter(Boolean);
const OUTDIR = argv.out;
const BBOX = argv.bbox;

async function mkdirp(dir){ await fs.mkdir(dir, { recursive: true }); }
async function pathExists(p){ try{ await fs.access(p); return true; } catch { return false; } }
const sleep = ms => new Promise(r => setTimeout(r, ms));

function monthList(startYM, endYM) {
  if (argv.months) {
    return argv.months.split(',').map(m => m.trim()).filter(Boolean).map(k => {
      const [y,m] = k.split('-').map(Number);
      return { y, m, key: k };
    });
  }
  const [ys, ms] = startYM.split('-').map(Number);
  const [ye, me] = endYM.split('-').map(Number);
  const out = [];
  for (let y = ys, m = ms; y < ye || (y === ye && m <= me); ) {
    out.push({ y, m, key: `${y}-${String(m).padStart(2,'0')}` });
    m++; if (m > 12) { m = 1; y++; }
  }
  return out;
}
function daysInMonth(y, m1to12){ return new Date(Date.UTC(y, m1to12, 0)).getUTCDate(); }
function ymLt(a, b){ return a < b; } // YYYY-MM string compare

function areaCSVUrl({ mapKey, source, bbox, startDateISO, dayRange }) {
  return `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${mapKey}/${source}/${bbox}/${dayRange}/${startDateISO}`;
}

function rowToFeature(row) {
  const lat = Number(row.latitude), lon = Number(row.longitude);
  const props = {
    acq_date: row.acq_date, acq_time: row.acq_time,
    satellite: row.satellite, instrument: row.instrument,
    confidence: row.confidence, frp: row.frp ? Number(row.frp) : undefined,
    daynight: row.daynight, version: row.version, src: row.src || row.source || ''
  };
  Object.keys(props).forEach(k => props[k] === undefined && delete props[k]);
  return { type: 'Feature', properties: props, geometry: { type: 'Point', coordinates: [lon, lat] } };
}
function featureKey(f){
  const p = f.properties || {}, [lon,lat] = f.geometry.coordinates;
  return `${lon.toFixed(4)}:${lat.toFixed(4)}:${p.acq_date}:${p.acq_time}:${p.instrument||''}:${p.satellite||''}`;
}

async function fetchSlice({ source, bbox, startDateISO, dayRange }) {
  const url = areaCSVUrl({ mapKey: MAP_KEY, source, bbox, startDateISO, dayRange });
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} (${source} ${startDateISO} +${dayRange-1})`);
  const text = await res.text();
  if (!text.trim()) return [];
  const rows = parseCSV(text, { columns: true, skip_empty_lines: true });
  return rows.map(rowToFeature);
}

function httpStatusFromError(err){
  const m = /HTTP\s+(\d+)/.exec(err?.message || '');
  return m ? Number(m[1]) : null;
}

async function fetchSliceRetry(args) {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const out = await fetchSlice(args);
      // small pace delay between any successful calls
      await sleep(THROTTLE_MS);
      return out;
    } catch (err) {
      const code = httpStatusFromError(err);
      if (code === 403 || code === 429) {
        const backoff = Math.min(120000, 15000 * (attempt + 1)); // 15s,30s,45s,60s
        console.warn(`⏳ limit (HTTP ${code}) for ${args.source} ${args.startDateISO} +${args.dayRange-1}. Cooling ${Math.round(backoff/1000)}s…`);
        await sleep(backoff);
        continue;
      }
      // Other errors: bubble up
      throw err;
    }
  }
  // All retries exhausted → treat as empty
  return [];
}

async function readExistingCount(file){
  try {
    const txt = await fs.readFile(file, 'utf8');
    const gj = JSON.parse(txt);
    return Array.isArray(gj.features) ? gj.features.length : 0;
  } catch { return null; }
}

async function prebakeMonth({ y, m }) {
  const monthKey = `${y}-${String(m).padStart(2,'0')}`;
  const outfile = path.join(OUTDIR, `CA-${monthKey}.geojson`);

  // If file exists and has data (or retryEmpty=false) → skip
  if (await pathExists(outfile)) {
    if (!argv.retryEmpty) { console.log(`✔ ${outfile} (exists)`); return; }
    const n = await readExistingCount(outfile);
    if (n === null) {
      // unreadable → rebuild
    } else if (n > 0) {
      console.log(`✔ ${outfile} (exists, features=${n})`);
      return;
    } else {
      console.log(`↻ ${outfile} has 0 features → retrying`);
    }
  }

  const dim = daysInMonth(y, m);
  const windows = [];
  for (let d = 1; d <= dim; d += DAY_MAX) {
    const startDateISO = `${y}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    const dayRange = Math.min(DAY_MAX, dim - d + 1);
    windows.push({ startDateISO, dayRange });
  }

  const uniq = new Map();
  let sawLimit = false;

  for (const source of SOURCES) {
    const start = SOURCE_START[source] || '1900-01-01';
    if (ymLt(monthKey, start.slice(0,7))) continue;

    for (const w of windows) {
      try {
        const feats = await fetchSliceRetry({ source, bbox: BBOX, ...w });
        for (const f of feats) {
          const k = featureKey(f);
          if (!uniq.has(k)) uniq.set(k, f);
        }
      } catch (err) {
        const code = httpStatusFromError(err);
        if (code === 403 || code === 429) {
          sawLimit = true;
          console.warn(`⚠ ${source} ${w.startDateISO} (${w.dayRange}d): HTTP ${code}`);
        } else {
          console.warn(`⚠ ${source} ${w.startDateISO} (${w.dayRange}d): ${err.message}`);
        }
      }
    }
  }

  const count = uniq.size;
  const fc = { type: 'FeatureCollection', features: Array.from(uniq.values()) };

  // If we ended up with zero AND we saw a rate-limit → don't write; retry later
  if (count === 0 && sawLimit) {
    console.warn(`↻ Skipping write for ${outfile} (0 features due to rate limit). Re-run later to fill.`);
    return;
  }

  await mkdirp(OUTDIR);
  await fs.writeFile(outfile, JSON.stringify(fc));
  console.log(`💾 Wrote ${outfile}  features=${count}`);
}

(async () => {
  console.log('FIRMS prebake starting…');
  console.log(`Sources: ${SOURCES.join(', ')}`);
  console.log(`BBOX: ${BBOX}`);
  console.log(`Throttle: ${THROTTLE_MS} ms, Retries: ${MAX_RETRIES}, RetryEmpty: ${argv.retryEmpty}`);
  await mkdirp(OUTDIR);

  const months = monthList(argv.start, argv.end);
  for (const mm of months) {
    await prebakeMonth(mm);
  }
  console.log('✅ Done');
})();
