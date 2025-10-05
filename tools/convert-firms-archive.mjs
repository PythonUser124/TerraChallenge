#!/usr/bin/env node
import { promises as fs } from 'fs';
import path from 'path';
import { parse as parseCSV } from 'csv-parse/sync';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const argv = yargs(hideBin(process.argv))
  .option('in',  { type: 'string', demandOption: true, desc: 'Input folder with FIRMS CSV/JSON files' })
  .option('out', { type: 'string', default: './data/firms', desc: 'Output folder for CA-YYYY-MM.geojson' })
  .option('bbox',{ type: 'string', default: '-125,32,-113.5,43', desc: 'west,south,east,north' })
  .help().argv;

const OUTDIR = argv.out;
const [WEST,SOUTH,EAST,NORTH] = argv.bbox.split(',').map(Number);
const isInBox = (lon,lat) => lon>=WEST && lon<=EAST && lat>=SOUTH && lat<=NORTH;

const toMonthKey = (iso) => iso.slice(0,7); // YYYY-MM
const featureKey = (f) => {
  const p=f.properties||{}, [lon,lat]=f.geometry.coordinates;
  return `${lon.toFixed(4)}:${lat.toFixed(4)}:${p.acq_date}:${p.acq_time}:${p.instrument||''}:${p.satellite||''}`;
};

function rowToFeature(row){
  const lat = Number(row.latitude ?? row.Latitude ?? row.lat);
  const lon = Number(row.longitude ?? row.Longitude ?? row.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (!isInBox(lon,lat)) return null;
  const props = {
    acq_date: row.acq_date ?? row.ACQ_DATE ?? row.date,
    acq_time: row.acq_time ?? row.ACQ_TIME ?? row.time,
    satellite: row.satellite ?? row.SATELLITE,
    instrument: row.instrument ?? row.INSTRUMENT,
    confidence: row.confidence ?? row.CONFIDENCE,
    frp: row.frp ?? row.FRP,
    daynight: row.daynight ?? row.DAYNIGHT,
    version: row.version ?? row.VERSION
  };
  return { type:'Feature', properties: props, geometry:{ type:'Point', coordinates:[lon,lat] } };
}

async function loadOne(file){
  const txt = await fs.readFile(file,'utf8');
  if (file.toLowerCase().endsWith('.csv') || file.toLowerCase().endsWith('.txt')){
    const rows = parseCSV(txt, { columns:true, skip_empty_lines:true });
    return rows.map(rowToFeature).filter(Boolean);
  }
  // JSON: try GeoJSON FeatureCollection or array-of-rows
  const obj = JSON.parse(txt);
  if (obj && obj.type==='FeatureCollection' && Array.isArray(obj.features)) {
    return obj.features.filter(f=>{
      if (!f?.geometry?.coordinates) return false;
      const [lon,lat]=f.geometry.coordinates;
      return isInBox(Number(lon),Number(lat));
    });
  }
  if (Array.isArray(obj)) {
    return obj.map(rowToFeature).filter(Boolean);
  }
  return [];
}

async function main(){
  await fs.mkdir(OUTDIR,{recursive:true});
  const files = (await fs.readdir(argv.in))
    .filter(n=>/\.(csv|txt|json)$/i.test(n))
    .map(n=>path.join(argv.in,n));

  const buckets = new Map(); // month -> Map(dedupKey -> feature)
  for (const f of files){
    console.log('Reading', path.basename(f));
    const feats = await loadOne(f);
    for (const ft of feats){
      const mk = toMonthKey(ft.properties.acq_date);
      if (!buckets.has(mk)) buckets.set(mk, new Map());
      const dedup = featureKey(ft);
      buckets.get(mk).set(dedup, ft);
    }
  }

  const months = [...buckets.keys()].sort();
  for (const mk of months){
    const fc = { type:'FeatureCollection', features:[...buckets.get(mk).values()] };
    const out = path.join(OUTDIR, `CA-${mk}.geojson`);
    await fs.writeFile(out, JSON.stringify(fc));
    console.log(`💾 Wrote ${out}  features=${fc.features.length}`);
  }
}
main().catch(e=>{ console.error(e); process.exit(1); });
