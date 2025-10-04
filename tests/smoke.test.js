import { readFileSync } from 'fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

const checks = [
  {
    description: 'Uses Cesium Viewer with ellipsoid terrain provider',
    test: () => html.includes('terrainProvider: new Cesium.EllipsoidTerrainProvider()')
  },
  {
    description: 'Adds ArcGIS World Imagery basemap',
    test: () => html.includes('ArcGisMapServerImageryProvider') && html.includes('World_Imagery')
  },
  {
    description: 'Camera flies to California coordinates',
    test: () => html.includes('Cesium.Cartesian3.fromDegrees(-119.5, 37.2, 1200000)')
  }
];

const failures = checks.filter(({ test }) => {
  try {
    return !test();
  } catch (error) {
    console.error(error);
    return true;
  }
});

if (failures.length > 0) {
  console.error(`Smoke test failed for ${failures.length} check(s):`);
  for (const { description } of failures) {
    console.error(` - ${description}`);
  }
  process.exitCode = 1;
} else {
  console.log('All Cesium smoke checks passed.');
}
