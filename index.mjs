import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.1-dev106.0/+esm';

let database = (async () => {
  try {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'})
    );

    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);

    console.log("DuckDB loaded");
    return db;
  } catch(e) {
    console.error(e);
    return null;
  }
})();

let queryForm = document.getElementById('query');
queryForm.addEventListener('submit', function(e) {
  e.preventDefault();

  let source = queryForm.elements['source'].value;
  let query = queryForm.elements['query'].value;

  query = (' ' + query).replace(/([^$])\${DATA}/, '$1' + source).substring(1).replace(/\$\$/, '$$');

  database.then((db) => {
    return queryDB(db, query);
  });
});

function tabulate(results) {
  let table = document.createElement('table');

  let header = document.createElement('tr');
  for(let col of results.schema.fields) {
    let cell = document.createElement('th');
    cell.innerText = col.name;
    header.appendChild(cell);
  }
  table.appendChild(header);

  for(let batch of results) {
    let row = document.createElement('tr');
    for(let [col, val] of batch) {
      let cell = document.createElement('td');
      cell.innerText = val;
      row.appendChild(cell);
    }
    table.appendChild(row);
  }
  return table;
}

function plot(results, resolution) {
  let graph = document.createElement('p');
  graph.innerText = 'Graph here, resolution=' + resolution;
  return graph;
}

async function queryDB(db, query) {
  try {
    const conn = await db.connect();
    let results = await conn.query(query);
    console.log('results:', results);

    let table = tabulate(results);

    let allFields = {};
    for(let col of results.schema.fields) {
      allFields[col.name] = true;
    }
    let resolution = null;
    if(allFields['year'] && allFields['month'] && allFields['day']) {
      if(allFields['hour']) {
        if(allFields['minute']) {
          if(allFields['second']) {
            if(allFields['microseconds']) {
              resolution = 'microseconds';
            } else {
              resolution = 'second';
            }
          } else {
            resolution = 'minute';
          }
        } else {
          resolution = 'hour';
        }
      } else {
        resolution = 'day';
      }
    }

    let graph = null;
    if(resolution) {
      graph = plot(results, resolution);
    }

    await conn.close();

    document.getElementById('viz').innerText = '';
    document.getElementById('viz').appendChild(table);
    if(graph) {
      document.getElementById('viz').appendChild(graph);
    }
  } catch(e) {
    console.error(e);
    document.getElementById('viz').innerText = '';
    let err = document.createElement('p');
    err.className = 'error';
    err.innerText = 'Error: ' + e;
    document.getElementById('viz').appendChild(err);
  }
}
