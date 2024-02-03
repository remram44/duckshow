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

async function queryDB(db, query) {
  try {
    const conn = await db.connect();
    let res = await conn.query(query);

    let table = document.createElement('table');

    let header = document.createElement('tr');
    for(let col of res.schema.fields) {
      let cell = document.createElement('th');
      cell.innerText = col.name;
      header.appendChild(cell);
    }
    table.appendChild(header);

    console.log('results:', res);
    for(let batch of res) {
      let row = document.createElement('tr');
      for(let [col, val] of batch) {
        let cell = document.createElement('td');
        cell.innerText = val;
        row.appendChild(cell);
      }
      table.appendChild(row);
    }
    await conn.close();

    document.getElementById('viz').innerText = '';
    document.getElementById('viz').appendChild(table);
  } catch(e) {
    console.error(e);
    document.getElementById('viz').innerText = '';
    let err = document.createElement('p');
    err.className = 'error';
    err.innerText = 'Error: ' + e;
    document.getElementById('viz').appendChild(err);
  }
}
