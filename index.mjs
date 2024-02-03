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
[].forEach.call(queryForm.querySelectorAll('input[type="submit"]'), (e) => { e.disabled = false; });

function tabulate(results) {
  let table = document.createElement('table');

  let header = document.createElement('tr');
  for(let col of results.schema.fields) {
    let cell = document.createElement('th');
    cell.innerText = col.name;
    header.appendChild(cell);
  }
  table.appendChild(header);

  for(let row of results) {
    let tr = document.createElement('tr');
    for(let [col, val] of row) {
      let cell = document.createElement('td');
      cell.innerText = val;
      tr.appendChild(cell);
    }
    table.appendChild(tr);
  }

  document.getElementById('table').innerText = '';
  document.getElementById('table').appendChild(table);
}

function zeroPad(num, digits) {
  return ('' + num).padStart(digits, '0');
}

const dateFormatters = {
  'year': (r) => zeroPad(r['year'], 4) + '-01-01',
  'month': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-01',
  'day': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-' + zeroPad(r['day'], 2),
  'hour': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-' + zeroPad(r['day'], 2) + 'T' + zeroPad(r['hour'], 2) + ':00:00Z',
  'minute': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-' + zeroPad(r['day'], 2) + 'T' + zeroPad(r['hour'], 2) + ':' + zeroPad(r['minute'], 2) + ':00Z',
  'second': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-' + zeroPad(r['day'], 2) + 'T' + zeroPad(r['hour'], 2) + ':' + zeroPad(r['minute'], 2) + ':' + zeroPad(r['second'], 2) + 'Z',
  'microseconds': (r) => zeroPad(r['year'], 4) + '-' + zeroPad(r['month'], 2) + '-' + zeroPad(r['day'], 2) + 'T' + zeroPad(r['hour'], 2) + ':' + zeroPad(r['minute'], 2) + ':' + zeroPad(r['second'], 2) + '.' + zeroPad(r['microseconds'], 6) + 'Z',
};

// https://vega.github.io/vega-lite/docs/timeunit.html
const vlTimeUnits = {
  'year': 'year',
  'month': 'yearmonth',
  'day': 'yearmonthdate',
  'hour': 'yearmonthdatehours',
  'minute': 'yearmonthdatehoursminutes',
  'second': 'yearmonthdatehoursminutesseconds',
  'microseconds': 'yearmonthdatehoursminutessecondsmilliseconds',
};

function plot(results) {
  let allFields = {};
  for(let col of results.schema.fields) {
    allFields[col.name] = true;
  }
  let resolution = null;
  if(allFields['value']) {
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
  }
  if(resolution === null) {
    document.getElementById('graph').innerText = '';
    return;
  }

  let formatter = dateFormatters[resolution];
  let data = [];
  for(let row of results) {
    let entry = {label: 'thing', date: formatter(row), value: Number(row['value'])};
    data.push(entry);
  }
  console.log(data);

  var vlSpec = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "description": "Time series",
    "width": "container",
    "height": "container",
    "data": {
      values: data,
    },
    "mark": {
      "type": "line",
      "point": true
    },
    "encoding": {
      "x": {"field": "date", "type": "temporal", "utc": true, "timeUnit": vlTimeUnits[resolution]},
      "y": {"field": "value", "type": "quantitative"},
      "color": {"field": "label", "type": "nominal"}
    }
  };
  vegaEmbed('#graph', vlSpec);
}

async function queryDB(db, query) {
  document.getElementById('error').style.display = 'none';
  document.getElementById('error').innerText = '';

  try {
    const conn = await db.connect();
    let results = await conn.query(query);
    console.log('results:', results);

    tabulate(results);

    plot(results);

    await conn.close();
  } catch(e) {
    console.error(e);
    document.getElementById('table').innerText = '';
    document.getElementById('graph').innerText = '';
    document.getElementById('error').style.display = '';
    document.getElementById('error').innerText = 'Error: ' + e;
  }
}
