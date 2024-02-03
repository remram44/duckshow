duckshow
========

This is a time-series plotting library. It is frontend-only thanks to [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm).

It allows you to run queries against large time-series datasets and plot the results, similarly to what [Grafana](https://github.com/grafana/grafana) does. Contrary to Grafana however, it does not require a server component, so you can host you interactive dashboard on static files storage, like S3 or GitHub Pages, without losing the ability to query, select, and pan around your data.

## Status

This is still in early development. The basic infrastructure is here, but the plotting capabilities are limited.
