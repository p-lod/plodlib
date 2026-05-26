Access the P-LOD Triplestore

Install using python3 -m pip install git+https://github.com/p-lod/plodlib

From command line try: python3 -m plodlib --method depicts_concepts pompeii

This will show a list of all the concepts that the Pompeii Artistic Landscape Project has recorded at Pompeii.

All of this is in early stages!

## Optional caching

`plodlib` can cache SPARQL query results on disk to speed up repeat lookups
(useful for classroom / demo workloads where the same URLs are hit many times
in a short window). Caching is **off by default** — importing the library
creates no files. Enable it explicitly at startup:

```python
import plodlib
plodlib.enable_cache()                # defaults: ./cache, 100 MB, 30-minute TTL
# or, with explicit args:
plodlib.enable_cache(directory='/var/lib/plod/cache', size_mb=200, ttl_sec=1800)
```

Backed by `diskcache` (SQLite, multi-process safe — gunicorn workers can share
the same cache directory). Disable with `plodlib.disable_cache()`. Invalidation
on a triplestore re-ingest: either wait out the TTL or `rm -rf` the cache dir.
