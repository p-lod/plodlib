# plodlib

`plodlib` is a Python library for querying the P-LOD RDF triplestore for data about the archaeological site of Pompeii.

## What it does

- Wraps common triplestore queries behind a Python API.
- Provides a `PLODResource` object for working with identifiers such as places, concepts, and predicates.
- Supports command-line use via `python3 -m plodlib`.

## Installation

Install directly from GitHub:

```bash
python3 -m pip install git+https://github.com/p-lod/plodlib
```

## Quick Start

### Command line

```bash
python3 -m plodlib --method depicts_concepts pompeii
```

This returns concepts recorded by the Pompeii Artistic Landscape Project for Pompeii.

### Python

```python
import plodlib

resource = plodlib.PLODResource("pompeii")
print(resource.label)
print(resource.depicts_concepts())
```

## Common Methods

Methods demonstrated in this repository include:

- `spatial_hierarchy_up()`
- `spatial_children()`
- `depicts_concepts()`
- `depicted_where()`
- `instances_of()`
- `used_as_predicate_by()`

See [plodlib_examples.py](plodlib_examples.py) for broader usage.

## Return values

Methods that wrap a SPARQL query return a `list[dict]` (or, for single-value accessors, a plain string/int/None). Every value inside those dicts is a native Python type — no `rdflib` objects, no pandas `NaN`:

| SPARQL term | Python type |
|---|---|
| URIRef, BNode | `str` |
| Literal (plain string) | `str` |
| Literal (`xsd:integer`, etc.) | `int` / `float` / `bool` (via `Literal.toPython()`) |
| Unbound / missing | `None` |

Geojson fields are returned as JSON-encoded strings (consumers parse them as needed). The `geojson` property on `PLODResource` is also a JSON string.

Every return value is safe to pass directly to `json.dumps` or to return from a Flask / FastAPI endpoint.

## Project Status

This library is in an early stage. APIs and behavior may evolve as the triplestore and use cases develop.

## Contributing

When adding a method that returns SPARQL results, build a `pd.DataFrame` from the query, then return `_records(df)` (defined at the top of [plodlib/__init__.py](plodlib/__init__.py)). Do not call `df.to_dict()` or `json.loads(df.to_json(...))` directly — `_records` is what enforces the type contract above. For single-column results, use `[_coerce_term(v) for v in series]`.