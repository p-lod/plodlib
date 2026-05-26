# Module for accessing information about P-LOD resource for rendering in PALP
# Define a function

from string import Template

import json
import requests
from requests.adapters import HTTPAdapter

import diskcache
import rdflib as rdf
from rdflib.plugins.stores import sparqlstore


_ENDPOINT = "http://52.170.134.25:3030/plod_endpoint/query"
_CONNECT_TIMEOUT_SECONDS = 10   # generous for slow networks
_READ_TIMEOUT_SECONDS = 120     # heavy SPARQL queries can take a while


class _TimeoutAdapter(HTTPAdapter):
    # requests.Session has no default-timeout knob; without this, a hung
    # triplestore would block the calling worker indefinitely. Per-request
    # timeouts passed to .get/.post still win (setdefault leaves them alone).
    def __init__(self, *args,
                 timeout=(_CONNECT_TIMEOUT_SECONDS, _READ_TIMEOUT_SECONDS),
                 **kwargs):
        self._timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        kwargs.setdefault('timeout', self._timeout)
        return super().send(request, **kwargs)


# One Session per process (gunicorn worker). Pools HTTP keep-alive
# connections so subsequent SPARQL queries skip the TCP handshake.
_SESSION = requests.Session()
_SESSION.mount('http://', _TimeoutAdapter(pool_connections=32, pool_maxsize=32))
_SESSION.mount('https://', _TimeoutAdapter(pool_connections=32, pool_maxsize=32))



def luna_tilde_val(luna_urn):
  if luna_urn.startswith("urn:p-lod:id:luna_img_PALP"):
    tilde_val = "14"

  if luna_urn.startswith("urn:p-lod:id:luna_img_PPM"):
    tilde_val = "16"

  return tilde_val

def add_luna_info(row):
  
  img_src = None #default if no URLs present (probably means LUNA doesn't have image though triplestore thinks it does)
  img_description = None

  if row['urn'].startswith("urn:p-lod:id:luna_img_PALP"):
    tilde_val = "14"

  if row['urn'].startswith("urn:p-lod:id:luna_img_PPM"):
    tilde_val = "16"
  
  luna_json = json.loads(requests.get(f'https://umassamherst.lunaimaging.com/luna/servlet/as/fetchMediaSearch?mid=umass~{tilde_val}~{tilde_val}~{row["l_record"]}~{row["l_media"]}&fullData=true').text)
  
  if len(luna_json):

    img_attributes = json.loads(luna_json[0]['attributes'])

    if 'image_description_english' in img_attributes.keys():
      img_description = img_attributes['image_description_english']
    else:
      try:
        if   tilde_val == '14':
          img_description = json.loads(luna_json[0]['fieldValues'])[2]['value']
        elif tilde_val == '16':
          img_description = json.loads(luna_json[0]['fieldValues'])[1]['value']
        else:
          img_description = f"unrecognized collection {tilde_val}"
      except:
        img_description = "Trying to get description failed"
    

    if 'urlSize4' in img_attributes.keys(): # use size 4, sure, but only if there's nothing else
      img_src = img_attributes['urlSize4']
    if 'urlSize2' in img_attributes.keys(): # preferred
      img_src = img_attributes['urlSize2']
    elif 'urlSize3' in img_attributes.keys():
      img_src = img_attributes['urlSize3']
    else:
      img_src = img_attributes['urlSize1']

  row['l_img_url'] = img_src
  row['l_current_description'] = img_description

  return row


def _coerce_term(v):
    # rdflib URIRef/BNode -> str, Literal -> its xsd-typed Python value,
    # None or NaN -> None. Leaves already-plain Python values untouched.
    if v is None:
        return None
    if isinstance(v, rdf.term.Literal):
        try:
            return v.toPython()
        except Exception:
            return str(v)
    if isinstance(v, (rdf.term.URIRef, rdf.term.BNode)):
        return str(v)
    if isinstance(v, float) and v != v:
        return None
    return v


def _records(result):
    # Convert a rdflib SPARQL Result to a list of plain-Python dicts.
    cols = [str(v) for v in result.vars]
    return [{c: _coerce_term(row[i]) for i, c in enumerate(cols)}
            for row in result]


def _graph(return_format='json'):
    # return_format=None lets the store use its default (xml/turtle), which
    # is what DESCRIBE / CONSTRUCT queries (rdf_describe, see_also) want.
    kwargs = {
        'query_endpoint': _ENDPOINT,
        'context_aware': False,
        'session': _SESSION,
    }
    if return_format is not None:
        kwargs['returnFormat'] = return_format
    return rdf.Graph(sparqlstore.SPARQLStore(**kwargs))


# Optional on-disk SPARQL result cache. Off until enable_cache() is called.
_CACHE = None
_CACHE_TTL_SEC = 30 * 60   # default; overridden by enable_cache()


def enable_cache(directory='./cache', size_mb=100, ttl_sec=30 * 60):
    """Enable on-disk SPARQL result caching for this process.

    Call once at startup (e.g. from a FastAPI lifespan handler or top-of-main).
    Subsequent calls reconfigure: the existing cache is closed and replaced.

    directory: where to put the SQLite cache files (created if missing).
    size_mb:   hard upper bound; diskcache evicts LRU when exceeded.
    ttl_sec:   per-entry TTL. Defaults to 30 minutes.
    """
    global _CACHE, _CACHE_TTL_SEC
    if _CACHE is not None:
        _CACHE.close()
    _CACHE = diskcache.Cache(directory, size_limit=size_mb * 1024 * 1024)
    _CACHE_TTL_SEC = ttl_sec


def disable_cache():
    """Turn caching off and release the cache files."""
    global _CACHE
    if _CACHE is not None:
        _CACHE.close()
        _CACHE = None


def _cached_select(query_string):
    if _CACHE is None:
        return _records(_graph().query(query_string))
    key = ('select', query_string)
    hit = _CACHE.get(key)
    if hit is not None:
        return hit
    records = _records(_graph().query(query_string))
    _CACHE.set(key, records, expire=_CACHE_TTL_SEC)
    return records


def _cached_describe(query_string, return_format='turtle'):
    if _CACHE is None:
        result = _graph(return_format=None).query(query_string).serialize(format=return_format)
        return result.decode('utf-8') if isinstance(result, bytes) else result
    key = ('describe', return_format, query_string)
    hit = _CACHE.get(key)
    if hit is not None:
        return hit
    result = _graph(return_format=None).query(query_string).serialize(format=return_format)
    if isinstance(result, bytes):
        result = result.decode('utf-8')
    _CACHE.set(key, result, expire=_CACHE_TTL_SEC)
    return result


# Define a class
class PLODResource(object):

    def __init__(self,identifier = 'pompeii'):

        # could default to 'pompeii' along with its info?
        if identifier == None:
          self.identifier = None
          return

        # __init__'s SPARQL stays out of the cache: it builds self._predicates
        # using str() on each term, while _cached_select() would coerce typed
        # Literals via Literal.toPython() (xsd:integer -> int, etc.). Routing
        # this through the cache would change the type contract that
        # p-lod-api's /id/{id} route serializes directly to JSON.
        g = _graph()

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT ?p ?o WHERE { p-lod:$identifier ?p ?o . }
""")

        results = g.query(qt.substitute(identifier = identifier))

        # Bucket (predicate, object) pairs into a dict keyed by predicate URI.
        predicates = {}
        for p, o in results:
            predicates.setdefault(str(p), []).append(str(o))

        self._identifier_parameter = identifier
        self.identifier = identifier if predicates else None

        def _strip(s): return s.replace('urn:p-lod:id:', '')
        def _first(uri): return (predicates.get(uri) or [None])[0]

        # rdf_type preserves the legacy quirk: scalar if one match, list if many, None if absent.
        rdf_types = [_strip(v) for v in predicates.get('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', [])]
        self.rdf_type = rdf_types[0] if len(rdf_types) == 1 else (rdf_types or None)

        self.label        = _first('http://www.w3.org/2000/01/rdf-schema#label')
        self.broader      = _first('urn:p-lod:id:broader')
        self.p_in_p_url   = _first('urn:p-lod:id:p-in-p-url')
        self.wikidata_url = _first('urn:p-lod:id:wikidata-url')

        # best_images is only set when the predicate exists (callers may rely on AttributeError).
        best = predicates.get('urn:p-lod:id:best-image')
        if best is not None:
            self.best_images = [_strip(v) for v in best]

        # Eager-parse geojson if the resource has the predicate; otherwise the property
        # falls through to compute a virtual geojson on access.
        self._eager_geojson = None
        geo_vals = predicates.get('urn:p-lod:id:geojson')
        if geo_vals:
            try:
                self._eager_geojson = json.loads(geo_vals[0])
            except (ValueError, TypeError):
                pass

        # Keep the raw predicate -> [object, ...] mapping accessible for
        # callers that need a full dump of the resource (e.g. p-lod-api's
        # /id/{id} endpoint).
        self._predicates = predicates

    def conceptual_ancestors(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?label WHERE { 
  p-lod:$identifier p-lod:broader* ?urn .
    ?urn a p-lod:concept  .
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    }""")
        return _cached_select(qt.substitute(identifier = identifier))

    def conceptual_descendants(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?label WHERE {
       ?urn p-lod:broader+  p-lod:$identifier.
              
      OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
                      }""")
        return _cached_select(qt.substitute(identifier = identifier))

    def conceptual_children(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?label WHERE {
      ?urn p-lod:broader p-lod:$identifier .
      OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
                      }""")
        return _cached_select(qt.substitute(identifier = identifier))

    
    def gather_images(self):
      # return format is urn (of image), depicts_urn, depicts_type, depicts_label, is_best_image, l_record, l_media, l_batch, l_description, geojson
      if self.rdf_type == 'concept':

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?urn ?label ?best_image ?l_record ?l_media ?l_batch ?l_description ?l_img_url ?feature WHERE {
   
    BIND ( p-lod:$identifier AS ?identifier )
   
    { ?component p-lod:depicts ?identifier . }
    UNION
    { ?component p-lod:depicts/p-lod:broader+ ?identifier }

    OPTIONAL { ?component p-lod:is-part-of+/p-lod:created-on-surface-of ?feature .
               ?feature a p-lod:feature .
               OPTIONAL { ?feature p-lod:geojson ?geojson } }

               BIND ( true AS ?best_image)
               ?component p-lod:best-image ?urn .
               ?urn p-lod:x-luna-record-id   ?l_record .
               ?urn p-lod:x-luna-media-id    ?l_media .
               ?urn p-lod:x-luna-batch-id    ?l_batch . 
               ?urn p-lod:x-luna-description ?l_description .
               ?urn p-lod:x-luna-url-3       ?l_img_url .
               OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }

} ORDER BY DESC(?best_image)""")

        return _cached_select(qt.substitute(identifier = identifier))

      elif self.rdf_type in ['space','property','insula','region']:

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?urn ?label ?l_record ?l_media ?l_batch ?l_img_url ?feature ?l_description WHERE {

BIND ( p-lod:$identifier AS ?identifier )
{
?urn p-lod:depicts ?feature .
?feature a p-lod:feature .
?identifier ^p-lod:spatially-within+ ?feature .
}
UNION 
{

?identifier ^p-lod:spatially-within*/^p-lod:created-on-surface-of*/^p-lod:is-part-of* ?component .
?component p-lod:best-image ?urn .

?component p-lod:is-part-of+/p-lod:created-on-surface-of ?feature .
?feature a p-lod:feature .
}

?urn p-lod:x-luna-record-id ?l_record .
?urn p-lod:x-luna-media-id  ?l_media .
?urn p-lod:x-luna-batch-id  ?l_batch .
?urn p-lod:x-luna-description ?l_description .
?urn p-lod:x-luna-url-3       ?l_img_url .

OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label}

}""")
        
        return _cached_select(qt.substitute(identifier = identifier))

      elif self.rdf_type in ['feature']:

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?urn ?label ?l_record ?l_media ?l_batch ?l_img_url ?feature ?l_description WHERE {

BIND ( p-lod:$identifier AS ?identifier )
BIND ( p-lod:$identifier AS ?feature )
{
?urn p-lod:depicts ?identifier .
}
UNION 
{
#?component p-lod:is-part-of+/p-lod:created-on-surface-of ?identifier .
?identifier ^p-lod:created-on-surface-of/^p-lod:is-part-of+ ?component .
?component p-lod:best-image ?urn .

}

?urn p-lod:x-luna-record-id ?l_record .
?urn p-lod:x-luna-media-id  ?l_media .
?urn p-lod:x-luna-batch-id  ?l_batch .
?urn p-lod:x-luna-description ?l_description .
?urn p-lod:x-luna-url-3       ?l_img_url .

OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label}
}""")
        
        return _cached_select(qt.substitute(identifier = identifier))
      else:
        return self.images_from_luna or []
      

    @property
    def geojson(self):
      if self._eager_geojson is not None:
        return self._eager_geojson

      # No direct geojson predicate (or it was malformed) — build a virtual one.
      try:
        if self.rdf_type == 'pompeian-wall-painting-style':
          as_object_list = self.as_object(add_predicate='geojson', set_predicate='has-pompeian-wall-painting-style')
          geojson_list = [d["added"] for d in as_object_list]
          if len(geojson_list):
            my_geojson_d = {"type": "FeatureCollection", "features": []}
            for g in geojson_list:
              try:
                my_geojson_d['features'].append(json.loads(g))
              except:
                print(f"Couldn't parse {g}")
            my_geojson = my_geojson_d
          else:
            my_geojson = None
            print("Failed to parse geojson")

        elif self.rdf_type == 'space-characterization':
          as_object_list = self.as_object(add_predicate='geojson', set_predicate='has-space-characterization')
          geojson_list = [d["added"] for d in as_object_list]
          if len(geojson_list):
            my_geojson_d = {"type": "FeatureCollection", "features": []}
            for g in geojson_list:
              try:
                my_geojson_d['features'].append(json.loads(g))
              except:
                print(f"Couldn't parse {g}")
            my_geojson = my_geojson_d
          else:
            my_geojson = None
            print("Failed to parse geojson")

        # note that depicted_where will return an empty list so check length after calling
        else:
          dw_d = self.depicted_where(level_of_detail='space')
          if len(dw_d):
            my_geojson_d = {"type": "FeatureCollection", "features": []}
            for g in dw_d:
              if g['geojson'] != 'None':
                my_geojson_d['features'].append(json.loads(g['geojson']))
            my_geojson = my_geojson_d
          else:
            my_geojson = None
            print("Failed to parse geojson")
      except:
        print("Error after no geojson found.")
        return []

      return my_geojson
    

    def as_predicate(self):

        identifier = self.identifier
        if identifier == None:
            return []
        
        qt = Template("""
        PREFIX p-lod: <urn:p-lod:id:>
        SELECT ?subject ?object WHERE 
        { ?subject p-lod:$identifier ?object . }
        ORDER BY ?subject ?object LIMIT 15000""")
                      
        return _cached_select(qt.substitute(identifier = identifier))

    def as_object(self, set_predicate = None ,
                  add_predicate = None,
                  broader = False ):
        

        identifier = self.identifier
        if identifier == None:
            return []
        
        set_predicate_str = '?predicate'
        if set_predicate:
           set_predicate_str = f'p-lod:{set_predicate}'

        add_predicate_str = ''
        if add_predicate:
           add_predicate_str = f"OPTIONAL {{?subject p-lod:{add_predicate} ?added}}"

        broader_union_str = ''
        if broader:
          broader_union_str = f"""
          UNION
          {{
          ?subject {set_predicate_str} ?broader_start .
          ?broader_start p-lod:broader+ p-lod:{identifier} .
          }}
           """

        qt = Template("""
        PREFIX p-lod: <urn:p-lod:id:>
        SELECT ?subject ?predicate ?added WHERE 
        {
          {
          ?subject $set_predicate_str p-lod:$identifier .
          }
          $broader_union_str
                      
          $add_predicate_str 

          }
        ORDER BY ?subject ?predicate LIMIT 15000""")

        
        query_str = qt.substitute(identifier = identifier,
                                  broader_union_str = broader_union_str,
                                  set_predicate_str = set_predicate_str ,
                                  add_predicate_str = add_predicate_str )

        print(query_str)

        records = _cached_select(query_str)
        if add_predicate is None:
            # Build a fresh list of dicts without 'added'; never mutate the
            # cached object (diskcache returns fresh deserialized copies, but
            # avoid any chance of relying on that).
            records = [{k: v for k, v in r.items() if k != 'added'} for r in records]
        return records

    ## get_predicate_values ##
    def get_predicate_values(self,predicate = 'urn:p-lod:id:label'):
        # predicate should be a fully qualified url or urn as a string.
        # returns json array of keyed dictionaries.



        identifier = self.identifier
        if identifier == None:
            return []

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT ?values WHERE { p-lod:$identifier <$predicate> ?values . }
""")

        # SELECT clause is just `?values`, so each cached record is {'values': X}.
        return [r['values'] for r in _cached_select(qt.substitute(identifier = identifier, predicate = predicate))]


    ## depicts_concepts ##
    def depicts_concepts(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>

SELECT ?urn ?label (COUNT(*) AS ?count) (GROUP_CONCAT(DISTINCT ?within_depicts ; separator = '||') AS ?within_spatial_units_depict) WHERE {

  BIND ( p-lod:$identifier AS ?identifier )

  ?identifier ^p-lod:spatially-within*/^p-lod:created-on-surface-of*/^p-lod:is-part-of* ?component .
  ?component a p-lod:artwork-component .
  ?component p-lod:depicts ?urn .

  OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }

  OPTIONAL {
    ?identifier a p-lod:property .
    ?identifier ^p-lod:spatially-within+ ?within_depicts .
    ?within_depicts a p-lod:space .
    ?component p-lod:is-part-of*/p-lod:created-on-surface-of/p-lod:spatially-within+ ?within_depicts .
  }

  OPTIONAL {
    ?identifier a p-lod:space .
    ?identifier ^p-lod:spatially-within+ ?within_depicts .
    ?within_depicts a p-lod:feature .
    ?component p-lod:is-part-of*/p-lod:created-on-surface-of ?within_depicts .
  }

  OPTIONAL {
    ?identifier a p-lod:feature .
    ?component p-lod:is-part-of*/p-lod:created-on-surface-of ?identifier .
    ?component p-lod:best-image ?within_depicts .
  }

} GROUP BY ?urn ?label ORDER BY ?urn""")

        return _cached_select(qt.substitute(identifier = identifier))


    ## depicted_where ##
    def depicted_where(self, level_of_detail = 'feature'):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?type ?label ?within ?best_image ?l_record ?l_media ?l_batch ?l_description ?l_img_url ?geojson  WHERE {
    
    BIND ( p-lod:$identifier AS ?identifier )
   
    { ?component p-lod:depicts ?identifier }
    UNION
    { ?identifier ^p-lod:broader+/^p-lod:depicts ?component }
    UNION
    { ?identifier ^p-lod:has-pompeian-wall-painting-style/^p-lod:created-on-surface-of/^p-lod:is-part-of+ ?component }
    
    ?component p-lod:is-part-of+/p-lod:created-on-surface-of/p-lod:spatially-within* ?urn .
    ?urn a p-lod:$level_of_detail
    OPTIONAL { ?urn a ?type }
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?urn p-lod:spatially-within ?within }
    OPTIONAL { ?urn p-lod:geojson ?geojson }
 
    OPTIONAL { ?component p-lod:has-action ?action . }
    OPTIONAL { ?component p-lod:has-color  ?color . }
    OPTIONAL { ?component p-lod:best-image ?best_image .
               ?best_image p-lod:x-luna-record-id ?l_record .
               ?best_image p-lod:x-luna-media-id  ?l_media .
               ?best_image p-lod:x-luna-batch-id  ?l_batch .
               ?best_image p-lod:x-luna-description ?l_description .
               ?best_image p-lod:x-luna-url-3    ?l_img_url .
               }
} ORDER BY ?within""")

       # identifier = what you're looking for, level_of_detail = spatial resolution at which to list results
        return _cached_select(qt.substitute(identifier = identifier, level_of_detail = level_of_detail))

    def rdf_describe(self):
        identifier = self.identifier
        q = f"""
PREFIX p-lod: <urn:p-lod:id:>
DESCRIBE p-lod:{identifier}"""
        return _cached_describe(q, return_format='turtle')

    def see_also(self):
      identifier = self.identifier
      qt = Template("""
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX p-lod: <urn:p-lod:id:>

CONSTRUCT {
?s rdfs:seeAlso ?o .
?s ?sa_predicate ?o .

}

WHERE { BIND(p-lod:$identifier AS ?s )
    ?s ?sa_predicate ?o .
      ?sa_predicate owl:equivalentProperty rdfs:seeAlso .
}
      """)
      return _cached_describe(qt.substitute(identifier = identifier), return_format='turtle')

   ## spatial_ancestors ##
    def spatial_ancestors(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?type ?label ?geojson WHERE { 
  { p-lod:$identifier p-lod:is-part-of*/p-lod:created-on-surface-of* ?feature .
    ?feature p-lod:spatially-within* ?urn .
    ?feature a p-lod:feature  .
    OPTIONAL { ?urn a ?type }
    OPTIONAL { ?urn p-lod:geojson ?geojson }
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    }
    UNION
    { p-lod:$identifier p-lod:spatially-within* ?urn  . 
      OPTIONAL { ?urn a ?type }
      OPTIONAL { ?urn p-lod:geojson ?geojson }
      OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    }
  }""")

        return _cached_select(qt.substitute(identifier = identifier))


## spatial_children ##
    def spatial_children(self, rdf_type: str = 'all', exclude_rdf_type: str = ''):

        identifier = self.identifier
        if rdf_type == 'all':
            rdf_type = ''
        else:
            rdf_type = f"?urn a p-lod:{rdf_type} ."

        if exclude_rdf_type != '':
            exclude_rdf_type = f"FILTER NOT EXISTS {{ ?urn a p-lod:{exclude_rdf_type} . }}"

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?type ?label ?geojson WHERE {
      ?urn p-lod:spatially-within p-lod:$identifier .
      $rdf_type
      $exclude_rdf_type
      OPTIONAL { ?urn a ?type }
      OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
      OPTIONAL { ?urn p-lod:geojson ?geojson }
                      }""")
        return _cached_select(qt.substitute(identifier = identifier, rdf_type = rdf_type, exclude_rdf_type = exclude_rdf_type))

## spatially_within
    @property
    def spatially_within(self):

        identifier = self.identifier

        qt = Template("""
    PREFIX p-lod: <urn:p-lod:id:>
    SELECT ?urn ?type ?label ?geojson WHERE {

        p-lod:$identifier p-lod:spatially-within ?urn  . 

        ?urn a ?type .
        OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label  }
        ?urn p-lod:geojson ?geojson .
        
      } LIMIT 1""")
        return _cached_select(qt.substitute(identifier = identifier))
     

## in_region ##
    @property
    def in_region(self):

        identifier = self.identifier

        qt = Template("""
    PREFIX p-lod: <urn:p-lod:id:>
    SELECT ?urn ?type ?label ?geojson WHERE {

        p-lod:$identifier p-lod:spatially-within+ ?urn  . 

        ?urn a ?type .
        ?urn a p-lod:region .
        OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label  }
        ?urn p-lod:geojson ?geojson .
        
      } LIMIT 1""")
        return _cached_select(qt.substitute(identifier = identifier))


## instances_of ##
    def instances_of(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?urn ?type ?label ?geojson (COUNT(DISTINCT ?component) AS ?depiction_count) WHERE
{   ?urn rdf:type/rdfs:subClassOf* p-lod:$identifier .

    OPTIONAL { ?urn a ?type }
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?urn p-lod:geojson ?geojson }
    OPTIONAL { ?component p-lod:depicts ?urn ;
               a p-lod:artwork-component . }
 } GROUP BY ?urn ?type ?label ?geojson ORDER BY ?urn""")
        return _cached_select(qt.substitute(identifier = identifier))


## used_as_predicate_by ##
    def used_as_predicate_by(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?subject ?object WHERE { ?subject p-lod:$identifier ?object}""")
        return _cached_select(qt.substitute(identifier = identifier))


## narrower ##
    @property
    def narrower(self):

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?label ?is_depicted WHERE {
    ?urn p-lod:broader+ p-lod:$identifier .
    ?urn rdfs:label ?label .

    BIND ( IF(EXISTS { ?x p-lod:depicts ?urn }, "true", "false") AS ?is_depicted )
  } ORDER BY ?label

""")
        
        return _cached_select(qt.substitute(identifier = identifier))


## images_from_luna ##
    @property
    def images_from_luna(self):


        identifier = self.identifier

        qt = Template("""
        PREFIX p-lod: <urn:p-lod:id:>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT ?urn ?label ?l_record ?l_media ?l_batch ?l_description
        WHERE {

       {?urn p-lod:depicts p-lod:$identifier .}
        UNION
        {p-lod:$identifier p-lod:best-image ?urn}

        ?urn a p-lod:luna-image .
        ?urn rdfs:label ?label .
        ?urn p-lod:x-luna-record-id ?l_record .
        ?urn p-lod:x-luna-media-id ?l_media .
        ?urn p-lod:x-luna-batch-id ?l_batch .
        ?urn p-lod:x-luna-description ?l_description .
         }""")
        return [add_luna_info(dict(r)) for r in _cached_select(qt.substitute(identifier = identifier))]

    def compare_depicts(self, right):
      right_r = PLODResource(right)

      left_urns = {r['urn'] for r in self.depicts_concepts() if 'urn' in r}
      right_urns = {r['urn'] for r in right_r.depicts_concepts() if 'urn' in r}

      return {"left_urn": f"urn:p-lod:id:{self.identifier}",
              "difference_left": list(left_urns - right_urns),
              "intersection": list(left_urns & right_urns),
              "difference_right": list(right_urns - left_urns),
              "right_urn": f"urn:p-lod:id:{right_r.identifier}"}

    def compare_depicted(self, right, level_of_detail='space'):
      right_r = PLODResource(right)

      left_urns = {r['urn'] for r in self.depicted_where(level_of_detail) if 'urn' in r}
      right_urns = {r['urn'] for r in right_r.depicted_where(level_of_detail) if 'urn' in r}

      return {"left_urn": f"urn:p-lod:id:{self.identifier}",
              "difference_left": list(left_urns - right_urns),
              "intersection": list(left_urns & right_urns),
              "difference_right": list(right_urns - left_urns),
              "right_urn": f"urn:p-lod:id:{right_r.identifier}"}

    # http://umassamherst.lunaimaging.com/luna/servlet/as/search?lc=umass%7E14%7E14&q=PALP_11258
    # j['results'][0]['urlSize4']


## dunder methods
    def __str__(self):
        return self.label

    