# Module for accessing information about P-LOD resource for rendering in PALP
# Define a function

from string import Template

import json
import pandas as pd
import requests
import requests_cache
requests_cache.install_cache('plodlib_cache')

import rdflib as rdf
from rdflib.plugins.stores import sparqlstore

# Convenience functions
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


# Define a class
class PLODResource(object):

    def __init__(self,identifier = None):

        # could defaut to 'pompeii' along with its info?
        if identifier == None:
        	self.identifier = None
        	return

        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)
        
        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT ?p ?o WHERE { p-lod:$identifier ?p ?o . }
""")

        results = g.query(qt.substitute(identifier = identifier))
        id_df = pd.DataFrame(results, columns = results.json['head']['vars'])
        id_df = id_df.applymap(str)
        id_df.set_index('p', inplace = True)
    
        # type and label first
        self.rdf_type = None
        try:
        	self.rdf_type = id_df.loc['http://www.w3.org/1999/02/22-rdf-syntax-ns#type','o'].replace('urn:p-lod:id:','')
        except:
        	self.rdf_type = None


        self.label = None
        try:
        	self.label = id_df.loc['http://www.w3.org/2000/01/rdf-schema#label','o']
        except:
        	self.label = None


        self.p_in_p_url = None
        try:
        	self.p_in_p_url = id_df.loc['urn:p-lod:id:p-in-p-url','o']
        except:
        	self.p_in_p_url = None

        self.wikidata_url = None
        try:
        	self.wikidata_url = id_df.loc['urn:p-lod:id:wikidata-url','o']
        except:
        	self.wikidata_url = None
        
        # set identifier if it exists. None otherwise. Preserve identifier as passed
        self._identifier_parameter = identifier
        if len(id_df.index) > 0:
        	self.identifier = identifier
        else:
        	self.identifier = None

        # extras
        # if with_extras:
        self._sparql_results_as_html_table = id_df.to_html()
        self._id_df = id_df


    def gather_images(self):
      # return format is urn (of image), depicts_urn, depicts_type, depicts_label, is_best_image, l_record, l_media, l_batch, l_description, geojson
      if self.rdf_type == 'concept':
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?urn ?label ?best_image ?l_record ?l_media ?l_batch ?l_description ?l_img_url ?feature WHERE {
   
    BIND ( p-lod:$identifier AS ?identifier )
   
    ?component p-lod:depicts ?identifier .

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

        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        #return df.apply(add_luna_info, axis = 1).to_json(orient='records')
        return df.to_json(orient='records')

      elif self.rdf_type in ['space','property','insula','region']:
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

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
        
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        return df.to_json(orient='records')

      elif self.rdf_type in ['feature']:
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

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
        
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        #return df.apply(add_luna_info, axis = 1).to_json(orient='records')
        return df.to_json(orient='records')
      else:
        luna_df =  pd.DataFrame(json.loads(self.images_from_luna))
        if len(luna_df):
          return luna_df.to_json(orient = 'records')
        else:
          return []
      

    @property
    def geojson(self):
      try:
        # if the there is geojson, use it
        my_geojson = self._id_df.loc['urn:p-lod:id:geojson','o']

      except:
        # if no, geojson, try and find some. this may well develop over time
        try:
          # note that depicted_where will return an empty list so check length after calling
          dw_j = json.loads(self.depicted_where(level_of_detail='space'))
          if len(dw_j):
              my_geojson_d = {"type": "FeatureCollection", "features":[]}
              for g in dw_j:
                if g['geojson'] != 'None':
                  f = json.loads(g['geojson'])
                  my_geojson_d['features'].append(f)
              my_geojson = json.dumps(my_geojson_d)
          else:
              my_geojson = None
              print("Failed to parse geojson")
        except:
          # not sure how we can get here but the try needs it and is a form of (slow) robustness.
          print("Failed to parse geojson")
          my_geojson = None

      return my_geojson

    ## get_predicate_values ##
    def get_predicate_values(self,predicate = 'urn:p-lod:id:label'):
        # predicate should be a fully qualified url or urn as a string.
        # returns json array of keyed dictionaries.


        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier
        if identifier == None:
            return json.dumps([])

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT ?values WHERE { p-lod:$identifier <$predicate> ?values . }
""")

        results = g.query(qt.substitute(identifier = identifier, predicate = predicate))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        return df['values'].to_json(orient = 'records')


    ## depicts_concepts ##
    def depicts_concepts(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>

SELECT ?urn ?label (COUNT(*) AS ?count) (GROUP_CONCAT(?within_depicts ; separator = '||') AS ?within_spatial_units_depict) WHERE {

  BIND ( p-lod:$identifier AS ?identifier )

  ?identifier ^p-lod:spatially-within*/^p-lod:created-on-surface-of*/^p-lod:is-part-of* ?component .
  ?component a p-lod:artwork-component .
  ?component p-lod:depicts ?urn .

  OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
  OPTIONAL {
    ?identifier a p-lod:space .
    ?identifier ^p-lod:spatially-within+ ?within_depicts .
    ?within_depicts a p-lod:feature .
    ?component p-lod:is-part-of*/p-lod:created-on-surface-of ?within_depicts .
  }

  OPTIONAL {
    ?identifier a p-lod:property .
    ?identifier ^p-lod:spatially-within+ ?within_depicts .
    ?within_depicts a p-lod:space .
    ?component p-lod:is-part-of*/p-lod:created-on-surface-of/p-lod:spatially-within+ ?within_depicts .
  }

} GROUP BY ?urn ?label ORDER BY ?urn""")

        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        return df.to_json(orient = 'records')


    ## depicted_where ##
    def depicted_where(self, level_of_detail = 'feature'):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn ?type ?label ?within ?best_image ?l_record ?l_media ?l_batch ?geojson  WHERE {
    
    BIND ( p-lod:$resource AS ?resource )
   
    ?component p-lod:depicts ?resource .
    ?component p-lod:is-part-of+/p-lod:created-on-surface-of/p-lod:spatially-within* ?urn .
    ?urn a p-lod:$level_of_detail
    OPTIONAL { ?urn a ?type }
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?urn p-lod:spatially-within ?within }
    OPTIONAL { ?urn p-lod:geojson ?geojson }
 
    OPTIONAL { ?component p-lod:has-action ?action . }
    OPTIONAL { ?component p-lod:has-color  ?color . }
    OPTIONAL { ?component p-lod:best-image ?o . 
               ?best_image rdfs:label ?o .
               ?best_image p-lod:x-luna-record-id ?l_record .
               ?best_image p-lod:x-luna-media-id  ?l_media .
               ?best_image p-lod:x-luna-batch-id  ?l_batch .}
} ORDER BY ?within""")

       # resource = what you're looking for, level_of_detail = spatial resolution at which to list results 
        results = g.query(qt.substitute(resource = identifier, level_of_detail = level_of_detail))

        

        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)

        return df.to_json(orient='records')




   ## spatial_hierarchy_up ##
    def spatial_hierarchy_up(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

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
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        
        return df.to_json(orient='records')


## spatial_children ##
    def spatial_children(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?urn WHERE { ?urn p-lod:spatially-within p-lod:$identifier }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.to_json(orient="records")

## spatially_within
    @property
    def spatially_within(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
    PREFIX p-lod: <urn:p-lod:id:>
    SELECT ?urn ?type ?label ?geojson WHERE {

        p-lod:$identifier p-lod:spatially-within ?urn  . 

        ?urn a ?type .
        OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label  }
        ?urn p-lod:geojson ?geojson .
        
      } LIMIT 1""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        return df.to_json(orient="records")
     

## in_region ##
    @property
    def in_region(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

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
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])

        return df.to_json(orient="records")


## instances_of ##
    def instances_of(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?urn ?type ?label ?geojson (COUNT(?urn) AS ?depiction_count) WHERE
{   ?urn rdf:type/rdfs:subClassOf* p-lod:$identifier .

    OPTIONAL { ?urn a ?type }
    OPTIONAL { ?urn <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?urn p-lod:geojson ?geojson }
    OPTIONAL { ?component p-lod:depicts ?urn ;
               a p-lod:artwork-component . }
 } GROUP BY ?urn ?type ?label ?geojson ORDER BY ?urn""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
    
        return df.to_json(orient="records")


## used_as_predicate_by ##
    def used_as_predicate_by(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?subject ?object WHERE { ?subject p-lod:$identifier ?object}""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.to_json(orient="records")


## images_from_luna ##
    @property
    def images_from_luna(self):

        # Connect to the remote triplestore with read-only connection
        store = rdf.plugins.stores.sparqlstore.SPARQLStore(query_endpoint = "http://52.170.134.25:3030/plod_endpoint/query",
                                           context_aware = False,
                                           returnFormat = 'json')
        g = rdf.Graph(store)

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
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])

        return df.apply(add_luna_info, axis = 1).to_json(orient='records')

    def compare_depicts(self,right):
      left_depicts_json = json.loads(self.depicts_concepts())

      right_depicts_r = PLODResource(right)
      right_depicts_json = json.loads(right_depicts_r.depicts_concepts())

      left_depicts_df = pd.DataFrame(left_depicts_json)
      right_depicts_df = pd.DataFrame(right_depicts_json)

      
      difference_left = set(left_depicts_df['urn']).difference(set(right_depicts_df['urn']))
      intersection = set(left_depicts_df['urn']).intersection(set(right_depicts_df['urn']))
      difference_right = set(right_depicts_df['urn']).difference(set(left_depicts_df['urn']))


      return json.dumps({ "left_urn": f"urn:p-lod:id:{self.identifier}",
                           "difference_left": list(difference_left),
                          "intersection": list(intersection),
                          "difference_right": list(difference_right),
                          "right_urn": f"urn:p-lod:id:{right_depicts_r.identifier}"})

    def compare_depicted(self, right, level_of_detail = 'space'):
      left_depicted_json = json.loads(self.depicted_where(level_of_detail))

      right_depicted_r = PLODResource(right)
      right_depicted_json = json.loads(right_depicted_r.depicted_where(level_of_detail))

      left_depicted_df = pd.DataFrame(left_depicted_json)
      right_depicted_df = pd.DataFrame(right_depicted_json)

      difference_left = set(left_depicted_df['urn']).difference(set(right_depicted_df['urn']))
      intersection = set(left_depicted_df['urn']).intersection(set(right_depicted_df['urn']))
      difference_right = set(right_depicted_df['urn']).difference(set(left_depicted_df['urn']))

      return json.dumps({ "left_urn": f"urn:p-lod:id:{self.identifier}",
                          "difference_left": list(difference_left),
                          "intersection": list(intersection),
                          "difference_right": list(difference_right),
                          "right_urn": f"urn:p-lod:id:{right_depicted_r.identifier}"})

    # http://umassamherst.lunaimaging.com/luna/servlet/as/search?lc=umass%7E14%7E14&q=PALP_11258
    # j['results'][0]['urlSize4']
## dunder methods
    def __str__(self):
        return self.label

    