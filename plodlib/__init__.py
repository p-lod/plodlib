# Module for accessing information about P-LOD resource for rendering in PALP
# Define a function

from string import Template

import json
import pandas as pd
import requests

import rdflib as rdf
from rdflib.plugins.stores import sparqlstore


# Define a class
class PLODResource(object):
    def __init__(self,identifier = None):

        # could defaut to 'pompeii' along with its info?
        if identifier == None:
        	self.identifier = None
        	return

    	# Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
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


    @property
    def geojson(self):
        try:
            print("Looking for p-lod:geojson predicate.")
            my_geojson_d = json.loads(self._id_df.loc['urn:p-lod:id:geojson','o'])

            if my_geojson_d['type'] == 'FeatureCollection':
                for f in my_geojson_d['features']:
                    f['id'] = self.identifier
                    f['properties'] = {'title':self.identifier}
            else:
                my_geojson_d['id'] = self.identifier
                my_geojson_d['properties'] = {'title':self.identifier}

            my_geojson = json.dumps(my_geojson_d)

        except:
            try:
                dw_l = self.depicted_where(level_of_detail='space')
                if len(dw_l):

                    my_geojson_d = {"type": "FeatureCollection", "features":[]}
                    for g in dw_l:
                        f = json.loads(g[-1])
                        f['id'] = g[0]
                        f['properties'] = {'title' : g[0]}
                        my_geojson_d['features'].append(f)
                    my_geojson = json.dumps(my_geojson_d)
                else:
                    my_geojson = None
            except:
                my_geojson = None

        return my_geojson

    ## get_predicate_values ##
    def get_predicate_values(self,predicate = 'urn:p-lod:id:label'):
        # predicate should be a fully qualified url or urn as a string.
        # returns a list.


        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier
        if identifier == None:
            return []

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT ?o WHERE { p-lod:$identifier <$predicate> ?o . }
""")

        results = g.query(qt.substitute(identifier = identifier, predicate = predicate))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = id_df.applymap(str)

        return df.values.tolist()


    ## get_depicted_concepts ##
    def depicts_concepts(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX plod: <urn:p-lod:id:>

SELECT DISTINCT ?concept ?label WHERE {
 
    plod:$identifier ^plod:spatially-within*/^plod:created-on-surface-of*/^plod:is-part-of* ?component .
    ?component a plod:artwork-component .
    ?component plod:depicts ?concept .

    OPTIONAL { ?concept <http://www.w3.org/2000/01/rdf-schema#label> ?label }

    # when this is part of the PALP interface, this clause can select "smallest 
    # clickable spatial unit" that will be shown to public via its own page
    #?component plod:is-part-of+/plod:created-on-surface-of/plod:spatially-within* ?within .
    #?within a plod:####within_resolution .

} ORDER BY ?concept""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.values.tolist()


    ## depicted_where ##
    def depicted_where(self, level_of_detail = 'feature'):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>

SELECT DISTINCT ?id ?type ?label ?within ?action ?color ?geojson  WHERE {
    
    BIND ( p-lod:$resource AS ?resource )
   
    ?component p-lod:depicts ?resource .

    ?component p-lod:is-part-of+/p-lod:created-on-surface-of/p-lod:spatially-within* ?id .
    ?id a p-lod:$level_of_detail


    OPTIONAL { ?id a ?type }
    OPTIONAL { ?id <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?component p-lod:is-part-of+/p-lod:created-on-surface-of/p-lod:spatially-within ?within }
    OPTIONAL { ?id p-lod:geojson ?geojson }
 
    OPTIONAL { ?component p-lod:has-action ?action . }
    OPTIONAL { ?component p-lod:has-color  ?color . }

} ORDER BY ?within""")

       # resource = what you're looking for, level_of_detail = spatial resolution at which to list results 
        results = g.query(qt.substitute(resource = identifier, level_of_detail = level_of_detail))

        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)

        return df.values.tolist()


   ## spatial_hierarchy_up ##
    def spatial_hierarchy_up(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?spatial_id ?type ?label ?geojson WHERE { 
  { p-lod:$identifier p-lod:is-part-of*/p-lod:created-on-surface-of* ?feature .
    ?feature p-lod:spatially-within* ?spatial_id .
    ?feature a p-lod:feature  .
    OPTIONAL { ?spatial_id a ?type }
    OPTIONAL { ?spatial_id p-lod:geojson ?geojson }
    OPTIONAL { ?spatial_id <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    }
    UNION
    { p-lod:$identifier p-lod:spatially-within* ?spatial_id  . 
      OPTIONAL { ?spatial_id a ?type }
      OPTIONAL { ?spatial_id p-lod:geojson ?geojson }
      OPTIONAL { ?spatial_id <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    }
  }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.values.tolist()


## spatial_children ##
    def spatial_children(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?spatial_id WHERE { ?spatial_id p-lod:spatially-within p-lod:$identifier }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.values.tolist()

## in_region ##
    @property
    def in_region(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
    PREFIX p-lod: <urn:p-lod:id:>
    SELECT DISTINCT ?spatial_id ?type ?label ?geojson WHERE {

      { p-lod:$identifier p-lod:is-part-of*/p-lod:created-on-surface-of* ?feature .
        ?feature p-lod:spatially-within* ?spatial_id .
        ?feature a p-lod:feature  .
        OPTIONAL { ?spatial_id a ?type }
        OPTIONAL { ?spatial_id p-lod:geojson ?geojson }
        OPTIONAL { ?spatial_id <http://www.w3.org/2000/01/rdf-schema#label> ?label }
        }
        UNION
        { p-lod:$identifier p-lod:spatially-within+ ?spatial_id  . 
          OPTIONAL { ?spatial_id a ?type }
          OPTIONAL { ?spatial_id p-lod:geojson ?geojson }
          OPTIONAL { ?spatial_id <http://www.w3.org/2000/01/rdf-schema#label> ?label }
        }
        FILTER EXISTS { ?type a p-lod:region}
      }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)

        return df.values.tolist()


## instances_of ##
    def instances_of(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>
SELECT DISTINCT ?instance ?type ?label ?geojson WHERE
{   ?instance a p-lod:$identifier .

    OPTIONAL { ?instance a ?type }
    OPTIONAL { ?instance <http://www.w3.org/2000/01/rdf-schema#label> ?label }
    OPTIONAL { ?instance p-lod:geojson ?geojson }
 }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.values.tolist()


## used_as_predicate_by ##
    def used_as_predicate_by(self):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
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
    
        return df.values.tolist()


## images_from_luna ##
    @property
    def images_from_luna(self):

        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
        PREFIX p-lod: <urn:p-lod:id:>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT DISTINCT ?label 
        WHERE {
        ?subject p-lod:depicts p-lod:$identifier .
        ?subject a p-lod:luna-image .
        ?subject rdfs:label ?label
         }""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
            
        l = df.values.tolist()
        q = ""
        for luna_image in l:
            #url = f'http://umassamherst.lunaimaging.com/luna/servlet/as/search?lc=umass%7E14%7E14&q={luna_image[0]}'
            #print(url)
            q = q + luna_image[0] + ' OR '

        url = requests.get(f'http://umassamherst.lunaimaging.com/luna/servlet/as/search?lc=umass%7E14%7E14&q={q}')
        text = url.text
        data = json.loads(text)
        # luna_image.append(data['results'][0]['urlSize4'])
        return_list = []
        for r in data['results']:
            try:
                img_url = r['urlSize4']
            except KeyError:
                img_url = r['urlSize2']

            return_list.append([r['fieldValues'][1]['Archive_ID'][0],r["id"],img_url])
            # print(f'{r["id"]}\n\n')


        #url = requests.get("https://jsonplaceholder.typicode.com/users")
        #text = url.text
        # <iframe id="widgetPreview" frameBorder="0"  width="700px"  height="350px"  border="0px" style="border:0px solid white"  src="https://umassamherst.lunaimaging.com/luna/servlet/detail/umass~14~14~99562~1272567?embedded=true&cic=umass%7E14%7E14&widgetFormat=javascript&widgetType=detail&controls=1&nsip=1" ></iframe>


        return return_list # df.values.tolist()

    # http://umassamherst.lunaimaging.com/luna/servlet/as/search?lc=umass%7E14%7E14&q=PALP_11258
    # j['results'][0]['urlSize4']
## dunder methods
    def __str__(self):
        return self.label

    