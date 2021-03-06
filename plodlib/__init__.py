# Module for accessing information about P-LOD resource for rendering in PALP
# Define a function

from string import Template

import pandas as pd

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

        # now in alphabetical order
        self.geojson = None
        try:
            self.geojson = id_df.loc['urn:p-lod:id:geojson','o']
        except:
        	self.geojson = None


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
        
        # set identifer if it exists. None otherwise. Preserve identifier as passed
        self._identifier_parameter = identifier
        if len(id_df.index) > 0:
        	self.identifier = identifier
        else:
        	self.identifier = None

        # extras
        # if with_extras:
        self._sparql_results_as_html_table = id_df.to_html()
        self._id_df = id_df


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

} ORDER BY ?depiction""")
        results = g.query(qt.substitute(identifier = identifier))
        df = pd.DataFrame(results, columns = results.json['head']['vars'])
        df = df.applymap(str)
    
        return df.values.tolist()


    ## depicted_where ##
    def depicted_where(self, level_of_detail = 'space'):
        # Connect to the remote triplestore with read-only connection
        store = rdf.plugin.get("SPARQLStore", rdf.store.Store)(endpoint="http://52.170.134.25:3030/plod_endpoint/query",
                                                       context_aware = False,
                                                       returnFormat = 'json')
        g = rdf.Graph(store)

        identifier = self.identifier

        qt = Template("""
PREFIX p-lod: <urn:p-lod:id:>

SELECT DISTINCT ?within ?type ?label ?geojson ?action ?color  WHERE {
    
    BIND ( p-lod:$resource AS ?resource )
   
    ?component p-lod:depicts ?resource .

    ?component p-lod:is-part-of+/p-lod:created-on-surface-of/p-lod:spatially-within* ?within .
    ?within a p-lod:$level_of_detail


    OPTIONAL { ?within a ?type }
    OPTIONAL { ?within p-lod:geojson ?geojson }
    OPTIONAL { ?within <http://www.w3.org/2000/01/rdf-schema#label> ?label }
 
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
    { p-lod:$identifier p-lod:spatially-within+ ?spatial_id  . 
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
SELECT DISTINCT ?instance WHERE { ?instance a p-lod:$identifier }""")
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


## dunder methods
    def __str__(self):
        return self.label

    