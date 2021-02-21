import plodlib

r_list = ['pompeii','r1','r1-i1','r1-i9-p1','bird','dog','ariadne','bogus_id_bogus']

# run through instantiating ids and printing standard info
for r in r_list:
	c = plodlib.PLODResource(r)
	print(f'''*Made instance of PLODResource for "{r}" now reading from returned object
  Identifier: {c.identifier} (as passed: {c._identifier_parameter})
  Type: {c.type}
  Label: {c.label}
  P-in-P URL: {c.p_in_p_url}
  Wikidata URL: {c.wikidata_url}

		''')

# Call functions for each item in r_list

for r in r_list:
	print(f'*Spatial hierarchy for "{r}"')
	print(plodlib.PLODResource(r).spatial_hierarchy_up())

for r in r_list:
	print(f'*Spatial children for "{r}"')
	print(plodlib.PLODResource(r).spatial_children())

for r in r_list:
	print(f'*Depicted concepts for "{r}"')
	print(plodlib.PLODResource(r).depicts_concepts())

for r in r_list:
	print(f'*Depicted where for "{r}"')
	print(plodlib.PLODResource(r).depicted_where())

# instances of type
type_list = ['region','street']

for r in type_list:
	print(f'*Instances of "{r}"')
	print(plodlib.PLODResource(r).instances_of())


# predicates
predicate_list = ['wikidata-url']

for r in predicate_list:
	print(f'*"{r}" used as predicate by')
	print(plodlib.PLODResource(r).used_as_predicate_by())
