from ooetl.extractors.sparql import SparqlExtractor
sparql = SparqlExtractor("http://dati.camera.it/sparql", query="select distinct ?o where {[] a ?o} LIMIT 100")
df = sparql.extract()

print(df)
