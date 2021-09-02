import pandas as pd

from ooetl.extractors import RemoteExtractor


class SparqlExtractor(RemoteExtractor):
    def __init__(self, url, query):
        """Create a new instance, setting the the `remote_url` attribute to
        the value passed as `url` argument.

        Args:
            url (string): remote location with data

        Returns:
            instance of a :class:`RemoteExtractor` subclass
        """
        super(SparqlExtractor, self).__init__(url)
        self.remote_url = url
        self.sparql_wrapper = SPARQLWrapper(self.remote_url)
        self.query = query

    def extract(self, **kwargs):
        self.sparql_wrapper.setQuery(self.query)
        self.sparql_wrapper.setReturnFormat("json")
        results = self.sparql_wrapper.query().convert()

        # transform results into a simple dict
        # so that it can be directly used to create a DataFrame
        res = []
        for r in results["results"]["bindings"]:
            res.append(dict((l, r[l]["value"]) for l in r.keys()))
        return pd.DataFrame(res)