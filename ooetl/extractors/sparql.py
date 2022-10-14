from SPARQLWrapper import SPARQLWrapper2
from SPARQLWrapper.SmartWrapper import Bindings
import pandas as pd
import urllib.error
from ooetl.extractors import RemoteExtractor


class SparqlExtractor(RemoteExtractor):
    def __init__(self, endpoint_url: str, query: str, max_retries: int = 10, legacy: bool = False, method: str = "GET"):
        """Create a new instance, setting the parameters

        Args:
            :endpoint_url: remote sparql endpoint
            :query: the query to post to the sparql endpoint
            :max_retries: the max retries to attempt
            :legacy: if the results are returned in a legacy format
            :method: "POST" or "GET"

        Returns:
            instance of a :class:`RemoteExtractor` subclass
        """
        super(SparqlExtractor, self).__init__(endpoint_url)
        self.remote_url = endpoint_url
        self.query = query
        self.max_retries = max_retries
        self.legacy = legacy
        self.method = method

    def extract(self, **kwargs):
        sparql_wrapper = SPARQLWrapper2(self.remote_url)
        sparql_wrapper.setQuery(self.query)
        sparql_wrapper.setMethod(self.method)

        # while loop to handle repeating in case of temporary errors.
        results = None
        max_retries = self.max_retries
        while max_retries > 0:

            try:
                result: Bindings = sparql_wrapper.query()
                if self.legacy:
                    results = result.fullResult["results"]["bindings"]
                else:
                    results = result.bindings
                break
            except urllib.error.HTTPError:
                max_retries -= 1
        if results is None:
            raise Exception(f"Endpoint {self.remote_url} failure.")

        # transform results into a simple dict
        # so that it can be directly used to create a DataFrame
        res = []
        for r in results:
            res.append(dict((k, r[k].value) for k in r.keys()))
        return pd.DataFrame(res)
