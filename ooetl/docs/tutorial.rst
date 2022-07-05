Tutorial
========

This is a quick tutorial on how to use the `ooetl` module in order to fetch
data from a SQL source and store them into a CSV destination.

The ETL process is performed by invoking the `etl()` method on a `ooetl.ETL` instance.
The `etl()` method is a shortcut to the sequence `ooetl.ETL::extract().transform().load()`,
which is possible, as each method returns a pointer to the `ooetl.ETL` instance.

When the `ooetl.ETL` instance invokes the `ooetl.ETL::extract()` method, it invokes the corresponging
`ooetl.Extractor::extract()` method of the *extractor*. The method extracts the data from the source
into the `ooetl.ETL::original_data` attribute of the ooetl.`ETL` instance.

The `ooetl.ETL::transform()` method is overridden in the instance and may be used to apply
custom data transformation, before the loading phase.
The data from `ooetl.ETL::original_data` are then transformed into `ooetl.ETL::processed_data`.

The `ooetl.ETL::load()` method invokes the `ooetl.Loader::load()` method storing the data from
`ooetl.ETL::processed_data` into the defined destination.

The package provides a series of simple Extractors and Loaders, derived from common abstract classes.

Extractors:

 - CSVExctractor(RemoteExtractor) - extracts data from a remote CSV
 - ZIPCSVExctractor(CSVExctractor) - extracts data from a remote zipped CSV (extends the CSVExtractor)
 - HTMLParserExtractor(RemoteExtractor) - extracts data from a remote HTML page (requires parse override)
 - SparqlExtractor(RemoteExtractor) - extracts data from a remote SPARQL endpoint
 - SqlExtractor(Extractor) - extracts data from a RDBMS
 - XSLExtractor - extracts data from a remote Excel file
 - ZIPXLSExctractor - extracts data from an excel file within a remote zipped archive

Loaders:

 - CSVLoader(Loader) - loads file into a CSV
 - ESLoader(Loader) - loads file into an ES instance

The `ooetl.ETL` abstract class is defined in the `__init__.py` file of the `ooetl` package.

As an example, here is how to extract data from a sql source into a CSV file

.. code-block:: python

    from ooetl import ETL
    from ooetl.extractors import SqlExtractor
    from ooetl.loaders import CSVLoader

    class MySqlETL(ETL):

        def transform(self):
            od = self.original_data

            od = some_transformation(od)

            self.processed_data = od
            return self


    etl = MySqlETL(
        extractor=SqlExtractor(
            conn_url="mysql://root:@localhost:3306/db_production",
            sql="select * from projects"
        ),
        loader=CSVLoader(
            csv_path="/Users/gu/Workspace/my-import/data-processed/",
            label='projects'
        )
    )
    etl.etl()


Extractors (and Loaders) may be easily extended within the projects using the `ooetl` package.
As an example, consider the following example, extending the `ooetl.HTMLParserExtractor`:

.. code-block:: python

    class GovernoItParserExtractor(HTMLParserExtractor):

        def parse(self, html_content):
            list_tree = html.fromstring(html_content)
            items = []
            for e in CSSSelector('div.content div.box_text a')(list_tree):
                item_name = e.text_content().strip()
                item_url = e.get('href').strip()
                item_page = requests.get(item_url)
                item_tree = html.fromstring(item_page.content)
                item_par = CSSSelector('div.content div.field')(item_tree)[0]
                item_charge = CSSSelector('blockquote p')(item_par)[0].text_content().strip()
                item_descr = " ".join([
                  e.text_content() for e in CSSSelector('p')(item_par)[1:] if\
                     e.text_content() is not None
                ])
                items.append({
                    'nome': item_name,
                    'url': item_url,
                    'incarico': item_charge,
                    'descrizione': item_descr
                })

                if self.etl.verbosity:
                    print item_name

            return items

The Extractors and Loaders defined in the package requires a few packages, in order to provide minimal
functionalities::

    cssselect
    lxml
    pandas
    rdflib
    rdflib-jsonld
    requests
    SPARQLWrapper
    elasticsearch


