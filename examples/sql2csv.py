"""
This is a basic example of how to use the ooetl framework to extract
data from a postgresql source, with a query, and write the results to
a CSV file.
"""

from ooetl import ETL
from ooetl.extractors import SqlExtractor
from ooetl.loaders import CSVLoader

ETL(
    extractor=SqlExtractor(
        conn_url="postgresql://postgres:@localhost:5432/opdm",
        sql="select id, name, inhabitants "
            "from popolo_area where istat_classification='COM' "
            "order by inhabitants DESC NULLS LAST"
    ),
    loader=CSVLoader(
        csv_path="./data/",
        label='opdm_areas'
    )
)()
