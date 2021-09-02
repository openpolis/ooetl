"""
This example shows how to extend an Extractor class, overwriting exising methods, in order
to perform specific tasks.

The following snippets parse content from the italian government we site and
writes the results to a CSV file.
"""

import logging
import requests
from lxml import html
from lxml.cssselect import CSSSelector
from ooetl import ETL
from ooetl.extractors import HTMLParserExtractor
from ooetl.loaders import CSVLoader

logging.basicConfig(format='%(levelname)s: %(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class GovernoExtractor(HTMLParserExtractor):

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

            if self.etl.logger:
                self.etl.logger.info(item_name)

        return items

ETL(
    extractor=GovernoExtractor("https://www.governo.it/it/ministri-e-sottosegretari"),
    loader=CSVLoader(
        csv_path="./data/",
        label='governo'
    ),
    logger=logger
)()
