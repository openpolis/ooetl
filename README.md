[![Latest Version](https://img.shields.io/pypi/v/ooetl.svg)](https://pypi.python.org/pypi/ooetl)
[![Latest Version](https://img.shields.io/pypi/pyversions/ooetl.svg)](https://pypi.python.org/pypi/ooetl)
[![License](https://img.shields.io/pypi/l/ooetl.svg)](https://pypi.python.org/pypi/ooetl)
[![Downloads](https://pepy.tech/badge/ooetl/month)](https://pepy.tech/project/ooetl/month)

[![Twitter Follow](https://img.shields.io/twitter/follow/openpolislab)](https://twitter.com/openpolislab)

![Tests Badge](https://op-badges.s3.eu-west-1.amazonaws.com/ooetl/tests-badge.svg?2)
![Coverage Badge](https://op-badges.s3.eu-west-1.amazonaws.com/ooetl/coverage-badge.svg?2)
![Flake8](https://op-badges.s3.eu-west-1.amazonaws.com/ooetl/flake8-badge.svg?2)


**ooetl** is a minimal opinionated object oriented ETL framework.

The class-based nature of the framework allows to build complex dedicated classes,
starting from simple abstract ones.


## Installation

Python versions from 3.7.1 are supported.

The package is hosted on pypi, and can be installed, for example using pip:

    pip install --upgrade "ooetl[all]"
    pip install "ooetl[elastic]==1.1.2"

or poetry:

    poetry add ooetl -Eall
    poetry add ooetl -Eelastic


## Usage

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

- CSVExctractor - extracts data from a remote CSV
- ZIPCSVExctractor - extracts data from a remote zipped CSV
- HTMLParserExtractor - extracts data from a remote HTML page (requires html extra and needs to be extended)
- SparqlExtractor - extracts data from a remote SPARQL endpoint (requires sparql extra)
- SqlExtractor - extracts data from a RDBMS (requires mysql or postgresql extra)
- XSLExtractor - extracts data from a remote Excel file
- ZIPXLSExctractor - extracts data from an excel file within a remote zipped archive

Loaders:

- CSVLoader - loads data into a CSV
- JsonLoader - loads data into a json file
- ESLoader - loads data into an ES instance (requires elastic extra)
- DjangoBulkLoader - adds data in bulk to a django model (only works inside a django project)
- DjangoUpdateOrCreateLoader - adds data with an update or create logic into a django model (slow, only works within a django project)

The `ooetl.ETL` abstract class is defined in the `__init__.py` file of the `ooetl` package.

ETL classes implement a pipeline of extraction, transformation and load logic.

Aa a very basic example, here is how to extract data from a postgresql query, into a CSV file.

```python
    from ooetl import ETL
    from ooetl.extractors import SqlExtractor
    from ooetl.loaders import CSVLoader
    
    ETL(
        extractor=SqlExtractor(
            conn_url="postgresql://postgres:@localhost:5432/opdm",
            sql="select id, name, inhabitants from popolo_area where istat_classification='COM' "
                "order by inhabitants desc"
        ),
        loader=CSVLoader(
            csv_path="./",
            label='opdm_areas'
        )
    )()
```

Extractors (and Loaders) may be easily extended within the projects using the `ooetl` package.
As an example, consider the following snippet, extending the abstract `ooetl.HTMLParserExtractor`, that parser
the Italian government's site and extracts the list of officials, as CSV.

This example requires the html extra to be installed.

```python
    import requests
    from lxml import html
    from lxml.cssselect import CSSSelector
    
    from ooetl import ETL
    from ooetl.extractors import HTMLParserExtractor
    from ooetl.loaders import CSVLoader
    
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
    
                print(item_name)
    
            return items
    
    ETL(
        extractor=GovernoExtractor("https://www.governo.it/it/ministri-e-sottosegretari"),
        loader=CSVLoader(
            csv_path="./",
            label='governo'
        )
    )()
```

Other, more complex examples can be found in the `examples` directory.

## Support

There is no guaranteed support available, but authors will try to keep up with issues 
and merge proposed solutions into the code base.

## Project Status
This project is currently being developed by the [Openpolis Foundation](https://www.openpolis.it/openpolis-foundation/)
and is being used interanally.

Currently extras for elasticsearch and sparql have been developed.
 
Should more be needed, you can either ask to increase the coverage, or try to contribute, following instructions below.

## Contributing
In order to contribute to this project:
* verify that python 3.7.1+ is being used (or use [pyenv](https://github.com/pyenv/pyenv))
* verify or install [poetry](https://python-poetry.org/), to handle packages and dependencies in a leaner way, 
  with respect to pip and requirements
* clone the project `git clone git@github.com:openpolis/ooetl.git` 
* install the dependencies in the virtualenv, with `poetry install -Eall`,
  this will also install the dev dependencies and all extras
* develop and test 
* create a [pull request](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests)
* wait for the maintainers to review and eventually merge your pull request into the main repository

### Testing
Tests are under the tests folder, and can be launched with 

    pytest

Requests and responses from ATOKA's API are mocked, in order to avoid having to connect to 
the remote service during tests (slow and needs an API key).

Coverage is installed as a dev dependency and can be used to see how much of the package's code is covered by tests:

    coverage run -m pytest

    # sends coverage report to terminal
    coverage report -m 

    # generate and open a web page with interactive coverage report
    coverage html
    open htmlcov/index.html 

Syntax can be checked with `flake8`.

Coverage and flake8 configurations are in their sections within `setup.cfg`.

## Authors
Guglielmo Celata - guglielmo@openpolis.it

## Licensing
This package is released under an MIT License, see details in the LICENSE.txt file.
