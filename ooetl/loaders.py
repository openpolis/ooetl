#!/usr/bin/env python
#  -*- coding: utf-8 -*-

""" Loaders classes

A :class:`Loader` class should be instantiated with a connection string,
to enable connection to the storage the data should be loaded to.

Once instantiated, invoking the :func:`load()` method will open the resource,
load all of the data found into the ``self.data`` dictionary and return a
dictionary of results back to the caller.

The ``self.data`` dictionary allows the :func:`load()` method to prepare
data for loading.

Details of the pre-loading operations foe each single subset of data should
be delegated to methods in the ETL object.
"""
import json
from abc import ABCMeta, abstractmethod
from collections import Iterable
import logging
import logging.config
import os
import resource
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import pandas as pd

__author__ = "guglielmo"

from pandas import DataFrame


class Loader(object):
    """Base abstract class. All Loaders classes extend from this.
    """

    __metaclass__ = ABCMeta

    etl = None
    logger = logging.getLogger(__name__)

    @abstractmethod
    def load(self, **kwargs):
        """Base extract method, must be instantiated in extended
        classes.
        """
        pass


class CSVLoader(Loader):
    """:class:`Loader` that stores data in a CSV file

    The csv filename is specified in the constructor::

        csvloader = CSVLoader('/tmp/test.csv')

    """

    def __init__(self, csv_path, label="dati", encoding="utf8", sep=";"):
        """Create a new instance of the loader

        Needs a csv path and may accept an encoding,
        if different from UTF8.

        Args:
            csv_path (string): The path where csv files will be stored
            label: (Optional[string]): A label used to name the file
              This is only used when a single DataFrame is being loaded
            encoding: (Optional[string]): Encoding used while writing the CSV
        """

        super(CSVLoader, self).__init__()
        self.csv_path = csv_path
        self.label = label
        self.encoding = encoding
        self.sep = sep

    def load(self, **kwargs):
        """Writes data to csv file.

        The data contained in the `processed_data`` attribute of
        the `etl` instance are written in one or more csv files, in the
        `csv_path` specified when instantiating the loader.

        All `DataFrame` in the `etl.processed_data` attribute are packed and
        written into CSV.

        If `etl.processed_data` is a dictionary, then multiple files are
        written, and they're named according to `etl.processed_data`'s keys,
        with a `.csv` extension added.

        If `etl.processed_data` is a `DataFrame`, then the `label` attribute
        of the loader is used as the only label, and the name of the csv
        file will be created out of that label.

        """

        # build data dictionary
        data = {}
        if not isinstance(self.etl.processed_data, pd.DataFrame):
            if isinstance(self.etl.processed_data, dict):
                for label, df in self.etl.processed_data.items():
                    data[label] = df
            elif (
                isinstance(self.etl.processed_data, list) and
                isinstance(self.etl.processed_data[0], dict)
            ):
                data[self.label] = pd.DataFrame(self.etl.processed_data)
            else:
                raise Exception(
                    "Could not build dataframe out of processed_data of type {0}".format(type(self.etl.processed_data))
                )
        else:
            data[self.label] = self.etl.processed_data

        # create path to store csv if not existing
        try:
            os.stat(self.csv_path)
        except FileNotFoundError:
            os.mkdir(self.csv_path)

        # write data to CSV
        for label, df in data.items():
            csv_filename = "{0}/{1}.csv".format(self.csv_path, label)
            df.to_csv(csv_filename, encoding=self.encoding, sep=self.sep, index=False)


class JsonLoader(Loader):
    """:class:`Loader` that stores data in a local JSON file

    The json filename and location are specified in the constructor::

        loader = JSONLoader(out_path='/tmp', label="test")

    """

    def __init__(self, filepath: str, encoding: str = "utf8"):
        """Create a new instance of the loader

        Needs a csv path and may accept an encoding,
        if different from UTF8.

        Args:
            filepath: The complete file with path where json file will be stored
        """

        super(JsonLoader, self).__init__()
        self.filepath = filepath
        self.encoding = encoding

    def load(self, **kwargs):
        """Writes data to json file.

        The data contained in the `processed_data`` attribute of
        the `etl` instance are written in one json file, in the
        `out_path` specified when instantiating the loader.

        kwargs: arguments passed to the json.dump function
                see: https://docs.python.org/3.5/library/json.html#basic-usage
        """
        try:
            with open(self.filepath, "w", encoding=self.encoding, **kwargs) as json_file:
                if isinstance(self.etl.processed_data, DataFrame):
                    self.etl.processed_data.to_json(json_file, orient='records')
                else:
                    json.dump(self.etl.processed_data, json_file)
        except Exception as e:
            self.logger.error("Data could not be exported as json: {0}.".format(e))


class ESLoader(Loader):
    """:class:`Loader` that stores data of a given type into an Elasticsearch
    instance

    The elasticsearch constructor needs three parameters::

        esloader = ESLoader(
            'http://user:pass@localhost:9200/',
            'politici', 'person')

    """

    def __init__(
        self,
        es_hosts,
        es_index,
        es_doc_type,
        es_id_field=None,
        es_mapping=None,
        es_batchsize=500,
        es_delete=False,
        log_level="info",
    ):
        """Create a new instance of the loader

        Needs a
        connection_url, the index and document type names,
        also accepts some optional arguments

        Args:
            es_url (string): The connection url (eg
                http://username@password:localhost:9200/)
            es_index: (string): Index name
            es_doc_type: (string): Document type
            es_id_field: (Optional[string]): Name of the unique id field.
                This is optional, but if not specified, an automatic ID will
                be used and previously inserted documents will not be recognized.
            es_mapping: (Optional[dict]): a mapping that allows you to
                specify how each field will be indexed. If not specified,
                the dynamic mapping takes place.
            es_batchsize: (Optional[integer]): Number of document to be
                indexed in a single bulk call. Defaults to 500.
            es_delete: (Optional[bool]): If the index needs to be deleted
                before bulk operations.
        """

        super(ESLoader, self).__init__()
        self.log_level = log_level
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc_type = es_doc_type
        self.es_id_field = es_id_field
        self.es_mapping = es_mapping
        self.es_batchsize = es_batchsize
        self.es_delete = es_delete

    def chunk_log(self, n):
        self.logger.info(u"{}: {}.".format(n, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1000))

    def load(self, **kwargs):
        """Writes data to the elasticsearch instance

        The data contained in the `processed_data`` attribute of
        the `etl` instance are written into the elasticsearch instance,
        using the specified index and doc_type

        """

        self.logger.setLevel(getattr(logging, self.log_level.upper(), logging.WARNING))

        es = Elasticsearch(self.es_hosts)

        if es.indices.exists(self.es_index):
            if self.es_delete:
                result = es.indices.delete(self.es_index)
                self.logger.info(u'Cancellazione indice "{}": {}.'.format(self.es_index, result))
        else:
            result = es.indices.create(self.es_index)
            self.logger.info(u'Creazione indice "{}": {}.'.format(self.es_index, result))

        # need to wait a couple of seconds before index is available
        # after deletion
        time.sleep(2)

        if self.es_mapping:
            result = es.indices.put_mapping(index=self.es_index, doc_type=self.es_doc_type, body=self.es_mapping)
            self.logger.info(u'Inserimento mapping su indice "{}": {}.'.format(self.es_index, result))

        self.logger.info(u"Import dati ...")
        n = 0

        for n, (ok, result) in enumerate(
            streaming_bulk(
                es, self.parse_data(), index=self.es_index, doc_type=self.es_doc_type, chunk_size=self.es_batchsize
            ),
            1,
        ):
            action, result = result.popitem()
            doc_id = "/{_index}/{_type}/{_id}".format(**result)

            if ok:
                self.logger.debug(
                    u'{}: Operazione "{}" riuscita sul documento {}: ' u"{}.".format(n, action, doc_id, result)
                )
            else:
                self.logger.error(
                    u'{}: Operazione "{}" fallita sul documento {}: ' u"{}.".format(n, action, doc_id, result)
                )

            if n % self.es_batchsize == 0:
                self.chunk_log(n)

        if n % self.es_batchsize != 0:
            self.chunk_log(n)

        self.logger.info(u"Fatto.")

    def parse_data(self):
        for n, row in self.etl.processed_data.iterrows():
            if self.es_id_field in row:
                row["_id"] = row[self.es_id_field]
            yield dict(row)


class DjangoBulkLoader(Loader):
    def __init__(self, django_model, chunk_size=10000, batch_size=1000):
        super().__init__()
        self.django_model = django_model
        self.chunk_size = chunk_size
        self.batch_size = batch_size

    def load(self, **kwargs):
        self.logger.info("Loading data using Django ORM...")

        dictionary = self.etl.processed_data.to_dict("records")

        self.logger.debug(
            "Attempting to import {0} records with bulk_create, "
            "with chunk size of {1} and batch size of {2}.".format(len(dictionary), self.chunk_size, self.batch_size)
        )

        for n, c in enumerate(range(0, len(dictionary), self.chunk_size)):
            model_instances = [self.django_model(**record) for record in dictionary[c: c + self.chunk_size]]
            try:
                self.django_model.objects.bulk_create(model_instances, batch_size=self.batch_size)
                self.logger.info("{0}/{1}".format(n * self.chunk_size, len(dictionary)))
            except Exception as e:
                self.logger.error(e)
                self.logger.error("Loading aborted.")
                return

        self.logger.info("{0}/{0}".format(len(dictionary)))
        self.logger.info(
            "Successfully imported {0} records into {1} table.".format(
                len(dictionary), self.django_model._meta.db_table
            )
        )


class DjangoUpdateOrCreateLoader(Loader):
    def __init__(self, django_model, fields_to_update: Iterable = tuple()):
        super().__init__()
        self.django_model = django_model
        self.fields_to_update = fields_to_update

    def load(self, **kwargs):
        self.logger.info("Loading data using Django ORM...")

        fields_to_update = [f for f in self.fields_to_update if f in self.etl.processed_data.columns]

        records = self.etl.processed_data.to_dict(orient="records")
        self.logger.debug("Attempting to import {0} records.".format(len(records)))
        c = 0
        for record in records:
            defaults = {k: v for k, v in record.items() if k in fields_to_update}
            new_values = {k: v for k, v in record.items() if k not in defaults}
            obj, created = self.django_model.objects.update_or_create(defaults=defaults, **new_values)
            c = c + (1 if created else 0)

        table = self.django_model._meta.db_table
        n_created = c
        n_updated = len(records) - c
        if n_created > 0:
            self.logger.info("Created {0} records into {1} table.".format(n_created, table))
        if n_updated > 0:
            self.logger.info("Updated {0} records into {1} table.".format(n_updated, table))


class LoaderException(Exception):
    pass
