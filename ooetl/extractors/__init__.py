# -*- coding: utf-8 -*-
""" Extractors classes

An :class:`Extractor` class should be instantiated
with a remote URL or a local path.

Once instantiated, invoking the :func:`extract()` method will open the resource,
extract the information into a :class:`pandas.DataFrame` and return it to
the caller.

The extract method, when invoked for an instance pointing to a local resource,
will not use the ``request`` module to open the resource, but the ``open``
python call.
"""
import logging

from io import BytesIO as _io  # noqa

import zipfile
from abc import ABCMeta, abstractmethod

import pandas as pd
import requests


class Extractor(object):
    """Base abstract class. All Extractors classes extend from this.
    """

    __metaclass__ = ABCMeta

    etl = None
    logger = logging.getLogger(__name__)

    @abstractmethod
    def extract(self, **kwargs):
        """Base extract method, must be instantiated in extended
        classes.
        """
        pass


class DataframeExtractor(Extractor):
    """A dummy extractor, that
    extract data from already in-memory dataframe (thus the nam edummy).

    This can be useful to build ETL processes where multiple sources are extracted and
    processed in memory, before the ETL is defined.
    """

    def __init__(self, df):
        """Create a new instance of the extractor, with datafram in memory.
        """
        self.df = df

    def extract(self):
        return self.df


class RemoteExtractor(Extractor):
    """Base :class:`Extractor` for remote data.

    Extractors reading data from a remote source need a
    `remote_url`, to be specified in the constructor.
    """

    remote_url = ""

    __metaclass__ = ABCMeta

    def __init__(self, url):
        """Create a new instance, setting the the `remote_url` attribute to
        the value passed as `url` argument.

        Args:
            url (string): remote location with data

        Returns:
            instance of a :class:`RemoteExtractor` subclass
        """
        super(RemoteExtractor, self).__init__()
        self.remote_url = url

    def extract(self):
        raise Exception("extract method cannot be defined in abstract class")


class CSVExtractor(Extractor):
    """:class:`Extractor`  for remote data, exposed in an
    uncompressed csv format
    """

    def __init__(self, source, **kwargs):
        """Create a new instance of the extractor

        Needs a remote url and may accept other named arguments,
        the arguments used here are a short list of arguments that can be
        found in the :function:`pandas.read_csv()` method.


        Args:
            source: (string)
            sep: (Optional[string]): The separator character; defaults to ";"
            skiprows: (Optional[string]): The number of rows to skip;
              defaults to 0
            header: (Optional[string]): The row at which the header is
              found; defaults to 0
            encoding: (Optional[string]): Encoding used while reading the
              CSV; defaults to "utf8"
            keep_default_na: (Optiona[bool]): If na_values are specified
              and keep_default_na is False the default NaN values are
             overridden, otherwise they’re appended to.
            dtype: (Optional[dict]): The types of objects extracted, as
              a dict, eg: {'a': np.float64, 'b': str, 'c': np.int32},
              defaults to 'str'
            converters : dict, default None
                Dict of functions for converting values in certain columns.
                Keys can either be integers or column labels
        """
        self.source = source
        self.sep = kwargs.get("sep", ";")
        self.skiprows = kwargs.get("skiprows", 0)
        self.skipfooter = kwargs.get("skipfooter", 0)
        self.header = kwargs.get("header", 0)
        self.encoding = kwargs.get("encoding", "utf8")
        self.dtype = kwargs.get("dtype", "str")
        self.converters = kwargs.get("converters", {})
        self.na_filter = kwargs.get("na_filter", True)
        self.na_values = kwargs.get(
            "na_values",
            [
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "-nan",
                "1.#IND",
                "1.#QNAN",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                "nan",
            ],
        )
        self.keep_default_na = kwargs.get("keep_default_na", True)

    @property
    def remote_url(self):
        """Return self.set_url when required, for compatibility

        :return:
        """
        return self.source

    def extract(self, **kwargs):
        """Extracts data from a remote, csv source

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing extracted items
        """

        df = pd.read_csv(
            self.source,
            index_col=False,
            sep=self.sep,
            skiprows=self.skiprows,
            skipfooter=self.skipfooter,
            header=self.header,
            encoding=self.encoding,
            dtype=self.dtype,
            converters=self.converters,
            na_filter=self.na_filter,
            na_values=self.na_values,
            keep_default_na=self.keep_default_na,
        )

        return df


class LocalCSVExtractor(CSVExtractor):
    """:class:`Extractor`  for local data, exposed in an
    uncompressed csv format

    DEPRECATED: kept here for compatibility

    Use CSVExtractor, passing the local_path as the url parameter
    """

    def __init__(self, abs_path, **kwargs):
        """Create a new instance of the extractor

        Needs a remote url and may accept other named arguments,
        the arguments used here are a short list of arguments that can be
        found in the :function:`pandas.read_csv()` method.


        Args:
            abs_path: (string)
            sep: (Optional[string]): The separator character; defaults to ";"
            skiprows: (Optional[string]): The number of rows to skip;
              defaults to 0
            header: (Optional[string]): The row at which the header is
              found; defaults to 0
            encoding: (Optional[string]): Encoding used while reading the
              CSV; defaults to "utf8"
            keep_default_na: (Optiona[bool]): If na_values are specified
              and keep_default_na is False the default NaN values are
             overridden, otherwise they’re appended to.
            dtype: (Optional[dict]): The types of objects extracted, as
              a dict, eg: {'a': np.float64, 'b': str, 'c': np.int32},
              defaults to 'str'
            converters : dict, default None
                Dict of functions for converting values in certain columns.
                Keys can either be integers or column labels
        """
        super(LocalCSVExtractor, self).__init__(abs_path, **kwargs)


class ZIPCSVExtractor(CSVExtractor):
    """:class:`Extractor` for remote data, exposed in a compressed csv format
    """

    def __init__(self, source, **kwargs):
        """Constructor method

        :param source: the source URL of the zipped file
        :param kwargs: kwargs used by CSVExtractor and ``verify_tls``,
            used to avoid https errors due to TLS verification failures
        """
        self.verify_tls = kwargs.pop('verify_tls', True)
        super().__init__(source, **kwargs)

    def extract(self, **kwargs):
        """Extracts data from remote, zipped csv source

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing extracted items
        """
        r = requests.get(self.source, verify=self.verify_tls)
        z = zipfile.ZipFile(_io(r.content))
        z_filename = [f.filename for f in z.filelist if f.filename[-4:] == '.csv'][0]
        df = pd.read_csv(
            z.open(z_filename),
            index_col=False,
            sep=self.sep,
            skiprows=self.skiprows,
            header=self.header,
            encoding=self.encoding,
            dtype=self.dtype,
            converters=self.converters,
            na_filter=self.na_filter,
            na_values=self.na_values,
            keep_default_na=self.keep_default_na,
        )

        return df


class HTMLParserExtractor(RemoteExtractor):
    """Base :class:`Extractor` class for data contained in a remote URL as HTML
    code.
    """

    def parse(self, html_content):
        """Parses the HTML content.

        Must be overriden by classes extending this one. Details and
        techniques of parsing are completely delegated to extending classes.

        Args:
            html_content (str): The HTML content to be parsed.

        Returns:
            list of dict: A list of dictionaries containing parsed items.

            The list can be passed to the :func:`pandas.DataFrame()` constructor to build a
            :class:`pandas.DataFrame`.
        """
        raise Exception("parse method cannot be launched in abstract class")

    def extract(self, verbosely=False, **kwargs):
        """Extracts html content from the remote URL.

        Delegates parsing of the HTML code to the
        :func:`HTMLParserExtractor.parse()` method.

        Args:
            verbosely (Optional[bool]): Verbosity during extraction.
                Sets the `etl.verbosity` flag to true, and allows the parser
                to use it internally, in order to print to screen or log,
                for debugging purposes
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing parsed items
        """
        r = requests.get(self.remote_url)

        old_verbosity = self.etl.verbosity
        if verbosely:
            self.etl.verbosity = True
        parsed_items = self.parse(r.content)
        self.etl.verbosity = old_verbosity

        return pd.DataFrame(parsed_items)


class SqlExtractor(Extractor):
    def __init__(self, conn_url, sql, index_col=None):
        """Create a new instance, setting the the `conn_url` attribute to
        the value passed as `conn_url` argument.

        Args:
            conn_url (string): connection url to the db (eg:
                               "mysql://root:@localhost:3306/fb")
            sql (string): string SQL query or SQLAlchemy
                          Selectable (select or text object)
                          to be executed, or database table name.
            index_col (string): name of the column to be used as index

        Returns:
            instance of an :class:`Extractor` subclass
        """
        super(SqlExtractor, self).__init__()
        self.conn_url = conn_url
        self.sql = sql
        if index_col:
            self.index_col = index_col

    def extract(self, **kwargs):
        return pd.read_sql(self.sql, self.conn_url, coerce_float=False)


class XLSExtractor(Extractor):
    """:class:`Extractor`  for remote data, exposed in an
    xls format
    """

    def __init__(self, io, **kwargs):
        """Create a new instance of the extractor

        Needs a remote url and may accept other named arguments,
        the arguments used here are a short list of arguments that can be
        found in the :function:`pandas.read_excel()` method.


        Args:
            io: (string), file-like object, pandas ExcelFile,
                or xlrd workbook. The string could be a URL.
                Valid URL schemes include http, ftp, s3, and file.
                For file URLs, a host is expected.
                For instance, a local file could be
                file://localhost/path/to/workbook.xlsx
            sheet_name: (Optional[string]): The sheet name, defaults to 0;
                the first sheet.
            header: (Optional[string]): The row at which the header is
              found; defaults to 0
            skiprows: (Optional[string]): The number of rows to skip;
              defaults to 0
            skipfooter: (Optional[string]): The number of rows to skip;
              at the bottom of the file; defaults to 0
            keep_default_na: (Optiona[bool]): If na_values are specified
              and keep_default_na is False the default NaN values are
             overridden, otherwise they’re appended to.
            dtype: (Optional[dict]): The types of objects extracted, as
              a dict, eg: {'a': np.float64, 'b': str, 'c': np.int32},
              defaults to 'str'
            converters : dict, default None
                Dict of functions for converting values in certain columns.
                Keys can either be integers or column labels
        """

        super(XLSExtractor, self).__init__()
        self.io = io
        self.sheet_name = kwargs.get("sheet_name", 0)
        self.skiprows = kwargs.get("skiprows", 0)
        self.skipfooter = kwargs.get("skipfooter", 0)
        self.header = kwargs.get("header", 0)
        self.dtype = kwargs.get("dtype", "str")
        self.converters = kwargs.get("converters", {})
        self.na_filter = kwargs.get("na_filter", True)
        self.na_values = kwargs.get(
            "na_values",
            [
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "-nan",
                "1.#IND",
                "1.#QNAN",
                "N/A",
                "NA",
                "NULL",
                "NaN",
                "nan",
            ],
        )
        self.keep_default_na = kwargs.get("keep_default_na", True)

    def extract(self, **kwargs):
        """Extracts data from a remote, csv source

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing extracted items
        """

        df = pd.read_excel(
            io=self.io,
            skiprows=self.skiprows,
            skipfooter=self.skipfooter,
            sheet_name=self.sheet_name,
            header=self.header,
            dtype=self.dtype,
            converters=self.converters,
            na_filter=self.na_filter,
            na_values=self.na_values,
            keep_default_na=self.keep_default_na,
        )

        return df


class ZIPXLSExtractor(XLSExtractor):
    """:class:`Extractor` for remote data, exposed in compressed xls files
    """

    def __init__(self, io, xls_filepath, **kwargs):
        """Constructor method

        :param source: the source URL of the zipped file
        :param xls_filepath: the path of the excel file to extract
        :param kwargs: kwargs used by XLSExtractor and ``verify_tls``,
            used to avoid https errors due to TLS verification failures
        """
        self.verify_tls = kwargs.pop('verify_tls', True)
        self.xls_filepath = xls_filepath
        super().__init__(io, **kwargs)

    def extract(self, **kwargs):
        """Extracts data from remote, zipped xls source

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing extracted items
        """
        r = requests.get(self.io, verify=self.verify_tls)
        z = zipfile.ZipFile(_io(r.content))
        try:
            z_filename = next(
                f.filename for f in z.filelist if self.xls_filepath in f.filename
            )
        except StopIteration:
            raise Exception(f"Could not find file {self.xls_filepath} in zipped file from {self.io}")
        else:
            df = pd.read_excel(
                io=z.open(z_filename),
                skiprows=self.skiprows,
                skipfooter=self.skipfooter,
                sheet_name=self.sheet_name,
                header=self.header,
                dtype=self.dtype,
                converters=self.converters,
                na_filter=self.na_filter,
                na_values=self.na_values,
                keep_default_na=self.keep_default_na,
            )

            return df


class FakeExtractor(Extractor):
    """Extractor that does nothing"""

    def extract(self, kwargs) -> pd.DataFrame:
        """Do nothing

        Args:
            **kwargs: Arbitrary keyword arguments.

        Returns:
            :class:`pandas.DataFrame`: The dataframe containing nothing
        """
        return pd.DataFrame()
