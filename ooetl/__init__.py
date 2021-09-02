# -*- coding: utf-8 -*-
import logging

from .transformations import DummyTransformation

__version__ = '1.1.2'
__description__ = "A framework to write ETL processes at the Openpolis Foundation."

"""A framework to write ETL processes at the Openpolis Foundation.

.. moduleauthor:: Guglielmo Celata <guglielmo@openpolis.it>

Its main scope is to make it easy for the developers to write parsing,
transformation and loading code, re-using logic wherever possible
and writing custom code only when needed.
"""


class ETL(object):
    """Base ETL class

    All ETL instances may be instantiated from this one.

    The class can be instantiated passing an :class:`Extractor`, a :class:`Transformation` and a
    :class:`Loader` instances, as arguments to the constructor::

            etl = ETL(
                extractor=ZIPCSVExctractor(
                        remote_url
                ),
                transformation=RegionCSV2CSVTransformation(),
                loader=CSVLoader(csv_path, label='governi')
            )


    ETL classes support method chaining::

        etl.extract().transform().load()

    or the equivalent shortcut::

        etl.etl()

    The :func:`extract()`, :func:`transform()` and :func:`load()` methods are just wrappers around
    the same methods of the respective
    :class:`Extractor`, :class:`Transformation` and :class:`Loader` instances. These wrappers
    return the `self` instance, in order to allow method chaining.

    """
    logger = logging.getLogger(__name__)

    def __init__(self, extractor, loader, transformation=None,
                 verbosity=False, log_level=logging.INFO, logger=None, source=None, **kwargs):
        self.verbosity = verbosity
        self.source = source

        self.extractor = None
        self.loader = None
        self.transformation = None

        self.original_data = None
        self.processed_data = {}

        if logger:
            self.logger = logger
            self.logger.setLevel(log_level)

        # opdmetl ETL subclasses are instantiated without
        # the transformation arguments, so this defaults to
        # the DummyTransformation, in order to be back-compatible
        # the transform method override will still work
        if not transformation:
            transformation = DummyTransformation()

        self.set_extractor(extractor)
        self.set_loader(loader)
        self.set_transformation(transformation)

    def __call__(self):
        """Shortcut for ``extract().transform().load()`` as call operator.

        May be used when there are no parameters to be passed to the
        intermediate methods.
        """
        return self.etl()

    def set_loader(self, loader):
        self.loader = loader
        self.loader.etl = self
        self.loader.logger = self.logger
        self.loader.logger.setLevel(self.logger.getEffectiveLevel())

    def set_transformation(self, transformation):
        self.transformation = transformation
        self.transformation.etl = self
        self.transformation.logger = self.logger
        self.transformation.logger.setLevel(self.logger.getEffectiveLevel())

    def set_extractor(self, extractor):
        self.extractor = extractor
        self.extractor.etl = self
        self.extractor.logger = self.logger
        self.extractor.logger.setLevel(self.logger.getEffectiveLevel())

    def extract(self, **kwargs):
        """Extracts data, using the associated extractor.

        Delegates extraction details to the :func:`Extractor.extract()`
        method of the associated :class:`Extractor` instance.

        Puts extracted data, in the form of :class:`pandas.DataFrame` into
        the `original_data` attribute.

        Returns:
            ETL: self, to allow method chaining.
        """
        self.original_data = self.extractor.extract(**kwargs)
        return self

    def load(self, **kwargs):
        """Loads data, using the associated loader.

        Delegates loading details to the :func:`Loader.load()` method of the
        associated :class:`Loader` instance. Data are fetched from the
        `processed_data` attribute of the class.

        Returns:
            ETL: self, to allow method chaining
        """
        self.loader.load(**kwargs)
        return self

    def transform(self, **kwargs):
        """Transforms the data, using the associated transformation

        Delegates loading details to the :func:`Transformation.transform()` method of the
        associated :class:`Transform` instance.
        Transforms the `original_data` fetched
        by the extractor and fill the `processed_data` attribute, used by the loader.

        `original_data` keeps the original parsed data
        `processed_data` stores the processed data,
        as single `DataFrame` or as a dictionary of multiple
        `DataFrame`, with defined labels.

        Version 1.0 opdmetl ETL subclasses, directly overriding the transform method
        are back-compatible

        Returns:
            ETL: self, to allow method chaining
        """
        self.transformation.transform(**kwargs)
        return self

    def etl(self):
        """ Shortcut for ``extract().transform().load()``

        May be used when there are no parameters to be passed to the
        intermediate methods
        """
        return self.extract().transform().load()
