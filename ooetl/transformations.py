import logging
from abc import ABCMeta, abstractmethod


class Transformation(object):
    """Base abstract class. All Transformation classes extend from this.
    """

    __metaclass__ = ABCMeta

    etl = None
    logger = logging.getLogger(__name__)

    @abstractmethod
    def transform(self, **kwargs):
        """Base transform method, must be instantiated in extended
        classes.
        """
        pass


class DummyTransformation(Transformation):
    """A dummy transformation that implements a neutral transform method
    original_data are passed to processed_data.

    This is implemented so that ETL subclasses written with ooetl 1.x
    are still compatible with the 2.x releases.
    """
    def transform(self):
        self.etl.processed_data = self.etl.original_data
