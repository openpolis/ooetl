# -*- coding: utf-8 -*-

try:
    import django.test
except ImportError as error:
    print(error.__class__.__name__ + ": " + error.message)

from unittest.mock import Mock, patch


class ETLTest(django.test.TestCase):

    @classmethod
    def setUpClass(cls):
        super(ETLTest, cls).setUpClass()
        cls.mock_get_patcher = patch('opdmetl.extractors.requests.get')
        cls.mock_get = cls.mock_get_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_get_patcher.stop()
        super(ETLTest, cls).tearDownClass()