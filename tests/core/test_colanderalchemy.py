"""
Test how well CA (a.k.a. ColanderAlchemy) plays as a ResourceSchema
"""

import unittest

from colanderalchemy import SQLAlchemySchemaNode
from sqlalchemy import Column, String, Float

from kinto.core.resource.sqlalchemy import Base, schema_setup

key = SQLAlchemySchemaNode.sqla_info_key


class ColanderAlchemyTest(unittest.TestCase):

    def tearDown(self):
        Base.metadata.remove(Base.metadata.tables['dummy'])

    def test_global_preparer(self):
        """
        CA will search for a global ```global_preparer``` method and apply it to the object
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String)

            @staticmethod
            def global_preparer(value):
                value['name'] = '{}-000'.format(value['name'])
                return value

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'Name-000')

    def test_attr_default_preparer(self):
        """
        CA will search for a default attribute preparer (which name is <attr_name>_preparer) and append it
        automatically to the Schema
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String)

            @staticmethod
            def name_preparer(value):
                return value.upper()

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'NAME')

    def test_attr_named_preparer(self):
        """
        If a preparer is attached to the attribute (via the Column.info attribute) CA will append it to the schema
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String, info={key: {'preparer': 'adhoc_preparer'}})

            @staticmethod
            def adhoc_preparer(value):
                return value.lower()

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'name')

    def test_attr_named_preparer_wins(self):
        """
        If both a default preparer and a named preparer exists, the one that is defined in the _info_ dict is used
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String, info={key: {'preparer': 'adhoc_preparer'}})

            @staticmethod
            def name_preparer(value):
                return value.upper()

            @staticmethod
            def adhoc_preparer(value):
                return value.lower()

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'name')

    def test_global_named_preparers(self):
        """
        If a global preparer exists for the schema (that name has to be ```global_preparer```) it will be
        applied automatically
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String, info={key: {'preparer': 'adhoc_preparer'}})

            @staticmethod
            def global_preparer(value):
                value['name'] = '{}-000'.format(value['name'])
                return value

            @staticmethod
            def adhoc_preparer(value):
                return value.lower()

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'name-000')

    def test_global_default_preparers(self):
        """
        If a global preparer and a default preparer is defined, they should be applied automatically
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String)

            @staticmethod
            def global_preparer(value):
                value['name'] = '{}-000'.format(value['name'])
                return value

            @staticmethod
            def name_preparer(value):
                return value.upper()

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name'))
        self.assertEqual(appstruct.name, 'NAME-000')

    def test_more_than_one_preparer(self):
        """
        With more than one attribute, make sure all are applied and working
        """

        class Dummy(Base):
            __tablename__ = "dummy"
            name = Column(String)
            price = Column(Float)

            @staticmethod
            def name_preparer(value):
                return value.upper()

            @staticmethod
            def price_preparer(value):
                return value * float(1.21)

            @staticmethod
            def global_preparer(value):
                value['name'] = '{}-{}'.format(value['name'], value['price'])
                return value

        schema_setup(None, Dummy)
        appstruct = Dummy.deserialize(dict(name='Name', price=float(100)))
        self.assertEqual(appstruct.price, float(121.0))
        self.assertEqual(appstruct.name, 'NAME-121.0')
