import unittest

from colanderalchemy import SQLAlchemySchemaNode
from sqlalchemy import Column, String

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

