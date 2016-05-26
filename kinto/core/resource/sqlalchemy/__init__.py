import datetime

from .. import logger
from colanderalchemy import SQLAlchemySchemaNode, setup_schema
from pyramid_sqlalchemy import BaseObject, metadata
from sqlalchemy import Column, event
from sqlalchemy import String, Boolean, Integer, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper

from ...resource import ShareableResource, ResourceSchema

key = SQLAlchemySchemaNode.sqla_info_key


class SQLABaseObject(object):

    _track_timestamp = True

    id = Column(Integer(), primary_key=True, info={key: {'repr': True}})
    parent_id = Column(String(), nullable=False, index=True, info={key: {'exclude': True}})
    last_modified = Column(BigInteger(), nullable=False)
    deleted = Column(Boolean(), default=False, index=True, info={key: {'exclude': True}})

    @property
    def is_timestamp_trackeable(self):
        """True if this object will be used to track the last time the collection it belongs to has been accessed"""
        return self._track_timestamp

    @property
    def last_modified_timestamp(self):
        return self.last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    def deserialize(self):
        try:
            return self.__schema__.dictify(self)
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self)
            raise Exception('Schema not set for model')

    def serialize(self, dict_, context=None):
        try:
            return self.__schema__.objectify(dict_, context)
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self)
            raise Exception('Schema not set for model')

    def __repr__(self):
        return '{class_name}{attributes}'.format(class_name=self.__class__.__name__,
                                                 attributes=self.__repr_attributes())

    def __repr_attributes(self):
        return dict([(att.name, getattr(self, att.name)) for att in self.__schema__.children
                                                                               if hasattr(att, 'repr') and att.repr])


class SQLASchemaResource(SQLAlchemySchemaNode, ResourceSchema):

    def __init__(self, class_):
        super(SQLASchemaResource, self).__init__(class_=class_)


class SQLAUserResource(ShareableResource):

    def __init__(self, request, context=None):
        super(SQLAUserResource, self).__init__(request, context)
        self.model.storage.collection = self.appmodel
        if not hasattr(self.model.storage.collection, '__schema__'):
            setattr(self.model.storage.collection, '__schema__', SQLASchemaResource(self.appmodel))
        self.mapping = self.model.storage.collection.__schema__


def schema_setup(mapper, klass):
    if not hasattr(klass, '__schema__'):
        klass.__schema__ = SQLASchemaResource(klass)

event.listen(mapper, 'mapper_configured', schema_setup)


def append_schema(target, context):
    if not hasattr(target, '__schema__'):
        target.__schema__ = SQLASchemaResource(target)


Base = declarative_base(cls=(BaseObject, SQLABaseObject), metadata=metadata)
