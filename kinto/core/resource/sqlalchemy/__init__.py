import datetime

import colander
from colanderalchemy import SQLAlchemySchemaNode
from pyramid_sqlalchemy import BaseObject, metadata
from sqlalchemy import Column, event
from sqlalchemy import String, Boolean, Integer, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper

from .. import logger
from ...resource import ShareableResource, ResourceSchema

key = SQLAlchemySchemaNode.sqla_info_key


class NoSchemaException(Exception):

    def __init__(self, klass):
        logger.exception('Schema for collection {} has not been set'.format(klass.__name__))

    def __str__(self):
        return 'Schema not set for model. Have you executed configure_mappers?'


class SQLABaseObject(object):

    _track_timestamp = True

    id = Column(Integer(), primary_key=True, info={key: {'repr': True}})
    parent_id = Column(String(), nullable=False, index=True, info={key: {'exclude': True}})
    last_modified = Column(BigInteger(), nullable=False, default=lambda: datetime.datetime.utcnow())
    deleted = Column(Boolean(), default=False, index=True, info={key: {'exclude': True}})

    @property
    def is_timestamp_trackeable(self):
        """True if this object will be used to track the last time the collection it belongs to has been accessed"""
        return self._track_timestamp

    @property
    def last_modified_timestamp(self):
        return self.last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    @classmethod
    def deserialize(cls, cstruct, context=None):
        try:
            appstruct = cls.__schema__.deserialize(cstruct)
            return cls.__schema__.objectify(appstruct, context)
        except AttributeError:
            raise NoSchemaException(cls)

    @classmethod
    def serialize(cls, appstruct):
        try:
            return cls.__schema__.serialize(appstruct)
        except AttributeError:
            raise NoSchemaException(cls)

    def __repr__(self):
        return '{class_name}{attributes}'.format(class_name=self.__class__.__name__,
                                                 attributes=self.__repr_attributes())

    def __repr_attributes(self):
        return dict([(att.name, getattr(self, att.name)) for att in self.__schema__.children
                                                                               if hasattr(att, 'repr') and att.repr])

    @staticmethod
    def global_preparer(value):
        return value

    @staticmethod
    def global_validator(node, value):
        return True


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
    """
    Evaluate if klass has a __schema__ attribute already attached. In case it does not, create it and later
    evaluate preparers and validators
    """
    if not hasattr(klass, '__schema__'):
        klass.__schema__ = SQLASchemaResource(klass)
        MethodAppender(klass).append_methods()


class MethodAppender(object):

    methods = ('preparer', 'validator')

    def __init__(self, klass):
        self.klass = klass

    def append_methods(self):
        """
        Evaluate if default preparers and validators have to be attach, first in the class and then in each child
        """
        self._conversion(self.klass.__schema__, 'global')
        for child in self.klass.__schema__.children:
            self._conversion(child, child.name)

    def _conversion(self, pointer, keyword):
        """
        For each pointer, append preparers and validators in case they exist
        """
        self._append_preparer(pointer, keyword)
        self._append_validator(pointer, keyword)

    def _append_preparer(self, pointer, keyword):
        """
        If preparer is null, append global default preparer for class or attribute. If it is a string, search
        for such method in class.
        """
        if pointer.preparer is None:
            pointer.preparer = getattr(self.klass, '{}_preparer'.format(keyword), None)
        elif isinstance(pointer.preparer, str):
            pointer.preparer = getattr(self.klass, pointer.preparer, None)

    def _append_validator(self, pointer, keyword):
        """
        Evaluate _validator_ attribute with the following rules:
         * append default global validator or attribute validator in case they exist to global list
         * if defined validator is a string, look for it in the klass methods and append to global list
         * if validator is a list, append them to global list (iterating them in case some of them are strings to
           be located)
         * if they are already methods append them to global list
        Once the list has been collected, _set_method will evaluate it and transform to the right thing
        """
        methods_list = [getattr(self.klass, '{keyword}_validator'.format(keyword=keyword), None)]
        if isinstance(pointer.validator, str):
            methods_list.append(getattr(self.klass, pointer.validator))
        elif hasattr(pointer.validator, '__iter__'):
            for each in pointer.validator:
                if isinstance(each, str):
                    methods_list.append(getattr(self.klass, each, None))
                else:
                    methods_list.append(each)
        elif pointer.validator is not None:
            methods_list.append(pointer.validator)
        self._set_method(pointer, methods_list)

    def _set_method(self, pointer, methods_list):
        """
        Remove null items from list of methods and in case only one method remain, append it directly as validator.
        Otherwise wrap them with Colander.All()
        """
        clean_list = [i for i in methods_list if i is not None]
        if len(clean_list) == 1:
            pointer.validator = clean_list[0]
        elif len(clean_list) > 1:
            pointer.validator = colander.All(*clean_list)


event.listen(mapper, 'mapper_configured', schema_setup)

Base = declarative_base(cls=(BaseObject, SQLABaseObject), metadata=metadata)
