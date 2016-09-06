import datetime

from pyramid_sqlalchemy import BaseObject, Session, metadata
from sqlalchemy import Column
from sqlalchemy import DateTime, String, Integer
from sqlalchemy import select, func, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only
from sqlalchemy.sql import label, and_

import transaction

from ... import logger
from ...utils import classname, COMPARISON
from ...storage import StorageBase
from ...storage import DEFAULT_ID_FIELD, DEFAULT_MODIFIED_FIELD, DEFAULT_DELETED_FIELD
from ...storage.exceptions import RecordNotFoundError
from ...storage.sqlalchemy.client import create_from_config
from ...storage.sqlalchemy.generators import IntegerId
from ...storage.sqlalchemy.exceptions import process_unicity_error


class Deleted(BaseObject):
    __tablename__ = "deleted"
    id = Column(Integer(), nullable=False, primary_key=True)
    parent_id = Column(String(), nullable=False, primary_key=True)
    collection_id = Column(String(), nullable=False, primary_key=True)
    last_modified = Column(DateTime(), nullable=False)


class Timestamps(BaseObject):
    __tablename__ = "timestamps"
    parent_id = Column(String(), primary_key=True)
    collection_id = Column(String(), primary_key=True)
    last_modified = Column(DateTime, nullable=False)


@event.listens_for(Session, 'before_flush')
def populate_timestamps_table(session, flush_context, instances):
    for parent_id, collection in filter_instances(session.new):
        timestamp = session.query(Timestamps).get([parent_id, collection])
        if timestamp:
            timestamp.last_modified = datetime.datetime.utcnow()
        else:
            timestamp = Timestamps(parent_id=parent_id, collection_id=collection,
                                   last_modified=datetime.datetime.utcnow())
        session.merge(timestamp)


def filter_instances(instances):
    return set([(i.parent_id, classname(i)) for i in instances if getattr(i, 'is_timestamp_trackeable', False)])


class Storage(StorageBase):

    id_generator = IntegerId()

    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)

    def initialize_schema(self, dry_run=False):
        """Create every necessary objects (like tables or indices) in the
        backend.

        This is excuted when the ``cliquet migrate`` command is ran.
        """
        if not dry_run:
            self.flush()

    def flush(self, auth=None):
        """Remove **every** object from this storage.
        """
        metadata.drop_all()
        metadata.create_all()

    def collection_timestamp(self, collection_id, parent_id, auth=None):
        """Get the highest timestamp of every objects in this `collection_id` for
        this `parent_id`.

        .. note::

            This should take deleted objects into account.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :returns: the latest timestamp of the collection.
        :rtype: int
        """
        tb = Timestamps.__table__
        qry = select([label('last_modified', func.max(tb.c.last_modified))]).where(and_(
                                                                                   tb.c.parent_id == parent_id,
                                                                                   tb.c.collection_id == collection_id))
        last_modified,  = Session.execute(qry).fetchone()
        if last_modified is None:
            last_modified = datetime.datetime.utcnow()
            with transaction.manager:
                Session.add(Timestamps(parent_id=parent_id, collection_id=collection_id,
                                       last_modified=last_modified))
        return last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    def create(self, collection_id, parent_id, record, id_generator=None,
               unique_fields=None, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        """Create the specified `object` in this `collection_id` for this `parent_id`.
        Assign the id to the object, using the attribute
        :attr:`cliquet.resource.Model.id_field`.

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.UnicityError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param dict record: the object to create.

        :returns: the newly created object.
        :rtype: dict
        """
        obj = self.collection.serialize(record)
        obj.parent_id = parent_id
        setattr(obj, modified_field, datetime.datetime.utcnow())
        try:
            Session.add(obj)
            Session.flush()
        except IntegrityError as e:
            logger.exception('Object %s for collection %s raised %s', record, self.collection, e)
            process_unicity_error(e, Session, self.collection, record)
        # TODO: store new timestamps date
        return self.collection.deserialize(obj)

    def get(self, collection_id, parent_id, object_id,
            id_field=DEFAULT_ID_FIELD,
            modified_field=DEFAULT_MODIFIED_FIELD,
            auth=None):
        """Retrieve the object with specified `object_id`, or raise error
        if not found.

        :raises: :exc:`cliquet.storage.exceptions.RecordNotFoundError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object

        :returns: the object object.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None or obj.deleted:
            raise RecordNotFoundError()
        return obj.deserialize()

    def update(self, collection_id, parent_id, object_id, object,
               unique_fields=None, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        """Overwrite the `object` with the specified `object_id`.

        If the specified id is not found, the object is created with the
        specified id.

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.UnicityError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object
        :param dict object: the object to update or create.

        :returns: the updated object.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None:
            obj = self.create(collection_id=collection_id, parent_id=parent_id,
                              record=object, unique_fields=unique_fields,
                              id_field=id_field, modified_field=modified_field,
                              auth=None)
        else:
            for k, v in object.items():
                setattr(obj, k, v)
        return obj.deserialize()

    def delete(self, collection_id, parent_id, object_id,
               with_deleted=True, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               deleted_field=DEFAULT_DELETED_FIELD,
               auth=None, **kwargs):
        """Delete the object with specified `object_id`, and raise error
        if not found.

        Deleted objects must be removed from the database, but their ids and
        timestamps of deletion must be tracked for synchronization purposes.
        (See :meth:`cliquet.storage.StorageBase.get_all`)

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.RecordNotFoundError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object
        :param bool with_deleted: track deleted record with a tombstone

        :returns: the deleted object, with minimal set of attributes.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None or getattr(obj, deleted_field):
            raise RecordNotFoundError()
        setattr(obj, deleted_field, True)
        setattr(obj, modified_field, datetime.datetime.utcnow())
        Session.add(Deleted(id=object_id, parent_id=parent_id,
                            collection_id=collection_id,
                            last_modified=getattr(obj, modified_field)))
        return obj.deserialize()

    def delete_all(self, collection_id, parent_id, filters=None,
                   with_deleted=True, id_field=DEFAULT_ID_FIELD,
                   modified_field=DEFAULT_MODIFIED_FIELD,
                   deleted_field=DEFAULT_DELETED_FIELD,
                   auth=None):
        """Delete all objects in this `collection_id` for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param filters: Optionnally filter the objects to delete.
        :type filters: list of :class:`cliquet.storage.Filter`
        :param bool with_deleted: track deleted records with a tombstone

        :returns: the list of deleted objects, with minimal set of attributes.
        :rtype: list of dict
        """
        qry = Session.query(self.collection).options(load_only('id'))\
                     .filter(and_(self.collection.parent_id == parent_id,
                                  getattr(self.collection, deleted_field) == False))
        for every in filters:
            qry = qry.filter(SQLAFilter(self.collection, every)())
        rows = [{"id": every.id, "parent_id": parent_id, "collection_id": collection_id,
                 modified_field: datetime.datetime.utcnow()} for every in qry.all()]
        Session.bulk_update_mappings(self.collection,
                                     [{"id": every['id'], deleted_field: True,
                                       modified_field: every[modified_field]} for every in rows])
        if with_deleted:
            Session.bulk_insert_mappings(Deleted, rows)
        return rows

    def purge_deleted(self, collection_id, parent_id, before=None,
                      id_field=DEFAULT_ID_FIELD,
                      modified_field=DEFAULT_MODIFIED_FIELD,
                      auth=None):
        """Delete all deleted object tombstones in this `collection_id`
        for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param int before: Optionnal timestamp to limit deletion (exclusive)

        :returns: The number of deleted objects.
        :rtype: int

        """
        tb = Deleted.__table__
        rst = Session.execute(tb.delete().where(and_(tb.c.parent_id == parent_id, tb.c.collection_id == collection_id)))
        return rst

    def get_all(self, collection_id, parent_id, filters=None, sorting=None,
                pagination_rules=None, limit=None, include_deleted=False,
                id_field=DEFAULT_ID_FIELD,
                modified_field=DEFAULT_MODIFIED_FIELD,
                deleted_field=DEFAULT_DELETED_FIELD,
                auth=None):
        """Retrieve all objects in this `collection_id` for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param filters: Optionally filter the objects by their attribute.
            Each filter in this list is a tuple of a field, a value and a
            comparison (see `cliquet.utils.COMPARISON`). All filters
            are combined using *AND*.
        :type filters: list of :class:`cliquet.storage.Filter`

        :param sorting: Optionnally sort the objects by attribute.
            Each sort instruction in this list refers to a field and a
            direction (negative means descending). All sort instructions are
            cumulative.
        :type sorting: list of :class:`cliquet.storage.Sort`

        :param pagination_rules: Optionnally paginate the list of objects.
            This list of rules aims to reduce the set of objects to the current
            page. A rule is a list of filters (see `filters` parameter),
            and all rules are combined using *OR*.
        :type pagination_rules: list of list of :class:`cliquet.storage.Filter`

        :param int limit: Optionnally limit the number of objects to be
            retrieved.

        :param bool include_deleted: Optionnally include the deleted objects
            that match the filters.

        :returns: the limited list of objects, and the total number of
            matching objects in the collection (deleted ones excluded).
        :rtype: tuple (list, integer)
        """
        self.qry = Session.query(self.collection)  #.filter(self.collection.parent_id == parent_id)
        # TODO: verify permissions
        total_records = self.qry.count()
        if not include_deleted:
            self.qry = self.qry.filter(getattr(self.collection, deleted_field) == False)
        self._apply_filters(filters)
        self._apply_orderby(sorting)
        if limit:
            self.qry = self.qry.limit(limit=limit)
        return [every.deserialize() for every in self.qry.all()], total_records

    def _apply_filters(self, filters):
        filter_clause = []
        for every in filters:
            filter_clause.append(SQLAFilter(self.collection, every)())
        if filter_clause:
            self.qry = self.qry.filter(*filter_clause)

    def _apply_orderby(self, sorting):
        order_by_clause = []
        for every in sorting:
            order_by_clause.append(SQLSort(self.collection, every)())
        if order_by_clause:
            self.qry = self.qry.order_by(*order_by_clause)


class SQLAFilter(object):

    iterables = (COMPARISON.IN, COMPARISON.EXCLUDE)
    sqla_enum_conversion = {COMPARISON.EQ: '=',
                            COMPARISON.IN: 'in_',
                            COMPARISON.EXCLUDE: 'notin_'}

    def __init__(self, collection, criteria):
        self.attribute = getattr(collection, criteria.field)
        self.value = criteria.value
        self.operator = criteria.operator
        self.sqla_operator = self.sqla_enum_conversion.setdefault(criteria.operator, criteria.operator.value)

    def __call__(self):
        if self.operator in (COMPARISON.EXCLUDE, COMPARISON.IN):
            return getattr(self.attribute.comparator, self.sqla_operator)(self.value)
        return self.attribute.op(self.sqla_operator)(self.value)


class SQLSort(object):

    sql_sort_enum = {-1: 'desc', 1: 'asc'}

    def __init__(self, collection, sorting):
        self.attribute = getattr(collection, sorting.field)
        self.ordering = self.sql_sort_enum[sorting.direction]

    def __call__(self):
        return getattr(self.attribute, self.ordering)()


def load_from_config(config):
    settings = config.get_settings()
    max_fetch_size = int(settings['storage_max_fetch_size'])
    create_from_config(config, prefix='storage_')
    return Storage(max_fetch_size=max_fetch_size)
