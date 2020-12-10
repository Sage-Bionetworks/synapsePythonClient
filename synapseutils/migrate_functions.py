import concurrent.futures
from enum import Enum
import json
import logging
import sys
import tempfile
import traceback
import typing

import synapseclient
from synapseclient.core import pool_provider
from synapseclient.core import utils
from synapseclient.core.upload.multipart_upload import multipart_copy, shared_executor

"""
Contains functions for migrating the storage location of Synapse entities.
Entities can be updated or moved so that their underlying file handles are stored
in the new location.

The main migrate function can migrate an entity recursively (e.g. a Project or Folder).
Because Projects and Folders are potentially very large and can have many children entities,
the migrate function orders migrations if entities selected by first indexing them into an
internal SQLite database and then ordering their migration by Synapse id. The ordering
reduces the impact of a large migration can have on Synapse by clustering changes locally
"""


def test_import_sqlite3():
    # sqlite3 is part of the Python standard library and is available on the vast majority
    # of Python installations and doesn't require any additional software on the system.
    # it may be unavailable in some rare cases though (for example Python compiled from source
    # without ay sqlite headers available). we dynamically import it when used to avoid making
    # this dependency hard for all client usage, however.
    try:
        import sqlite3  # noqa
    except ImportError:
        sys.stderr.write("""\nThis operation requires the sqlite3 module which is not available on this
installation of python. Using a Python installed from a binary package or compiled from source with sqlite
development headers available should ensure that the sqlite3 module is available.""")
        raise


class _MigrationStatus(Enum):
    # an internal enum for use within the sqlite db
    # to track the state of entities as they are indexed
    # and then migrated
    UNINDEXED = 0
    INDEXED = 1
    MIGRATED = 2
    MIGRATION_ERROR = 3


class MigrationResult:
    """A MigrationResult is a proxy object to the underlying sqlite db.
    It provides a programmatic interface that allows the caller to iterate over the
    file handles that were migrated without having to connect to or know the schema
    of the sqlite db, and also avoids the potential memory liability of putting
    everything into an in memory data structure that could be a liability when
    migrating a huge project of hundreds of thousands/millions of entities.

    As this proxy object is not thread safe since it accesses an underlying sqlite db.
    """

    def __init__(self, db_path):
        self.db_path = db_path

    def _yield_migrations(self, query, *args):
        import sqlite3
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            entity_id = ''
            while True:
                result = cursor.execute(
                    query,
                    (entity_id, *args)
                )

                batch = [r for r in result]
                if len(batch) == 0:
                    break

                for row in batch:
                    yield row

                entity_id = row[0]

    def get_file_versions_migrated(self):
        """Returns a generator returning 4-tuples of file entity versions that were migrated
        where each tuple is (entity id, version, old file handle id, new file handle id)"""
        for result in self._yield_migrations(
            """
                select id, version, from_file_handle_id, to_file_handle_id
                from file_entity_versions
                where id > ?
                order by id
                limit ?
            """,
            _get_batch_size(),
        ):
            yield result

    def get_table_files_migrated(self):
        """Returns a generator returning 5-tuples of file entity versions that were migrated
        where each tuple is (entity id, row, column, old file handle id, new file handle id)"""
        for result in self._yield_migrations(
            """
                select id, row, column, from_file_handle_id, to_file_handle_id
                from table_entity_files
                where id > ?
                order by id
                limit ?
            """,
            _get_batch_size(),
        ):
            yield result

    def _yield_errors(self, entity_type):
        for result in self._yield_migrations(
            """
                select id, exception
                from entities
                where id > ? and type = ? and exception is not null
                order by id
                limit ?
            """,
            entity_type,
            _get_batch_size(),
        ):
            yield result

    def get_file_migration_errors(self):
        """Returns a generator returning 2-tuples of entity ids to exception strings for
        file entity migrations that resulted in an error"""
        for result in self._yield_errors('file'):
            yield result

    def get_table_migration_errors(self):
        """Returns a generator returning 2-tuples of entity ids to exception strings for
        table entity migrations that resulted in an error"""
        for result in self._yield_errors('table'):
            yield result


def _get_executor():
    executor = pool_provider.get_executor(thread_count=pool_provider.DEFAULT_NUM_THREADS)

    # default the number of concurrent file copies to half the number of threads in the pool.
    # since we share the same thread pool between managing entity copies and the multipart
    # upload, we have to prevent thread starvation if all threads are consumed by the entity
    # code leaving none for the multipart copies
    max_concurrent_file_copies = max(int(pool_provider.DEFAULT_NUM_THREADS / 2), 1)
    return executor, max_concurrent_file_copies


def _get_batch_size():
    # just a limit on certain operations to put an upper bound on various
    # batch operations so they are chunked. a function to make it easily mocked.
    # don't anticipate needing to adjust this for any real activity
    return 500


def _ensure_tables(cursor):
    # ensure we have the sqlite schema we need to be able to record and sort our
    # entity file handle migration.

    cursor.execute(
        """
            create table if not exists entities (
                id text primary key,
                parent_id text null,
                type text not null,
                status integer not null,
                exception text null
            );
        """
    )

    cursor.execute(
        """
            create table if not exists file_entity_versions (
                id text,
                version integer,
                from_file_handle_id text not null,
                to_file_handle_id text not null,
                primary key(id, version),
                foreign key (id) references entities(id)
            );
        """
    )

    cursor.execute(
        """
            create table if not exists table_entity_files (
                id text,
                row integer,
                column text,
                from_file_handle_id text not null,
                to_file_handle_id text not null,
                primary key(id, row, column),
                foreign key (id) references entities(id)
            );
        """
    )


def _wait_futures(conn, cursor, futures, return_when, continue_on_error):
    completed, futures = concurrent.futures.wait(futures, return_when=return_when)
    for completed_future in completed:
        try:
            entity_id, record_fn, mapping = completed_future.result()
            record_fn(cursor, entity_id, mapping)

            cursor.execute(
                'update entities set status = ? where id = ?',
                (_MigrationStatus.MIGRATED.value, entity_id)
            )
            conn.commit()

        except _EntityMigrationError as ex:
            logging.exception('Encountered error when migrating entity')

            # for the purposes of recording and re-raise we're not interested in
            # the _EntityMigrationError, just the underlying cause

            cause = ex.__cause__
            tb_str = ''.join(traceback.format_exception(type(cause), cause, cause.__traceback__))
            cursor.execute(
                'update entities set status = ?, exception = ? where id = ?',
                (_MigrationStatus.MIGRATION_ERROR.value, tb_str, ex.entity_id)
            )
            conn.commit()

            if not continue_on_error:
                raise cause from None

    conn.commit()
    return futures


def migrate(
        syn,
        entity,
        storage_location_id,
        version='new',
        db_path=None,
        continue_on_error=False,
):
    """
    Migrate the given Table entity to the specified storage location

    :param syn:                                 A Synapse client instance
    :param entity:                              The entity to migrate, typically a Folder or Project
    :param storage_location_id:                 The storage location where the file handle(s) will be migrated to
    :param version:                             One of the following:
                                                    'new': create a new version for entities that are migrated
                                                    'all': migrate all entities in place by updating their file handles
                                                    'latest: migrate the latest entity version in place by updating
                                                            its file handle
    :param db_path:                             a path where a SQLite database can be saved to coordinate the progress
                                                of the migration, if None provided a temp file will be used
    :param continue_on_error:                   False if an error when migrating an individual entity should abort
                                                the entire migration, True if the migration should continue to other
                                                entities

    :returns: a 5-tuple consisting detailing the results of the migration;
        * map of file entity id to (version number, old file handle id, new file handle id)
        * a list of file entities that were not able to be migrated (exceptions recorded a the sqlite db)
        * map of tables with attached file handles that were migrated,
            entity id to (row id, old file handle id, new file handle id)
        * a list of tables that were not able to be migrated (exceptions recorded in the sqlite db)
        * path to the sqlite db
    """

    if db_path is None:
        db_path = tempfile.NamedTemporaryFile(delete=False).name

    executor, max_concurrent_file_copies = _get_executor()

    test_import_sqlite3()
    import sqlite3
    with sqlite3.connect(db_path) as conn:

        cursor = conn.cursor()
        _ensure_tables(cursor)
        conn.commit()

        entity = syn.get(entity, downloadFile=False)
        if not _check_indexed(cursor, entity):
            _index_entity(conn, cursor, syn, entity, None)

        entity_id = ''
        futures = set()

        while True:
            if len(futures) >= max_concurrent_file_copies:
                futures = _wait_futures(
                    conn,
                    cursor,
                    futures,
                    concurrent.futures.FIRST_COMPLETED,
                    continue_on_error,
                )

            results = cursor.execute(
                """
                    select id, type
                    from entities
                    where id > ? and type in ('file', 'table')
                    order by id asc
                    limit ?
                """,
                (entity_id, _get_batch_size())
            )

            row_count = 0

            for row in results:
                entity_id, entity_type = row
                migrate_args = [syn, entity_id]
                migrate_kwargs = {'storage_location_id': storage_location_id}

                if entity_type == 'file':
                    migrate_fn = migrate_file
                    record_fn = _record_file_migration
                    migrate_kwargs['version'] = version

                elif entity_type == 'table':
                    migrate_fn = migrate_table
                    record_fn = _record_table_migration

                    # we create a table snapshot if running in "new" mode (i.e. we are creating
                    # a new version of each entity with the updated file handles
                    migrate_kwargs['create_snapshot'] = version == 'new'

                else:
                    raise ValueError('Unexpected entity type not migratable: {}'.format(entity_type))

                futures.add(
                    executor.submit(
                        _migrate_entity(
                            executor,
                            entity_id,
                            migrate_fn,
                            record_fn,
                            *migrate_args,
                            **migrate_kwargs
                        )
                    )
                )

                row_count += 1

            if row_count == 0:
                # no more entities to migrate
                break

    if futures:
        _wait_futures(conn, cursor, futures, concurrent.futures.ALL_COMPLETED, continue_on_error)

    return MigrationResult(db_path)


def _migrate_entity(executor, entity_id, migrate_fn, record_fn, *args, **kwargs):
    # a wrapped closure to submit to an Executor so that we can return some
    # state to the consumer of a Future that isn't otherwise returned by
    # the underyling entity migration methods
    def _migrate_fn():
        with shared_executor(executor):
            try:
                mapping = migrate_fn(*args, **kwargs)
                return entity_id, record_fn, mapping
            except Exception as ex:
                # we need to be able to pass up the entity id to the Future handler
                # which does not have the original invoking scope
                raise _EntityMigrationError(entity_id) from ex
    return _migrate_fn


def _record_file_migration(cursor, entity_id, mapping):
    # record all the file handle migrations associated with a FileEntity migration
    # (can be multiple of multiple versions where migrated)
    insert_values = []
    for version, file_handle_ids in mapping.items():
        from_file_handle_id, to_file_handle_id = file_handle_ids
        insert_values.append((entity_id, version, from_file_handle_id, to_file_handle_id))

    cursor.executemany(
        """
            insert into file_entity_versions (id, version, from_file_handle_id, to_file_handle_id)
            values (?, ?, ?, ?)
        """,
        insert_values
    )


def _record_table_migration(cursor, entity_id, mapping):
    # record all the file handle migrations associated with a TableEntity migration
    # (can be multiple if there were multiple rows/columns of file handles)
    insert_values = []
    for row_id, col_mapping in mapping.items():
        for col_name, file_handle_ids in col_mapping.items():
            from_file_handle_id, to_file_handle_id = file_handle_ids
            insert_values.append((entity_id, row_id, col_name, from_file_handle_id, to_file_handle_id))

    cursor.executemany(
        """
            insert into table_entity_files (id, row, column, from_file_handle_id, to_file_handle_id)
            values (?, ?, ?, ?, ?)
        """,
        insert_values
    )


def _entity_type_of(entity):
    concrete_type = utils.concrete_type_of(entity)

    # e.g. 'org.sagebionetworks.repo.model.FileEntity' -> 'file'
    concrete_type_short = concrete_type.rsplit('.', 1)[-1].lower().replace('entity', '')
    return concrete_type_short


def _check_indexed(cursor, entity):
    # check if we have indexed the given entity in the sqlite db yet.
    # if so it can skip reindexing it. supports resumption.
    entity_id = utils.id_of(entity)
    indexed_row = cursor.execute(
        "select 1 from entities where id = ? and status >= ?",
        (entity_id, _MigrationStatus.INDEXED.value)
    ).fetchone()

    if indexed_row:
        logging.info('%s already indexed, skipping', entity_id)
        return True

    logging.debug('%s not yet indexed, indexing now', entity_id)
    return False


def _index_entity(conn, cursor, syn, entity, parent_id):
    # recursive function to index a given entity into the sqlite db.

    entity_id = utils.id_of(entity)

    if not _check_indexed(cursor, entity_id):
        entity_type = _entity_type_of(entity)

        if entity_type == 'file' or entity_type == 'table':
            cursor.execute(
                'insert into entities (id, parent_id, type, status) values (?, ?, ?, ?)',
                (entity_id, parent_id, entity_type, _MigrationStatus.INDEXED.value)
            )

        elif entity_type == 'folder' or entity_type == 'project':
            children = syn.getChildren(entity_id, includeTypes=['file', 'table', 'folder'])
            for child in children:
                _index_entity(conn, cursor, syn, child, entity_id)

            # once all the children are recursively indexed we mark this parent itself as indexed
            container_type = _entity_type_of(entity)
            cursor.execute(
                "insert into entities (id, parent_id, type, status) values (?, ?, ?, ?)",
                [entity_id, parent_id, container_type, _MigrationStatus.INDEXED.value]
            )

        conn.commit()


def migrate_file(
        syn,
        entity,
        storage_location_id: str,
        version: typing.Union[str, int] = 'new'
) -> typing.Mapping[typing.Tuple[int], typing.Tuple[str, str]]:
    """
    Migrate the given FileEntity.

    :param syn:                                 A Synapse client instance
    :param entity:                              A FileEntity or the id of a Synapse file
    :param storage_location_id:                 The storage location where the file handle(s) will be migrated to
    :param version:                             Describes which version(s) of the file entity to migrate:
                                                'new':  (default) create a new version of the entity
                                                'all':  migrate the file handle(s) of all revisions
                                                'latest': migrate the file handle associated with latest revision
                                                (int):  migrate the file handle associated with specified version

    :returns: a mapping of (old version, new version) -> (old storage location id, new storage location id)
        representing the migrated entity versions
    """

    entity = syn.get(entity, downloadFile=False)
    if not isinstance(entity, synapseclient.File):
        raise ValueError('passed value is not a FileEntity')
    if version not in ('new', 'all', 'latest') and not isinstance(version, int):
        raise ValueError("invalid value {} passed for version".format(version))

    mapping = {}
    if version == 'new':
        try:
            version, from_file_handle_id, to_file_handle_id = _create_new_file_version(
                syn,
                entity,
                storage_location_id
            )
            mapping[version] = (from_file_handle_id, to_file_handle_id)

        except _AlreadyMigratedException:
            # most recent entity revision is already stored in the destination storage location
            pass

    elif version == 'all':
        version_mapping = _migrate_all_file_versions(syn, entity, storage_location_id)
        mapping.update(version_mapping)

    else:
        if version == 'latest':
            # internally for the purposes of syn.get latest means we pass no value for version
            version = None
        # otherwise we know version is an integer

        # we are either migrating the most recent revision (None) or a specific passed revision
        try:
            migrated_version, from_file_handle_id, to_file_handle_id = _migrate_file_version(
                syn,
                entity,
                storage_location_id,
                version,
            )
            mapping[migrated_version] = (from_file_handle_id, to_file_handle_id)

        except _AlreadyMigratedException:
            # specified revision is already stored in the destination storage location
            pass

    return mapping


def _create_new_file_version(syn, entity, storage_location_id):
    existing_file_handle_id = entity.dataFileHandleId
    existing_file_name = entity._file_handle['fileName']
    existing_storage_location_id = entity._file_handle['storageLocationId']

    if str(existing_storage_location_id) == str(storage_location_id):
        logging.info(
            'Skipped creating a new version of file %s, it is already in the destination storage location (%s)',
            entity.id,
            storage_location_id
        )
        raise _AlreadyMigratedException()

    source_file_handle_association = {
        'fileHandleId': existing_file_handle_id,
        'associateObjectId': entity.id,
        'associateObjectType': 'FileEntity',
    }

    new_file_handle_id = multipart_copy(
        syn,
        source_file_handle_association,
        dest_file_name=existing_file_name,
        storage_location_id=storage_location_id,
    )

    entity.dataFileHandleId = new_file_handle_id
    entity = syn.store(entity)

    return entity.versionNumber, existing_file_handle_id, new_file_handle_id


def _migrate_all_file_versions(syn, entity, storage_location_id):
    mapping = {}
    entity_id = utils.id_of(entity)

    for version in syn._GET_paginated("/entity/{id}/version".format(id=entity_id)):
        version_number = version['versionNumber']
        try:
            _, from_file_handle_id, to_file_handle_id = _migrate_file_version(
                syn,
                entity,
                storage_location_id,
                version_number,
            )

            mapping[version_number] = (from_file_handle_id, to_file_handle_id)

        except _AlreadyMigratedException:
            pass

    return mapping


def _migrate_file_version(syn, entity, storage_location_id, version):
    # if walking a container the children entities passed will be the light weight
    # dictionary representation of the FileEntity, so we retrieve the full entity
    entity_id = utils.id_of(entity)
    entity = syn.get(entity, downloadFile=False, version=version)
    version = entity.versionNumber

    existing_file_handle_id = entity.dataFileHandleId
    existing_file_name = entity._file_handle['fileName']
    existing_storage_location_id = entity._file_handle['storageLocationId']

    if str(existing_storage_location_id) == str(storage_location_id):
        logging.info(
            'Skipped migrating file %s, version %s, it is already in the destination storage location (%s)',
            entity_id,
            entity['versionNumber'],
            storage_location_id
        )
        raise _AlreadyMigratedException()

    source_file_handle_assocation = {
        'fileHandleId': existing_file_handle_id,
        'associateObjectId': entity.id,
        'associateObjectType': 'FileEntity',
    }

    new_file_handle_id = multipart_copy(
        syn,
        source_file_handle_assocation,
        dest_file_name=existing_file_name,
        storage_location_id=storage_location_id,
    )

    file_handle_update_request = {
        'oldFileHandleId': entity.dataFileHandleId,
        'newFileHandleId': new_file_handle_id,
    }

    # no response, we rely on a 200 here
    syn.restPUT(
        "/entity/{id}/version/{versionNumber}/filehandle".format(
            id=utils.id_of(entity),
            versionNumber=version,
        ),
        json.dumps(file_handle_update_request),
    )

    logging.info(
        'Migrated file % version % to storage location %s',
        entity_id,
        version,
        storage_location_id,
    )

    return version, existing_file_handle_id, new_file_handle_id


def migrate_table(
    syn,
    entity,
    storage_location_id: str,
    create_snapshot=True
):
    """
    Migrate the given Table entity to the specified storage location

    :param syn:                                 A Synapse client instance
    :param entity:                              A Table entity or the id of a Synapse one
    :param storage_location_id:                 The storage location where the file handle(s) will be migrated to
    :param create_snapshot:                     Whether a snapshot should be created prior to the migration.

    :returns: a mapping of {row_id: {column: (old_file_handle_id, new_file_handle_id)}} that represents each
        of the attached file handles that were changed in the table
    """

    entity = syn.get(entity, downloadFile=False)
    if not isinstance(entity, synapseclient.Schema):
        raise ValueError('passed value is not a table Schema')

    entity_id = utils.id_of(entity)

    file_columns = []
    for col in syn.restGET("/entity/{id}/column".format(id=entity_id))['results']:
        if col['columnType'] == 'FILEHANDLEID':
            file_columns.append(col)

    mapping = {}
    if file_columns:
        if create_snapshot:
            # we are about to make changes so we create a snapshot version
            # first if that was requested
            syn.create_snapshot_version(entity)

        # this table has a file column that may contain files that need to be migrated
        file_column_names = [c['name'] for c in file_columns]
        select_cols = ','.join(file_column_names)
        results = syn.tableQuery("select {} from {}".format(select_cols, entity_id))

        partial_changes = {}

        for row in results:
            # first two cols are row id and row version, rest are file handle ids from our query
            row_id = row[0]

            row_id_changes = mapping.setdefault(row_id, {})
            migrated_file_handle_ids = []

            for i, file_handle_id in enumerate(row[2:]):
                col_name = file_column_names[i]
                source_file_handle_association = {
                    'fileHandleId': file_handle_id,
                    'associateObjectId': entity_id,
                    'associateObjectType': 'TableEntity',
                }

                migrated_file_handle_id = multipart_copy(
                    syn,
                    source_file_handle_association,
                    storage_location_id=storage_location_id
                )
                migrated_file_handle_ids.append(migrated_file_handle_id)
                row_id_changes[col_name] = (file_handle_id, migrated_file_handle_id)

            row_changes = {col: val for col, val in zip(file_column_names, migrated_file_handle_ids)}
            partial_changes[row_id] = row_changes

            if len(partial_changes) % _get_batch_size() == 0:
                partial_rowset = synapseclient.PartialRowset.from_mapping(partial_changes, results)
                syn.store(partial_rowset)
                partial_changes = {}

        if partial_changes:
            partial_rowset = synapseclient.PartialRowset.from_mapping(partial_changes, results)
            syn.store(partial_rowset)

    return mapping


class _AlreadyMigratedException(ValueError):
    pass


class _EntityMigrationError(Exception):
    def __init__(self, entity_id):
        self.entity_id = entity_id
