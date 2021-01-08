import concurrent.futures
import csv
from enum import Enum
import json
import logging
import sys
import traceback
import typing

import synapseclient
from synapseclient.core.logging_setup import DEFAULT_LOGGER_NAME
from synapseclient.core.constants import concrete_types
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


logger = logging.getLogger(DEFAULT_LOGGER_NAME)


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
    # and then migrated.
    INDEXED = 1
    MIGRATED = 2
    ALREADY_MIGRATED = 3
    ERRORED = 4


class _MigrationType(Enum):
    # container types (projects and folders) are only used during the indexing phase.
    # we record the containers we've indexed so we don't reindex them on a subsequent
    # run using the same db file (or reindex them after an indexing dry run)
    PROJECT = 1
    FOLDER = 2

    # files and table attached files represent file handles that are actually migrated
    FILE = 3
    TABLE_ATTACHED_FILE = 4

    @classmethod
    def from_concrete_type(cls, concrete_type):
        if concrete_type == concrete_types.PROJECT_ENTITY:
            return cls.PROJECT
        elif concrete_type == concrete_types.FOLDER_ENTITY:
            return cls.FOLDER
        elif concrete_type == concrete_types.FILE_ENTITY:
            return cls.FILE
        elif concrete_type == concrete_types.TABLE_ENTITY:
            return cls.TABLE_ATTACHED_FILE

        raise ValueError("Unhandled type {}".format(concrete_type))


class _MigrationKey(typing.NamedTuple):
    id: str
    type: _MigrationType
    version: int
    row_id: int
    col_id: int


def _get_row_dict(cursor, row, include_empty):
    return {
        col[0]: row[i] for i, col in enumerate(cursor.description)
        if (include_empty or row[i] is not None) and col[0] != 'rowid'
    }


class MigrationResult:
    """A MigrationResult is a proxy object to the underlying sqlite db.
    It provides a programmatic interface that allows the caller to iterate over the
    file handles that were migrated without having to connect to or know the schema
    of the sqlite db, and also avoids the potential memory liability of putting
    everything into an in memory data structure that could be a liability when
    migrating a huge project of hundreds of thousands/millions of entities.

    As this proxy object is not thread safe since it accesses an underlying sqlite db.
    """

    def __init__(self, syn, db_path):
        self._syn = syn
        self.db_path = db_path

    def get_counts_by_status(self):
        import sqlite3
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # for the purposes of these counts, containers (Projects and Folders) do not count.
            # we are counting actual files only
            result = cursor.execute(
                'select status, count(*) from migrations where type in (?, ?) group by status',
                (_MigrationType.FILE.value, _MigrationType.TABLE_ATTACHED_FILE.value)
            )

            counts_by_status = {status.name: 0 for status in _MigrationStatus}
            for row in result:
                status = row[0]
                count = row[1]
                counts_by_status[_MigrationStatus(status).name] = count

            return counts_by_status

    def get_migrations(self):
        import sqlite3
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            last_id = None
            column_names = None

            rowid = -1
            while True:
                results = cursor.execute(
                    """
                        select
                            rowid,

                            id,
                            type,
                            version,
                            row_id,
                            col_id,
                            from_storage_location_id,
                            from_file_handle_id,
                            to_file_handle_id,
                            status,
                            exception
                        from migrations
                        where
                            rowid > ?
                            and type in (?, ?)
                        order by
                            rowid
                        limit ?
                    """,
                    (
                        rowid,
                        _MigrationType.FILE.value, _MigrationType.TABLE_ATTACHED_FILE.value,
                        _get_batch_size()
                    )
                )

                row_count = 0
                for row in results:
                    row_count += 1

                    # using the internal sqlite rowid for ordering only
                    rowid = row[0]

                    # exclude the sqlite internal rowid
                    row_dict = _get_row_dict(cursor, row, False)
                    entity_id = row_dict['id']
                    if entity_id != last_id:
                        # if the next row is dealing with a different entity than the last table
                        # id then we discard any cached column names we looked up
                        column_names = {}

                    row_dict['type'] = 'file' if row_dict['type'] == _MigrationType.FILE.value else 'table'

                    for int_arg in (
                            'version',
                            'row_id',
                            'from_storage_location_id',
                            'from_file_handle_id',
                            'to_file_handle_id'
                    ):
                        int_val = row_dict.get(int_arg)
                        if int_val is not None:
                            row_dict[int_arg] = int(int_val)

                    col_id = row_dict.pop('col_id', None)
                    if col_id is not None:
                        column_name = column_names.get(col_id)

                        # for usability we look up the actual column name from the id,
                        # but that involves a lookup so we cache them for re-use across
                        # rows that deal with the same table entity
                        if column_name is None:
                            column = self._syn.restGET("/column/{}".format(col_id))
                            column_name = column_names[col_id] = column['name']

                        row_dict['col_name'] = column_name

                    row_dict['status'] = _MigrationStatus(row_dict['status']).name

                    yield row_dict

                    last_id = entity_id

                if row_count == 0:
                    # out of rows
                    break

    def as_csv(self, path):
        with open(path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            # headers
            csv_writer.writerow([
                'id',
                'type',
                'version',
                'row_id',
                'col_name',
                'from_storage_location_id',
                'from_file_handle_id',
                'to_file_handle_id',
                'status',
                'exception'
            ])

            for row_dict in self.get_migrations():
                row_data = [
                    row_dict['id'],
                    row_dict['type'],
                    row_dict.get('version'),
                    row_dict.get('row_id'),
                    row_dict.get('col_name'),
                    row_dict.get('from_storage_location_id'),
                    row_dict.get('from_file_handle_id'),
                    row_dict.get('to_file_handle_id'),
                    row_dict['status'],
                    row_dict.get('exception')
                ]

                csv_writer.writerow(row_data)


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
    # return 500
    return 500


def _ensure_schema(cursor):
    # ensure we have the sqlite schema we need to be able to record and sort our
    # entity file handle migration.

    # one-row table records the parameters used to create the index
    cursor.execute(
        """
            create table if not exists migration_settings (
                root_id text not null,
                storage_location_id text not null,
                file_version_strategy text not null,
                include_table_files integer not null
            )
        """
    )

    # our representation of migratable file handles is flat including both file entities
    # and table attached files, so not all columns are applicable to both. row id and col id
    # are only used by table attached files, for example.
    cursor.execute(
        """
            create table if not exists migrations (
                id text not null,
                type integer not null,
                version integer null,
                row_id integer null,
                col_id integer null,

                parent_id null,
                status integer not null,
                exception text null,

                from_storage_location_id null,
                from_file_handle_id text null,
                to_file_handle_id text null,

                primary key (id, type, row_id, col_id, version)
            )
        """
    )

    # we get counts grouping on status
    cursor.execute("create index if not exists ix_status on migrations(status)")


def _wait_futures(conn, cursor, futures, return_when, continue_on_error):
    completed, futures = concurrent.futures.wait(futures, return_when=return_when)

    for completed_future in completed:

        to_file_handle_id = None
        ex = None
        try:
            key, to_file_handle_id = completed_future.result()
            status = _MigrationStatus.MIGRATED.value

        except _MigrationError as migration_ex:
            # for the purposes of recording and re-raise we're not interested in
            # the _MigrationError, just the underlying cause

            ex = migration_ex.__cause__
            key = migration_ex.key
            status = _MigrationStatus.ERRORED.value

        tb_str = ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__)) if ex else None
        update_statement = """
            update migrations set
                status = ?,
                to_file_handle_id = ?,
                exception = ?
            where
                id = ?
                and type = ?
        """

        update_args = [status, to_file_handle_id, tb_str, key.id, key.type]
        for arg in ('version', 'row_id', 'col_id'):
            arg_value = getattr(key, arg)
            if arg_value is not None:
                update_statement += "and {} = ?\n".format(arg)
                update_args.append(arg_value)
            else:
                update_statement += "and {} is null\n".format(arg)

        cursor.execute(update_statement, tuple(update_args))
        conn.commit()

        if not continue_on_error and ex:
            raise ex from None

    return futures


def index_files_for_migration(
    syn: synapseclient.Synapse,
    entity,
    storage_location_id: str,
    db_path: str,
    file_version_strategy='new',
    include_table_files=False,
    continue_on_error=False,
):
    root_id = utils.id_of(entity)

    file_version_strategies = {'new', 'all', 'latest', 'skip'}
    if file_version_strategy not in file_version_strategies:
        raise ValueError(
            "Invalid file_version_strategy: {}, must be one of {}".format(
                file_version_strategy,
                file_version_strategies
            )
        )

    if file_version_strategy == 'skip' and not include_table_files:
        raise ValueError('Skipping both files entities and table attached files, nothing to migrate')

    _verify_storage_location_ownership(syn, storage_location_id)

    test_import_sqlite3()
    import sqlite3
    with sqlite3.connect(db_path) as conn:

        cursor = conn.cursor()
        _ensure_schema(cursor)

        _verify_index_settings(
            cursor,
            db_path,
            root_id,
            storage_location_id,
            file_version_strategy,
            include_table_files
        )
        conn.commit()

        entity = syn.get(root_id, downloadFile=False)
        try:
            _index_entity(
                conn,
                cursor,
                syn,
                entity,
                None,
                storage_location_id,
                file_version_strategy,
                include_table_files,
                continue_on_error,
            )

        except _IndexingError as indexing_ex:
            logger.exception(
                "Aborted due to failure to index entity %s of type %s. Use the continue_on_error option to skip "
                "over entities due to individual failures.",
                indexing_ex.entity_id,
                indexing_ex.concrete_type,
            )

            raise indexing_ex.__cause__

    return MigrationResult(syn, db_path)


def _confirm_migration(cursor, force, storage_location_id):
    # we proceed with migration if either using the force option or if
    # we can prompt the user with the count if items that are going to
    # be migrated and receive their confirmation from shell input
    confirmed = force
    if not force:
        count = cursor.execute(
            "select count(*) from migrations where status = ?",
            (_MigrationStatus.INDEXED.value,)
        ).fetchone()[0]

        if count == 0:
            logger.info("No items for migration.")

        elif sys.stdout.isatty():
            uinput = input("{} items for migration to {}. Proceed? (y/n)? ".format(
                count,
                storage_location_id
            ))
            confirmed = uinput.strip().lower() == 'y'

        else:
            logger.info(
                "%s items for migration. "
                "force option not used, and console input not available to confirm migration, aborting. "
                "Use the force option or run from an interactive shell to proceed with migration.",
                count
            )

    return confirmed


def migrate_indexed_files(
    syn: synapseclient.Synapse,
    db_path: str,
    create_table_snapshots=True,
    continue_on_error=False,
    force=False
):
    executor, max_concurrent_file_copies = _get_executor()

    test_import_sqlite3()
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        _ensure_schema(cursor)
        settings = _retrieve_index_settings(cursor)
        if settings is None:
            # no settings were available at the index given
            raise ValueError(
                "Unable to retrieve existing index settings from '{}'. "
                "Either this path does represent a previously created migration index file or the file is corrupt."
            )

        storage_location_id = settings['storage_location_id']
        if not _confirm_migration(cursor, force, storage_location_id):
            logger.info("Migration aborted.")
            return

        key = _MigrationKey(id='', type=None, row_id=-1, col_id=-1, version=-1)
        futures = set()

        # we've completed the index, only proceed with the changes if not in a dry run

        while True:
            if len(futures) >= max_concurrent_file_copies:
                futures = _wait_futures(
                    conn,
                    cursor,
                    futures,
                    concurrent.futures.FIRST_COMPLETED,
                    continue_on_error,
                )

            # we query for additional file or table associated file handles to migrate in batches
            # ordering by synapse id. there can be multiple file handles associated with a particular
            # synapse id (i.e. multiple file entity versions or multiple table attached files per table),
            # so the ordering and where clause need to account for that.
            version = key.version if key.version is not None else -1
            row_id = key.row_id if key.row_id is not None else -1
            col_id = key.col_id if key.col_id is not None else -1
            results = cursor.execute(
                """
                    select
                        id,
                        type,
                        version,
                        row_id,
                        col_id,
                        from_file_handle_id
                    from migrations
                    where
                        status = ?
                        and ((id > ? and type in (?, ?))
                            or (id = ? and type = ? and version is not null and version > ?)
                            or (id = ? and type = ? and (row_id > ? or (row_id = ? and col_id > ?))))
                    order by
                        id,
                        type,
                        row_id,
                        col_id,
                        version
                    limit ?
                """,
                (
                    _MigrationStatus.INDEXED.value,
                    key.id, _MigrationType.FILE.value, _MigrationType.TABLE_ATTACHED_FILE.value,
                    key.id, _MigrationType.FILE.value, version,
                    key.id, _MigrationType.TABLE_ATTACHED_FILE.value, row_id, row_id, col_id,
                    _get_batch_size()
                )
            )

            row_count = 0
            for row in results:
                row_count += 1

                row_dict = _get_row_dict(cursor, row, True)
                key_dict = {
                    k: v for k, v in row_dict.items()
                    if k in ('id', 'type', 'version', 'row_id', 'col_id')
                }

                last_key = key
                key = _MigrationKey(**key_dict)
                from_file_handle_id = row_dict['from_file_handle_id']

                if key.type == _MigrationType.FILE.value:
                    if key.version is None:
                        migration_fn = _create_new_file_version

                    else:
                        migration_fn = _migrate_file_version

                elif key.type == _MigrationType.TABLE_ATTACHED_FILE.value:
                    if last_key.id != key.id and create_table_snapshots:
                        syn.create_snapshot_version(key.id)

                    migration_fn = _migrate_table_attached_file

                else:
                    raise ValueError("Unexpected type {} with id {}".format(key.type, key.id))

                def migration_task(syn, key, from_file_handle_id, storage_location_id):
                    with shared_executor(executor):
                        try:
                            # instrument the shared executor in this thread so that we won't
                            # create a new executor to perform the multipart copy
                            to_file_handle_id = migration_fn(syn, key, from_file_handle_id, storage_location_id)
                            return key, to_file_handle_id
                        except Exception as ex:
                            raise _MigrationError(key) from ex

                future = executor.submit(migration_task, syn, key, from_file_handle_id, storage_location_id)
                futures.add(future)

            if row_count == 0:
                # we've run out of migratable sqlite rows, we're done
                break

        if futures:
            _wait_futures(
                conn,
                cursor,
                futures,
                concurrent.futures.ALL_COMPLETED,
                continue_on_error
            )

    return MigrationResult(syn, db_path)


def _verify_storage_location_ownership(syn, storage_location_id):
    # if this doesn't raise an error we're okay
    try:
        syn.restGET("/storageLocation/{}".format(storage_location_id))
    except synapseclient.core.exceptions.SynapseHTTPError:
        raise ValueError(
            "Error verifying storage location ownership of {}. You must be creator of the destination storage location"
            .format(storage_location_id)
        )


def _retrieve_index_settings(cursor):
    results = cursor.execute(
        """
            select
                root_id,
                storage_location_id,
                file_version_strategy,
                include_table_files
            from migration_settings
        """
    )

    row = results.fetchone()
    if row:
        settings = {
            col[0]: row[i] for i, col in enumerate(cursor.description)
            if row[i] is not None and col[0] != 'rowid'
        }
    else:
        settings = None

    return settings


def _verify_index_settings(
        cursor,
        db_path,
        root_id,
        storage_location_id,
        file_version_strategy,
        include_table_files,
):
    existing_settings = _retrieve_index_settings(cursor)

    if existing_settings is not None:
        settings = locals()
        for setting in ('root_id', 'storage_location_id', 'file_version_strategy', 'include_table_files'):
            parameter = settings[setting]
            existing_value = existing_settings[setting]

            if not parameter == existing_value:
                # value does not match the existing index settings.
                # we can't resume indexing with an existing index file using a different setting.
                raise ValueError(
                    "Index parameter for '{}' value of '{}' does not match the setting ({}) recorded in the existing "
                    "index file at path '{}'. To change the index settings start over by deleting the existing "
                    "index file or using a different db_path.".format(
                        setting,
                        parameter,
                        existing_value,
                        db_path,
                    )
                )

    else:
        # this is a new index file, no previous values to compare against,
        # instead record the current settings
        cursor.execute(
            """
            insert into migration_settings (
                root_id,
                storage_location_id,
                file_version_strategy,
                include_table_files
            ) values (?, ?, ?, ?)
            """,
            (root_id, storage_location_id, file_version_strategy, 1 if include_table_files else 0)
        )


def _check_indexed(cursor, entity_id):
    # check if we have indexed the given entity in the sqlite db yet.
    # if so it can skip reindexing it. supports resumption.
    indexed_row = cursor.execute(
        "select 1 from migrations where id = ?",
        (entity_id,)
    ).fetchone()

    if indexed_row:
        logger.debug('%s already indexed, skipping', entity_id)
        return True

    logger.debug('%s not yet indexed, indexing now', entity_id)
    return False


def _get_version_numbers(syn, entity_id):
    for version_info in syn._GET_paginated("/entity/{id}/version".format(id=entity_id)):
        yield version_info['versionNumber']


def _index_file_entity(cursor, syn, entity_id, parent_id, to_storage_location_id, file_version_strategy):
    logging.info('Indexing file entity %s', entity_id)

    # 2-tuples of entity, version # to record
    entity_versions = []

    if file_version_strategy == 'new':
        # we'll need the etag to be able to do an update on an entity version
        # so we need to fetch the full entity now
        entity = syn.get(entity_id, downloadFile=False)
        entity_versions.append((
            entity,
            None  # no version number, this record indicates we will create a new version
        ))

    elif file_version_strategy == 'all':
        # one row for each existing version that will all be migrated
        for version in _get_version_numbers(syn, entity_id):
            entity = syn.get(entity_id, version=version, downloadFile=False)
            entity_versions.append((
                entity,
                version)
            )

    elif file_version_strategy == 'latest':
        # one row for the most recent version that will be migrated
        entity = syn.get(entity_id, downloadFile=False)
        entity_versions.append((
            entity,
            entity.versionNumber
        ))

    if entity_versions:
        insert_values = []
        for (entity, version) in entity_versions:
            from_storage_location_id = entity._file_handle['storageLocationId']
            migration_status = _MigrationStatus.INDEXED.value \
                if str(from_storage_location_id) != str(to_storage_location_id) \
                else _MigrationStatus.ALREADY_MIGRATED.value

            insert_values.append((
                entity_id,
                _MigrationType.FILE.value,
                version,
                parent_id,
                from_storage_location_id,
                entity.dataFileHandleId,
                migration_status
            ))

        cursor.executemany(
            """
                insert into migrations (
                    id,
                    type,
                    version,
                    parent_id,
                    from_storage_location_id,
                    from_file_handle_id,
                    status
                ) values (?, ?, ?, ?, ?, ?, ?)
            """,
            insert_values
        )


def _get_file_handle_rows(syn, table_id):
    file_handle_columns = [c for c in syn.restGET("/entity/{id}/column".format(id=table_id))['results']
                           if c['columnType'] == 'FILEHANDLEID']
    file_column_select = ','.join(c['name'] for c in file_handle_columns)
    results = syn.tableQuery("select {} from {}".format(file_column_select, table_id))
    for row in results:
        file_handles = {}

        # first two cols are row id and row version, rest are file handle ids from our query
        row_id, row_version = row[:2]

        file_handle_ids = row[2:]
        for i, file_handle_id in enumerate(file_handle_ids):
            col_id = file_handle_columns[i]['id']
            file_handle = syn._getFileHandleDownload(file_handle_id, table_id, objectType='TableEntity')['fileHandle']
            file_handles[col_id] = file_handle

        yield row_id, row_version, file_handles


def _index_table_entity(cursor, syn, entity, parent_id, storage_location_id):
    entity_id = utils.id_of(entity)
    logging.info('Indexing table entity %s', entity_id)

    row_batch = []

    def _insert_row_batch(row_batch):
        cursor.executemany(
            """insert into migrations
                (
                    id,
                    type,
                    parent_id,
                    row_id,
                    col_id,
                    version,
                    from_storage_location_id,
                    from_file_handle_id,
                    status
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row_batch
        )

    for row_id, row_version, file_handles in _get_file_handle_rows(syn, entity_id):
        for col_id, file_handle in file_handles.items():
            existing_storage_location_id = file_handle['storageLocationId']

            migration_status = _MigrationStatus.INDEXED.value \
                if str(storage_location_id) != str(existing_storage_location_id) \
                else _MigrationStatus.ALREADY_MIGRATED.value

            row_batch.append((
                entity_id,
                _MigrationType.TABLE_ATTACHED_FILE.value,
                parent_id,
                row_id,
                col_id,
                row_version,
                existing_storage_location_id,
                file_handle['id'],
                migration_status
            ))

            if len(row_batch) % _get_batch_size() == 0:
                _insert_row_batch(row_batch)
                row_batch = []

    if row_batch:
        _insert_row_batch(row_batch)


def _index_container(
        conn,
        cursor,
        syn,
        container_entity,
        parent_id,
        storage_location_id,
        file_version_strategy,
        include_table_files,
        continue_on_error
):
    entity_id = utils.id_of(container_entity)
    concrete_type = utils.concrete_type_of(container_entity)
    logger.info('Indexing %s %s', concrete_type[concrete_type.rindex('.') + 1:], entity_id)

    include_types = ['folder']
    if file_version_strategy != 'skip':
        include_types.append('file')
    if include_table_files:
        include_types.append('table')

    children = syn.getChildren(entity_id, includeTypes=include_types)
    for child in children:
        _index_entity(
            conn,
            cursor,
            syn,
            child,
            entity_id,
            storage_location_id,
            file_version_strategy,
            include_table_files,
            continue_on_error,
        )

    # once all the children are recursively indexed we mark this parent itself as indexed
    container_type = (
        _MigrationType.PROJECT.value
        if concrete_types.PROJECT_ENTITY == concrete_type
        else _MigrationType.FOLDER.value
    )
    cursor.execute(
        "insert into migrations (id, type, parent_id, status) values (?, ?, ?, ?)",
        [entity_id, container_type, parent_id, _MigrationStatus.INDEXED.value]
    )


def _index_entity(
        conn,
        cursor,
        syn,
        entity,
        parent_id,
        storage_location_id,
        file_version_strategy,
        include_table_files,
        continue_on_error
):
    # recursive function to index a given entity into the sqlite db.

    entity_id = utils.id_of(entity)
    concrete_type = utils.concrete_type_of(entity)

    try:
        if not _check_indexed(cursor, entity_id):
            # if already indexed we short circuit (previous indexing will be used)

            if concrete_type == concrete_types.FILE_ENTITY:
                _index_file_entity(
                    cursor,
                    syn,
                    entity_id,
                    parent_id,
                    storage_location_id,
                    file_version_strategy
                )

            elif concrete_type == concrete_types.TABLE_ENTITY:
                _index_table_entity(
                    cursor,
                    syn,
                    entity,
                    parent_id,
                    storage_location_id
                )

            elif concrete_type in [concrete_types.FOLDER_ENTITY, concrete_types.PROJECT_ENTITY]:
                _index_container(
                    conn,
                    cursor,
                    syn,
                    entity,
                    parent_id,
                    storage_location_id,
                    file_version_strategy,
                    include_table_files,
                    continue_on_error,
                )

        conn.commit()

    except _IndexingError:
        # this is a recursive function, we don't need to log the error at every level so just
        # pass up exceptions of this type that wrap the underlying exception and indicate
        # that they were already logged
        raise

    except Exception as ex:

        if continue_on_error:
            logger.warning("Error indexing entity %s of type %s", entity_id, concrete_type, exc_info=True)
            tb_str = ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__))

            cursor.execute(
                """
                    insert into migrations (
                        id,
                        type,
                        parent_id,
                        status,
                        exception
                    ) values (?, ?, ?, ?, ?)
                """,
                (
                    entity_id,
                    _MigrationType.from_concrete_type(concrete_type).value,
                    parent_id,
                    _MigrationStatus.ERRORED.value,
                    tb_str,
                )
            )

        else:
            raise _IndexingError(entity_id, concrete_type) from ex


def _create_new_file_version(syn, key, from_file_handle_id, storage_location_id):
    logging.info('Creating new version for file entity %s', key.id)

    entity = syn.get(key.id, downloadFile=False)

    source_file_handle_association = {
        'fileHandleId': from_file_handle_id,
        'associateObjectId': key.id,
        'associateObjectType': 'FileEntity',
    }

    new_file_handle_id = multipart_copy(
        syn,
        source_file_handle_association,
        storage_location_id=storage_location_id,
    )

    entity.dataFileHandleId = new_file_handle_id
    syn.store(entity)

    return new_file_handle_id


def _migrate_file_version(syn, key, from_file_handle_id, storage_location_id):
    logging.info('Migrating file entity %s version %s', key.id, key.version)

    source_file_handle_association = {
        'fileHandleId': from_file_handle_id,
        'associateObjectId': key.id,
        'associateObjectType': 'FileEntity',
    }

    new_file_handle_id = multipart_copy(
        syn,
        source_file_handle_association,
        storage_location_id=storage_location_id,
    )

    file_handle_update_request = {
        'oldFileHandleId': from_file_handle_id,
        'newFileHandleId': new_file_handle_id,
    }

    # no response, we rely on a 200 here
    syn.restPUT(
        "/entity/{id}/version/{versionNumber}/filehandle".format(
            id=key.id,
            versionNumber=key.version,
        ),
        json.dumps(file_handle_update_request),
    )

    return new_file_handle_id


def _migrate_table_attached_file(syn, key, from_file_handle_id, storage_location_id):
    logging.info('Migrating table attached file %s, row %s, col %s', key.id, key.row_id, key.col_id)

    source_file_handle_association = {
        'fileHandleId': from_file_handle_id,
        'associateObjectId': key.id,
        'associateObjectType': 'TableEntity',
    }

    to_file_handle_id = multipart_copy(
        syn,
        source_file_handle_association,
        storage_location_id=storage_location_id
    )

    row_mapping = {str(key.col_id): to_file_handle_id}
    partial_rows = [synapseclient.table.PartialRow(row_mapping, key.row_id)]
    partial_rowset = synapseclient.PartialRowset(key.id, partial_rows)
    syn.store(partial_rowset)

    return to_file_handle_id


class _MigrationError(Exception):
    def __init__(self, key):
        self.key = key


class _IndexingError(Exception):
    def __init__(self, entity_id, concrete_type):
        self.entity_id = entity_id
        self.concrete_type = concrete_type
