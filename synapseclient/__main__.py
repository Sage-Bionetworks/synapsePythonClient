"""
***************************
Synapse command line client
***************************

The Synapse Python Client can be used from the command line via the **synapse** command.

Installation
============

The command line client is installed along with `installation of the Synapse Python client \
<http://python-docs.synapse.org/build/html/index.html#installation>`_.

Help
====

For help, type::

    synapse -h.

For help on specific commands, type::

    synapse [command] -h


Optional arguments
==================

.. code-block:: shell

    -h, --help            show this help message and exit
    --version             show program's version number and exit
    -u SYNAPSEUSER, --username SYNAPSEUSER
                        Username used to connect to Synapse
    -p SYNAPSEPASSWORD, --password SYNAPSEPASSWORD
                        Password used to connect to Synapse

Commands
========
  * **get**              - download an entity and associated data
  * **sync**             - Synchronize files described in a manifest to Synapse
  * **store**            - uploads and adds a file to Synapse
  * **store-table**      - uploads a table to Syanpse given a csv
  * **add**              - add or modify content to Synapse
  * **mv**               - move a dataset in Synapse
  * **cp**               - copy an entity/dataset in Synapse
  * **associate**        - Associate local files with the files stored in Synapse so
                           that calls to 'syntapse get' and 'synapse show' don't
                           re-download the files, but use the already existing file.
  * **delete**           - removes a dataset from Synapse
  * **query**            - performs SQL like queries on Synapse
  * **submit**           - submit an entity for evaluation
  * **show**             - displays information about a Entity
  * **cat**              - prints a dataset from Synapse
  * **list**             - List Synapse entities contained by the given Project or
                           Folder. Note: May not be supported in future versions of
                           the client.
  * **set-provenance**   - create provenance records
  * **get-provenance**   - show provenance records
  * **set-annotations**  - create annotations
  * **get-annotations**  - show annotations
  * **create**           - Creates folders or projects on Synapse
  * **onweb**            - opens Synapse website for Entity
  * **login**            - login to Synapse and (optionally) cache credentials
  * **test-encoding**    - test character encoding to help diagnose problems
"""
import argparse
import os
import sys
import signal
import json
import getpass
import csv
import re

import synapseclient
import synapseutils
from . import Activity
from .wiki import Wiki
from synapseclient.core.exceptions import *


def query(args, syn):
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, ValueError):
        # Different OS's have different signals defined. In particular,
        # SIGPIPE doesn't exist on Windows. The docs have this to say,
        # "On Windows, signal() can only be called with SIGABRT, SIGFPE,
        # SIGILL, SIGINT, SIGSEGV, or SIGTERM. A ValueError will be raised
        # in any other case."
        pass
    # TODO: Should use loop over multiple returned values if return is too long

    queryString = ' '.join(args.queryString)

    if re.search('from syn\d', queryString.lower()):
        results = syn.tableQuery(queryString)
        reader = csv.reader(open(results.filepath))
        for row in reader:
            sys.stdout.write("%s\n" % ("\t".join(row)))
    else:
        sys.stderr.write('Input query cannot be parsed. Please see our documentation for writing Synapse query:'
                         ' https://docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html')


def _getIdsFromQuery(queryString, syn):
    """Helper function that extracts the ids out of returned query."""

    if re.search('from syn\d', queryString.lower()):
        tbl = syn.tableQuery(queryString)

        check_for_id_col = filter(lambda x: x.get('id'), tbl.headers)
        assert check_for_id_col, ValueError("Query does not include the id column.")

        ids = [x['id'] for x in csv.DictReader(open(tbl.filepath))]
        return ids
    else:
        raise ValueError('Input query cannot be parsed. Please see our documentation for writing Synapse query:'
                         ' https://docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html')



def get(args, syn):
    syn.multi_threaded = args.multiThreaded
    if args.recursive:
        if args.version is not None:
            raise ValueError('You cannot specify a version making a recursive download.')
        synapseutils.syncFromSynapse(syn, args.id, args.downloadLocation, followLink=args.followLink)
    elif args.queryString is not None:
        if args.version is not None or args.id is not None:
            raise ValueError('You cannot specify a version or id when you are downloading a query.')
        ids = _getIdsFromQuery(args.queryString, syn)
        for id in ids:
            syn.get(id, downloadLocation=args.downloadLocation)
    else:
        # search by MD5
        if isinstance(args.id, str) and os.path.isfile(args.id):
            entity = syn.get(args.id, version=args.version, limitSearch=args.limitSearch, downloadFile=False)
            if "path" in entity and entity.path is not None and os.path.exists(entity.path):
                print("Associated file: %s with synapse ID %s" % (entity.path, entity.id))
        # normal syn.get operation
        else:
            entity = syn.get(args.id, version=args.version,  # limitSearch=args.limitSearch,
                             followLink=args.followLink,
                             downloadLocation=args.downloadLocation)
            if "path" in entity and entity.path is not None and os.path.exists(entity.path):
                print("Downloaded file: %s" % os.path.basename(entity.path))
            else:
                print('WARNING: No files associated with entity %s\n' % entity.id)
                print(entity)

        print('Creating %s' % entity.path)


def sync(args, syn):
    synapseutils.syncToSynapse(syn, manifestFile=args.manifestFile,
                               dryRun=args.dryRun, sendMessages=args.sendMessages,
                               retries=args.retries)


def store(args, syn):
    # If we are storing a fileEntity we need to have id or parentId
    if args.parentid is None and args.id is None and args.file is not None:
        raise ValueError('synapse store requires at least either parentId or id to be specified.')
    # If both args.FILE and args.file specified raise error
    if args.file and args.FILE:
        raise ValueError('only specify one file')

    _descriptionFile_arg_check(args)

    args.file = args.FILE if args.FILE is not None else args.file
    args.type = 'FileEntity' if args.type == 'File' else args.type

    # Since force_version defaults to True, negate to determine what
    # forceVersion action should be
    force_version = not args.noForceVersion

    if args.id is not None:
        entity = syn.get(args.id, downloadFile=False)
    else:
        entity = {'concreteType': 'org.sagebionetworks.repo.model.%s' % args.type,
                  'name': utils.guess_file_name(args.file) if args.file and not args.name else None,
                  'parentId': None}
    # Overide setting for parameters included in args
    entity['name'] = args.name if args.name is not None else entity['name']
    entity['parentId'] = args.parentid if args.parentid is not None else entity['parentId']
    entity['path'] = args.file if args.file is not None else None
    entity['synapseStore'] = not utils.is_url(args.file)

    used = syn._convertProvenanceList(args.used, args.limitSearch)
    executed = syn._convertProvenanceList(args.executed, args.limitSearch)
    entity = syn.store(entity, used=used, executed=executed,
                       forceVersion=force_version)

    _create_wiki_description_if_necessary(args, entity, syn)

    print('Created/Updated entity: %s\t%s' % (entity['id'], entity['name']))

    # After creating/updating, if there are annotations to add then
    # add them
    if args.annotations is not None:
        # Need to override the args id parameter
        setattr(args, 'id', entity['id'])
        setAnnotations(args, syn)


def _create_wiki_description_if_necessary(args, entity, syn):
    """
    store the description in a Wiki
    """
    if args.description or args.descriptionFile:
        syn.store(Wiki(markdown=args.description, markdownFile=args.descriptionFile, owner=entity))


def _descriptionFile_arg_check(args):
    """
    checks that descriptionFile(if specified) is a valid file path
    """
    if args.descriptionFile:
        if not os.path.isfile(args.descriptionFile):
            raise ValueError('The specified descriptionFile path is not a file or does not exist')


def move(args, syn):
    """Moves an entity specified by args.id to args.parentId"""
    entity = syn.move(args.id, args.parentid)
    print('Moved %s to %s' % (entity.id, entity.parentId))


def associate(args, syn):
    files = []
    if args.r:
        files = [os.path.join(dp, f) for dp, dn, filenames in
                 os.walk(args.path) for f in filenames]
    if os.path.isfile(args.path):
        files = [args.path]
    if len(files) == 0:
        raise Exception(("The path specified is inaccurate. "
                         "If it is a directory try using 'associate -r'"))
    for fp in files:
        try:
            ent = syn.get(fp, limitSearch=args.limitSearch)
        except SynapseFileNotFoundError:
            print('WARNING: The file %s is not available in Synapse' % fp)
        else:
            print('%s.%i\t%s' % (ent.id, ent.versionNumber, fp))


def copy(args, syn):
    mappings = synapseutils.copy(syn, args.id, args.destinationId,
                                 skipCopyWikiPage=args.skipCopyWiki,
                                 skipCopyAnnotations=args.skipCopyAnnotations,
                                 excludeTypes=args.excludeTypes,
                                 version=args.version, updateExisting=args.updateExisting,
                                 setProvenance=args.setProvenance)
    print(mappings)


def cat(args, syn):
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, ValueError):
        # Different OS's have different signals defined. In particular,
        # SIGPIPE doesn't exist one Windows. The docs have this to say,
        # "On Windows, signal() can only be called with SIGABRT, SIGFPE,
        # SIGILL, SIGINT, SIGSEGV, or SIGTERM. A ValueError will be raised
        # in any other case."
        pass
    entity = syn.get(args.id, version=args.version)
    if 'path' in entity:
        with open(entity.path) as inputfile:
            for line in inputfile:
                sys.stdout.write(line)


def ls(args, syn):
    """List entities in a Project or Folder"""
    syn._list(args.id, recursive=args.recursive, long_format=args.long, show_modified=args.modified)


def show(args, syn):
    """Show metadata for an entity."""

    ent = syn.get(args.id, downloadFile=False)
    syn.printEntity(ent)
    sys.stdout.write('Provenance:\n')
    try:
        prov = syn.getProvenance(ent)
        print(prov)
    except SynapseHTTPError:
        print('  No Activity specified.\n')


def delete(args, syn):
    if args.version:
        syn.delete(args.id, args.version)
        print('Deleted entity %s, version %s' % (args.id, args.version))
    else:
        syn.delete(args.id)
        print('Deleted entity: %s' % args.id)


def create(args, syn):
    _descriptionFile_arg_check(args)

    entity = {'name': args.name,
              'concreteType': 'org.sagebionetworks.repo.model.%s' % args.type}

    if args.parentid is not None:
        entity['parentId'] = args.parentid

    entity = syn.store(entity)

    _create_wiki_description_if_necessary(args, entity, syn)

    print('Created entity: %s\t%s\n' % (entity['id'], entity['name']))


def onweb(args, syn):
    syn.onweb(args.id)


def setProvenance(args, syn):
    """Set provenance information on a synapse entity."""

    activity = Activity(name=args.name, description=args.description)

    if args.used:
        for item in syn._convertProvenanceList(args.used, args.limitSearch):
            activity.used(item)
    if args.executed:
        for item in syn._convertProvenanceList(args.executed, args.limitSearch):
            activity.used(item, wasExecuted=True)
    activity = syn.setProvenance(args.id, activity)

    # Display the activity record, if -o or -output specified
    if args.output:
        if args.output == 'STDOUT':
            sys.stdout.write(json.dumps(activity))
            sys.stdout.write('\n')
        else:
            with open(args.output, 'w') as f:
                f.write(json.dumps(activity))
                f.write('\n')
    else:
        print('Set provenance record %s on entity %s\n' % (str(activity['id']), str(args.id)))


def getProvenance(args, syn):
    activity = syn.getProvenance(args.id, args.version)

    if args.output is None or args.output == 'STDOUT':
        print(json.dumps(activity, sort_keys=True, indent=2))
    else:
        with open(args.output, 'w') as f:
            f.write(json.dumps(activity))
            f.write('\n')


def setAnnotations(args, syn):
    """Method to set annotations on an entity.

    Requires a JSON-formatted string that evaluates to a dict.

    Annotations can be updated or overwritten completely.

    """

    try:
        newannots = json.loads(args.annotations)
    except Exception as e:
        sys.stderr.write(
            "Please check that your JSON string is properly formed and evaluates to a dictionary (key/value pairs). "
            "For example, to set an annotations called 'foo' to the value 1, the format should be "
            "'{\"foo\": 1, \"bar\":\"quux\"}'.")
        raise e

    if type(newannots) is not dict:
        raise TypeError(
            "Please check that your JSON string is properly formed and evaluates to a dictionary (key/value pairs). "
            "For example, to set an annotations called 'foo' to the value 1, the format should be "
            "'{\"foo\": 1, \"bar\":\"quux\"}'.")

    entity = syn.get(args.id, downloadFile=False)

    if args.replace:
        annots = newannots
    else:
        annots = syn.getAnnotations(entity)
        annots.update(newannots)

    syn.setAnnotations(entity, annots)

    sys.stderr.write('Set annotations on entity %s\n' % (args.id,))


def getAnnotations(args, syn):
    annotations = syn.getAnnotations(args.id)

    if args.output is None or args.output == 'STDOUT':
        print(json.dumps(annotations, sort_keys=True, indent=2))
    else:
        with open(args.output, 'w') as f:
            f.write(json.dumps(annotations))
            f.write('\n')


def storeTable(args, syn):
    """Store table given csv"""
    table = synapseclient.table.build_table(args.name,
                                            args.parentid,
                                            args.csv)
    table_ent = syn.store(table)
    print('{"tableId": "%s"}' % table_ent.tableId)


def submit(args, syn):
    """
    Method to allow challenge participants to submit to an evaluation queue.

    Examples::
    synapse submit --evaluation 'ra_challenge_Q1_leaderboard' -f ~/testing/testing.txt --parentId syn2345030 \
    --used syn2351967 --executed syn2351968
    synapse submit --evaluation 2343117 -f ~/testing/testing.txt --parentId syn2345030 --used syn2351967 \
    --executed syn2351968
    """
    # check if evaluation is a number, if so it is assumed to be a evaluationId else it is a evaluationName
    if args.evaluation is not None:
        try:
            args.evaluationID = str(int(args.evaluation))
        except ValueError:
            args.evaluationName = args.evaluation

    # checking if user has entered a evaluation ID or evaluation Name
    if args.evaluationID is None and args.evaluationName is None:
        raise ValueError('Evaluation ID or Evaluation Name is required\n')
    elif args.evaluationID is not None and args.evaluationName is not None:
        sys.stderr.write('[Warning]: Both Evaluation ID & Evaluation Name are specified \n EvaluationID will be used\n')
    elif args.evaluationID is None:  # get evalID from evalName
        try:
            args.evaluationID = syn.getEvaluationByName(args.evaluationName)['id']
        except Exception:
            raise ValueError('Could not find an evaluation named: %s \n' % args.evaluationName)

    # checking if a entity id or file was specified by the user
    if args.entity is None and args.file is None:
        raise ValueError('Either entityID or filename is required for a submission\n')
    elif args.entity is not None and args.file is not None:
        sys.stderr.write('[Warning]: Both entityID and filename are specified \n entityID will be used\n')
    elif args.entity is None:  # upload the the file to synapse and get synapse entity id for the file
        if args.parentid is None:
            raise ValueError('parentID required with a file upload\n')
        if not os.path.exists(args.file):
            raise IOError('file path %s not valid \n' % args.file)
        # //ideally this should be factored out
        synFile = syn.store(synapseclient.File(path=args.file, parent=args.parentid),
                            used=syn._convertProvenanceList(args.used, args.limitSearch),
                            executed=syn._convertProvenanceList(args.executed, args.limitSearch))
        args.entity = synFile.id

    submission = syn.submit(args.evaluationID, args.entity, name=args.name, team=args.teamName)
    sys.stdout.write('Submitted (id: %s) entity: %s\t%s to Evaluation: %s\n'
                     % (submission['id'], submission['entityId'], submission['name'], submission['evaluationId']))


def login(args, syn):
    """Log in to Synapse, optionally caching credentials"""
    login_with_prompt(syn, args.synapseUser, args.synapsePassword, rememberMe=args.rememberMe, forced=True)
    profile = syn.getUserProfile()
    print("Logged in as: {userName} ({ownerId})".format(**profile))


def test_encoding(args, syn):
    import locale
    import platform
    print("python version =               ", platform.python_version())
    print("sys.stdout.encoding =          ",
          sys.stdout.encoding if hasattr(sys.stdout, 'encoding') else 'no encoding attribute')
    print("sys.stdout.isatty() =          ", sys.stdout.isatty())
    print("locale.getpreferredencoding() =", locale.getpreferredencoding())
    print("sys.getfilesystemencoding() =  ", sys.getfilesystemencoding())
    print("PYTHONIOENCODING =             ", os.environ.get("PYTHONIOENCODING", None))
    print("latin1 chars =                 D\xe9j\xe0 vu, \xfcml\xf8\xfats")
    print("Some non-ascii chars =         '\u0227\u0188\u0188\u1e17\u019e\u0167\u1e17\u1e13 u\u028dop-\u01ddp\u0131sdn "
          "\u0167\u1e17\u1e8b\u0167 \u0192\u01ff\u0159 \u0167\u1e17\u015f\u0167\u012b\u019e\u0260'", )


def build_parser():
    """Builds the argument parser and returns the result."""

    USED_HELP = ('Synapse ID, a url, or a local file path (of a file previously'
                 'uploaded to Synapse) from which the specified entity is derived')
    EXECUTED_HELP = ('Synapse ID, a url, or a local file path (of a file previously'
                     'uploaded to Synapse) that was executed to generate the specified entity')

    parser = argparse.ArgumentParser(description='Interfaces with the Synapse repository.')
    parser.add_argument('--version', action='version',
                        version='Synapse Client %s' % synapseclient.__version__)
    parser.add_argument('-u', '--username', dest='synapseUser',
                        help='Username used to connect to Synapse')
    parser.add_argument('-p', '--password', dest='synapsePassword',
                        help='Password used to connect to Synapse')
    parser.add_argument('-c', '--configPath', dest='configPath', default=synapseclient.client.CONFIG_FILE,
                        help='Path to configuration file used to connect to Synapse [default: %(default)s]')

    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.add_argument('-s', '--skip-checks', dest='skip_checks', action='store_true',
                        help='suppress checking for version upgrade messages and endpoint redirection')

    subparsers = parser.add_subparsers(title='commands',
                                       description='The following commands are available:',
                                       help='For additional help: "synapse <COMMAND> -h"')

    parser_get = subparsers.add_parser('get',
                                       help='downloads a file from Synapse')
    parser_get.add_argument('-q', '--query', metavar='queryString', dest='queryString', type=str, default=None,
                            help='Optional query parameter, will fetch all of the entities returned by a query '
                                 '(see query for help).')
    parser_get.add_argument('-v', '--version', metavar='VERSION', type=int, default=None,
                            help='Synapse version number of entity to retrieve. Defaults to most recent version.')
    parser_get.add_argument('-r', '--recursive', action='store_true', default=False,
                            help='Fetches content in Synapse recursively contained in the parentId specified by id.')
    parser_get.add_argument('--followLink', action='store_true', default=False,
                            help='Determines whether the link returns the target Entity.')
    parser_get.add_argument('--limitSearch', metavar='projId', type=str,
                            help='Synapse ID of a container such as project or folder to limit search for files '
                                 'if using a path.')
    parser_get.add_argument('--downloadLocation', metavar='path', type=str, default="./",
                            help='Directory to download file to [default: %(default)s].')
    parser_get.add_argument('--multiThreaded', action='store_true',
                            default=False, help='Download file using a multiple threaded implementation. '
                                                'This flag will be removed in the future when multi-threaded download '
                                                'is deemed fully stable and becomes the default implementation.')
    parser_get.add_argument('id', metavar='syn123', nargs='?', type=str,
                            help='Synapse ID of form syn123 of desired data object.')
    parser_get.set_defaults(func=get)

    parser_sync = subparsers.add_parser('sync',
                                        help='Synchronize files described in a manifest to Synapse')
    parser_sync.add_argument('--dryRun', action='store_true', default=False,
                             help='Perform validation without uploading.')
    parser_sync.add_argument('--sendMessages', action='store_true', default=False,
                             help='Send notifications via Synapse messaging (email) at specific intervals, '
                                  'on errors and on completion.')
    parser_sync.add_argument('--retries', metavar='INT', type=int, default=4)
    parser_sync.add_argument('manifestFile', metavar='FILE', type=str,
                             help='A tsv file with file locations and metadata to be pushed to Synapse.')
    parser_sync.set_defaults(func=sync)

    parser_store = subparsers.add_parser('store',  # Python 3.2+ would support alias=['store']
                                         help='uploads and adds a file to Synapse')
    parent_id_group = parser_store.add_mutually_exclusive_group(required=True)
    parent_id_group.add_argument('--parentid', '--parentId', '-parentid', '-parentId', metavar='syn123', type=str,
                                 required=False, dest='parentid',
                                 help='Synapse ID of project or folder where to upload data '
                                      '(must be specified if --id is not used.')
    parent_id_group.add_argument('--id', metavar='syn123', type=str, required=False,
                                 help='Optional Id of entity in Synapse to be updated.')
    parent_id_group.add_argument('--type', type=str, default='File',
                                 help='Type of object, such as "File", "Folder", or '
                                      '"Project", to create in Synapse. Defaults to "File"')

    parser_store.add_argument('--name', '-name', metavar='NAME', type=str, required=False,
                              help='Name of data object in Synapse')
    description_group_store = parser_store.add_mutually_exclusive_group()
    description_group_store.add_argument('--description', '-description', metavar='DESCRIPTION', type=str,
                                         help='Description of data object in Synapse.')
    description_group_store.add_argument('--descriptionFile', '-descriptionFile', metavar='DESCRIPTION_FILE_PATH',
                                         type=str,
                                         help='Path to a markdown file containing description of project/folder')
    parser_store.add_argument('--used', '-used', metavar='target', type=str, nargs='*',
                              help=USED_HELP)
    parser_store.add_argument('--executed', '-executed', metavar='target', type=str, nargs='*',
                              help=EXECUTED_HELP)
    parser_store.add_argument('--limitSearch', metavar='projId', type=str,
                              help='Synapse ID of a container such as project or folder to limit search for provenance '
                                   'files.')
    parser_store.add_argument('--noForceVersion', action='store_true',
                              help='Do not force a new version to be created if the contents of the file have not changed. The default is a new version is created.')  # noqa: E501
    parser_store.add_argument('--annotations', metavar='ANNOTATIONS', type=str, required=False, default=None,
                              help="Annotations to add as a JSON formatted string, should evaluate to a dictionary "
                                   "(key/value pairs). Example: '{\"foo\": 1, \"bar\":\"quux\"}'")
    parser_store.add_argument('--replace', action='store_true', default=False,
                              help='Replace all existing annotations with the given annotations')

    parser_store.add_argument('--file', type=str, help=argparse.SUPPRESS)
    parser_store.add_argument('FILE', nargs='?', type=str,
                              help='file to be added to synapse.')
    parser_store.set_defaults(func=store)

    parser_add = subparsers.add_parser('add',  # Python 3.2+ would support alias=['store']
                                       help='uploads and adds a file to Synapse')
    parent_id_group = parser_add.add_mutually_exclusive_group(required=True)
    parent_id_group.add_argument('--parentid', '--parentId', '-parentid', '-parentId', metavar='syn123', type=str,
                                 required=False, dest='parentid',
                                 help='Synapse ID of project or folder where to upload data (must be specified if --id '
                                      'is not used.')
    parent_id_group.add_argument('--id', metavar='syn123', type=str, required=False,
                                 help='Optional Id of entity in Synapse to be updated.')
    parent_id_group.add_argument('--type', type=str, default='File',
                                 help='Type of object, such as "File", "Folder", or '
                                      '"Project", to create in Synapse. Defaults to "File"')

    parser_add.add_argument('--name', '-name', metavar='NAME', type=str, required=False,
                            help='Name of data object in Synapse')
    description_group_add = parser_add.add_mutually_exclusive_group()
    description_group_add.add_argument('--description', '-description', metavar='DESCRIPTION', type=str,
                                       help='Description of data object in Synapse.')
    description_group_add.add_argument('--descriptionFile', '-descriptionFile', metavar='DESCRIPTION_FILE_PATH',
                                       type=str,
                                       help='Path to a markdown file containing description of project/folder')
    parser_add.add_argument('-type', type=str, default='File', help=argparse.SUPPRESS)
    parser_add.add_argument('--used', '-used', metavar='target', type=str, nargs='*',
                            help=USED_HELP)
    parser_add.add_argument('--executed', '-executed', metavar='target', type=str, nargs='*',
                            help=EXECUTED_HELP)
    parser_add.add_argument('--limitSearch', metavar='projId', type=str,
                            help='Synapse ID of a container such as project or folder to limit search for provenance '
                                 'files.')
    parser_add.add_argument('--noForceVersion', action='store_true',
                            help='Do not force a new version to be created if the contents of the file have not changed. The default is a new version is created.')  # noqa: E501
    parser_add.add_argument('--annotations', metavar='ANNOTATIONS', type=str, required=False, default=None,
                            help="Annotations to add as a JSON formatted string, should evaluate to a dictionary "
                                 "(key/value pairs). Example: '{\"foo\": 1, \"bar\":\"quux\"}'")
    parser_add.add_argument('--replace', action='store_true', default=False,
                            help='Replace all existing annotations with the given annotations')
    parser_add.add_argument('--file', type=str, help=argparse.SUPPRESS)
    parser_add.add_argument('FILE', nargs='?', type=str,
                            help='file to be added to synapse.')
    parser_add.set_defaults(func=store)

    parser_mv = subparsers.add_parser('mv',
                                      help='Moves a file/folder in Synapse')
    parser_mv.add_argument('--id', metavar='syn123', type=str, required=True,
                           help='Id of entity in Synapse to be moved.')
    parser_mv.add_argument('--parentid', '--parentId', '-parentid', '-parentId', metavar='syn123', type=str,
                           required=True, dest='parentid',
                           help='Synapse ID of project or folder where file/folder will be moved ')
    parser_mv.set_defaults(func=move)

    parser_cp = subparsers.add_parser('cp',
                                      help='Copies specific versions of synapse content such as files, folders and '
                                           'projects by recursively copying all sub-content')
    parser_cp.add_argument('id', metavar='syn123', type=str,
                           help='Id of entity in Synapse to be copied.')
    parser_cp.add_argument('--destinationId', metavar='syn123', required=True,
                           help='Synapse ID of project or folder where file will be copied to.')
    parser_cp.add_argument('--version', '-v', metavar='1', type=int, default=None,
                           help=('Synapse version number of File or Link to retrieve. '
                                 'This parameter cannot be used when copying Projects or Folders. '
                                 'Defaults to most recent version.'))
    parser_cp.add_argument('--setProvenance', metavar='traceback', type=str, default='traceback',
                           help=('Has three values to set the provenance of the copied entity-'
                                 'traceback: Sets to the source entity'
                                 'existing: Sets to source entity\'s original provenance (if it exists)'
                                 'None/none: No provenance is set'))
    parser_cp.add_argument('--updateExisting', action='store_true',
                           help='Will update the file if there is already a file that is named the same in the '
                                'destination')
    parser_cp.add_argument('--skipCopyAnnotations', action='store_true',
                           help='Do not copy the annotations')
    parser_cp.add_argument('--excludeTypes', nargs='*', metavar='file table', type=str, default=list(),
                           help='Accepts a list of entity types (file, table, link) which determines which entity'
                                ' types to not copy.')
    parser_cp.add_argument('--skipCopyWiki', action='store_true',
                           help='Do not copy the wiki pages')
    parser_cp.set_defaults(func=copy)

    parser_associate = subparsers.add_parser('associate',
                                             help=(
                                                 'Associate local files with the files stored in Synapse so that calls'
                                                 ' to "synapse get" and "synapse show" don\'t re-download the files '
                                                 'but use the already existing file.'))
    parser_associate.add_argument('path', metavar='path', type=str,
                                  help='local file path')
    parser_associate.add_argument('--limitSearch', metavar='projId', type=str,
                                  help='Synapse ID of a container such as project or folder to limit search to.')
    parser_associate.add_argument('-r', action='store_true',
                                  help='Perform recursive association with all local files in a folder')
    parser_associate.set_defaults(func=associate)

    parser_delete = subparsers.add_parser('delete',
                                          help='removes a dataset from Synapse')
    parser_delete.add_argument('id', metavar='syn123', type=str,
                               help='Synapse ID of form syn123 of desired data object')
    parser_delete.add_argument('--version', type=str,
                               help='Version number to delete of given entity.')
    parser_delete.set_defaults(func=delete)

    parser_query = subparsers.add_parser('query',
                                         help='Performs SQL like queries on Synapse')
    parser_query.add_argument('queryString', metavar='string', type=str, nargs='*',
                              help='A query string, see '
                                   'https://docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html'
                                   ' for more information')
    parser_query.set_defaults(func=query)

    parser_submit = subparsers.add_parser('submit',
                                          help='submit an entity or a file for evaluation')
    parser_submit.add_argument('--evaluationID', '--evaluationId', '--evalID', type=str,
                               help='Evaluation ID where the entity/file will be submitted')
    parser_submit.add_argument('--evaluationName', '--evalN', type=str,
                               help='Evaluation Name where the entity/file will be submitted')
    parser_submit.add_argument('--evaluation', type=str,
                               help=argparse.SUPPRESS)  # mainly to maintain the backward compatibility
    parser_submit.add_argument('--entity', '--eid', '--entityId', '--id', type=str,
                               help='Synapse ID of the entity to be submitted')
    parser_submit.add_argument('--file', '-f', type=str,
                               help='File to be submitted to the challenge')
    parser_submit.add_argument('--parentId', '--parentid', '--parent', type=str, dest='parentid',
                               help='Synapse ID of project or folder where to upload data')
    parser_submit.add_argument('--name', type=str,
                               help='Name of the submission')
    parser_submit.add_argument('--teamName', '--team', type=str,
                               help='Submit of behalf of a registered team')
    parser_submit.add_argument('--submitterAlias', '--alias', metavar='ALIAS', type=str,
                               help='A nickname, possibly for display in leaderboards')
    parser_submit.add_argument('--used', metavar='target', type=str, nargs='*',
                               help=USED_HELP)
    parser_submit.add_argument('--executed', metavar='target', type=str, nargs='*',
                               help=EXECUTED_HELP)
    parser_submit.add_argument('--limitSearch', metavar='projId', type=str,
                               help='Synapse ID of a container such as project or folder to limit search for '
                                    'provenance files.')
    parser_submit.set_defaults(func=submit)

    parser_show = subparsers.add_parser('show', help='show metadata for an entity')
    parser_show.add_argument('id', metavar='syn123', type=str,
                             help='Synapse ID of form syn123 of desired synapse object')
    parser_show.add_argument('--limitSearch', metavar='projId', type=str,
                             help='Synapse ID of a container such as project or folder to limit search for provenance '
                                  'files.')
    parser_show.set_defaults(func=show)

    parser_cat = subparsers.add_parser('cat', help='prints a dataset from Synapse')
    parser_cat.add_argument('id', metavar='syn123', type=str,
                            help='Synapse ID of form syn123 of desired data object')
    parser_cat.add_argument('-v', '--version', metavar='VERSION', type=int, default=None,
                            help='Synapse version number of entity to display. Defaults to most recent version.')
    parser_cat.set_defaults(func=cat)

    parser_list = subparsers.add_parser('list',
                                        help='List Synapse entities contained by the given Project or Folder. '
                                             'Note: May not be supported in future versions of the client.')
    parser_list.add_argument('id', metavar='syn123', type=str,
                             help='Synapse ID of a project or folder')
    parser_list.add_argument('-r', '--recursive', action='store_true', default=False, required=False,
                             help='recursively list contents of the subtree descending from the given Synapse ID')
    parser_list.add_argument('-l', '--long', action='store_true', default=False, required=False,
                             help='List synapse entities in long format')
    parser_list.add_argument('-m', '--modified', action='store_true', default=False, required=False,
                             help='List modified by and modified date')
    parser_list.set_defaults(func=ls)

    parser_set_provenance = subparsers.add_parser('set-provenance',
                                                  help='create provenance records')
    parser_set_provenance.add_argument('-id', '--id', metavar='syn123', type=str, required=True,
                                       help='Synapse ID of entity whose provenance we are accessing.')
    parser_set_provenance.add_argument('-name', '--name', metavar='NAME', type=str, required=False,
                                       help='Name of the activity that generated the entity')
    parser_set_provenance.add_argument('-description', '--description',
                                       metavar='DESCRIPTION', type=str, required=False,
                                       help='Description of the activity that generated the entity')
    parser_set_provenance.add_argument('-o', '-output', '--output', metavar='OUTPUT_FILE', dest='output',
                                       const='STDOUT', nargs='?', type=str,
                                       help='Output the provenance record in JSON format')
    parser_set_provenance.add_argument('-used', '--used', metavar='target', type=str, nargs='*',
                                       help=USED_HELP)
    parser_set_provenance.add_argument('-executed', '--executed', metavar='target', type=str, nargs='*',
                                       help=EXECUTED_HELP)
    parser_set_provenance.add_argument('-limitSearch', '--limitSearch', metavar='projId', type=str,
                                       help='Synapse ID of a container such as project or folder to limit search for '
                                            'provenance files.')
    parser_set_provenance.set_defaults(func=setProvenance)

    parser_get_provenance = subparsers.add_parser('get-provenance',
                                                  help='show provenance records')
    parser_get_provenance.add_argument('-id', '--id', metavar='syn123', type=str, required=True,
                                       help='Synapse ID of entity whose provenance we are accessing.')
    parser_get_provenance.add_argument('--version', metavar='version', type=int, required=False,
                                       help='version of Synapse entity whose provenance we are accessing.')

    parser_get_provenance.add_argument('-o', '-output', '--output', metavar='OUTPUT_FILE', dest='output',
                                       const='STDOUT', nargs='?', type=str,
                                       help='Output the provenance record in JSON format')
    parser_get_provenance.set_defaults(func=getProvenance)

    parser_set_annotations = subparsers.add_parser('set-annotations',
                                                   help='create annotations records')
    parser_set_annotations.add_argument("--id", metavar='syn123', type=str, required=True,
                                        help='Synapse ID of entity whose annotations we are accessing.')
    parser_set_annotations.add_argument('--annotations', metavar='ANNOTATIONS', type=str, required=True,
                                        help="Annotations to add as a JSON formatted string, should evaluate to a "
                                             "dictionary (key/value pairs). Example: '{\"foo\": 1, \"bar\":\"quux\"}'")
    parser_set_annotations.add_argument('-r', '--replace', action='store_true', default=False,
                                        help='Replace all existing annotations with the given annotations')
    parser_set_annotations.set_defaults(func=setAnnotations)

    parser_get_annotations = subparsers.add_parser('get-annotations',
                                                   help='show annotations records')
    parser_get_annotations.add_argument('--id', metavar='syn123', type=str, required=True,
                                        help='Synapse ID of entity whose annotations we are accessing.')
    parser_get_annotations.add_argument('-o', '--output', metavar='OUTPUT_FILE', dest='output',
                                        const='STDOUT', nargs='?', type=str,
                                        help='Output the annotations record in JSON format')
    parser_get_annotations.set_defaults(func=getAnnotations)

    parser_create = subparsers.add_parser('create',
                                          help='Creates folders or projects on Synapse')
    parser_create.add_argument('-parentid', '-parentId', '--parentid', '--parentId', metavar='syn123', type=str,
                               dest='parentid', required=False,
                               help='Synapse ID of project or folder where to place folder [not used with project]')
    parser_create.add_argument('-name', '--name', metavar='NAME', type=str, required=True,
                               help='Name of folder/project.')
    description_group_create = parser_create.add_mutually_exclusive_group()
    description_group_create.add_argument('-description', '--description', metavar='DESCRIPTION', type=str,
                                          help='Description of project/folder')
    description_group_create.add_argument('-descriptionFile', '--descriptionFile', metavar='DESCRIPTION_FILE_PATH',
                                          type=str,
                                          help='Path to a markdown file containing description of project/folder')
    parser_create.add_argument('type', type=str,
                               help='Type of object to create in synapse one of {Project, Folder}')
    parser_create.set_defaults(func=create)

    # parser_update = subparsers.add_parser('update',
    #         help='uploads a new file to an existing Synapse Entity')
    # parser_update.add_argument('-id', metavar='syn123', type=str, required=True,
    #         help='Synapse ID of entity to be updated')
    # parser_update.add_argument('file', type=str,
    #         help='file to be added to synapse.')
    # parser_update.set_defaults(func=update)

    parser_store_table = subparsers.add_parser('store-table',
                                               help='Creates a Synapse Table given a csv')
    parser_store_table.add_argument('--name', metavar='NAME', type=str,
                                    required=True, help='Name of Table')
    parser_store_table.add_argument('--parentid', '--parentId',
                                    metavar='syn123', type=str,
                                    dest='parentid', required=False,
                                    help='Synapse ID of project')
    parser_store_table.add_argument('--csv',
                                    metavar='foo.csv', type=str,
                                    required=False,
                                    help='Path to csv')
    parser_store_table.set_defaults(func=storeTable)

    parser_onweb = subparsers.add_parser('onweb',
                                         help='opens Synapse website for Entity')
    parser_onweb.add_argument('id', type=str, help='Synapse id')
    parser_onweb.set_defaults(func=onweb)

    # the purpose of the login command (as opposed to just using the -u and -p args) is
    # to allow the command line user to cache credentials
    parser_login = subparsers.add_parser('login',
                                         help='login to Synapse and (optionally) cache credentials')
    parser_login.add_argument('-u', '--username', dest='synapseUser',
                              help='Username used to connect to Synapse')
    parser_login.add_argument('-p', '--password', dest='synapsePassword',
                              help='Password used to connect to Synapse')
    parser_login.add_argument('--rememberMe', '--remember-me', dest='rememberMe', action='store_true', default=False,
                              help='Cache credentials for automatic authentication on future interactions with Synapse')
    parser_login.set_defaults(func=login)

    # test character encoding
    parser_test_encoding = subparsers.add_parser('test-encoding',
                                                 help='test character encoding to help diagnose problems')
    parser_test_encoding.set_defaults(func=test_encoding)

    return parser


def perform_main(args, syn):
    if 'func' in args:
        try:
            args.func(args, syn)
        except Exception as ex:
            if args.debug:
                raise
            else:
                sys.stderr.write(utils._synapse_error_msg(ex))


def login_with_prompt(syn, user, password, rememberMe=False, silent=False, forced=False):
    try:
        syn.login(user, password, silent=silent, rememberMe=rememberMe, forced=forced)
    except SynapseNoCredentialsError:
        # if there were no credentials in the cache nor provided, prompt the user and try again
        while not user:
            user = input("Synapse username: ")

        passwd = None
        while not passwd:
            # must encode password prompt because getpass() has OS-dependent implementation and complains about unicode
            # on Windows python 2.7
            passwd = getpass.getpass(("Password for " + user + ": ").encode('utf-8'))
        syn.login(user, passwd, rememberMe=rememberMe, forced=forced)


def main():
    args = build_parser().parse_args()
    synapseclient.USER_AGENT['User-Agent'] = "synapsecommandlineclient " + synapseclient.USER_AGENT['User-Agent']
    syn = synapseclient.Synapse(debug=args.debug, skip_checks=args.skip_checks, configPath=args.configPath)
    if not ('func' in args and args.func == login):
        # if we're not executing the "login" operation, automatically authenticate before running operation
        login_with_prompt(syn, args.synapseUser, args.synapsePassword, silent=True)
    perform_main(args, syn)


if __name__ == "__main__":
    main()
