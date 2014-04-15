"""
***************************
Synapse command line client
***************************

The Synapse client can be used from the command line via the **synapse**
command. For help, type::

    synapse -h.

For help on commands, type::

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
  * **login**            - login to Synapse and (optionally) cache credentials
  * **get**              - download an entity and associated data
  * **store**            - create or update
  * **delete**           - removes a dataset from Synapse
  * **query**            - performs SQL like queries on Synapse
  * **submit**           - submit an entity for evaluation
  * **set-provenance**   - create provenance records
  * **get-provenance**   - show provenance records
  * **show**             - show metadata for an entity
  * **onweb**            - opens Synapse website for Entity
  * **show**             - Displays information about a Entity

A few more commands (cat, create, update, associate)

"""

import argparse
import os
import shutil
import sys
import synapseclient
from synapseclient import Activity
import utils
import signal
import json
import warnings
from synapseclient.exceptions import *


def query(args, syn):
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, ValueError) as ex1:
        ## Different OS's have different signals defined. In particular,
        ## SIGPIPE doesn't exist one Windows. The docs have this to say,
        ## "On Windows, signal() can only be called with SIGABRT, SIGFPE,
        ## SIGILL, SIGINT, SIGSEGV, or SIGTERM. A ValueError will be raised
        ## in any other case."
        pass
    ## TODO: Should use loop over multiple returned values if return is too long
    results = syn.chunkedQuery(' '.join(args.queryString))

    headings = {}
    temp = [] # Since query returns a generator, the results must be stored locally
    for res in results:
        temp.append(res)
        for head in res:
            headings[head] = True
    if len(headings) == 0: # No results found
        return 
    sys.stdout.write('%s\n' %'\t'.join(headings))
    
    for res in temp:
        out = []
        for key in headings:
            out.append(str(res.get(key, "")))
        sys.stdout.write('%s\n' % "\t".join(out))

        
def get(args, syn):
    entity = syn.get(args.id, limitSearch=args.limitSearch)
    
    ## TODO: Is this part even necessary?
    ## (Other than the print statements)
    if 'files' in entity:
        for file in entity['files']:
            src = os.path.join(entity['cacheDir'], file)
            dst = os.path.join('.', file.replace(".R_OBJECTS/",""))
            print 'Creating %s' % dst
            if not os.path.exists(os.path.dirname(dst)):
                os.mkdir(dst)
            shutil.copyfile(src, dst)
    else:
        sys.stderr.write('WARNING: No files associated with entity %s\n' % args.id)
        syn.printEntity(entity)


def store(args, syn):
    #If we are storing a fileEntity we need to have id or parentId
    if args.parentid is None and args.id is None and args.file is not None:
        raise ValueError('synapse store requires at least either parentId or id to be specified.')
    #If both args.FILE and args.file specified raise error
    if args.file and args.FILE:
        raise ValueError('only specify one file')
    args.file = args.FILE if args.FILE is not None else args.file
    args.type = 'FileEntity' if args.type == 'File' else args.type

    if args.id is not None:
        entity = syn.get(args.id)
    else:
        entity = {'concreteType': u'org.sagebionetworks.repo.model.%s' % args.type, 
                  'name': utils.guess_file_name(args.file) if args.file and not args.name else None,
                  'parentId' : None,
                  'description' : None,
                  'path': args.file}
    #Overide setting for parameters included in args
    entity['name'] =  args.name if args.name is not None else entity['name']
    entity['description'] = args.description if args.description is not None else entity['description']
    entity['parentId'] = args.parentid if args.parentid is not None else entity['parentId']
    entity['path'] = args.file if args.file is not None else None
    if utils.is_url(args.file):
        entity['synapseStore'] = False

    used = _convertProvenanceList(args.used, args.limitSearch, syn)
    executed = _convertProvenanceList(args.executed, args.limitSearch, syn)
    entity = syn.store(entity, used=used, executed=executed)
    print 'Created/Updated entity: %s\t%s' %(entity['id'], entity['name'])


def associate(args, syn):
    if args.r:
        files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(args.path) for f in filenames]
    if os.path.isfile(args.path):
        files = [args.path]
    if len(files) ==0:
        raise Exception('The path specified is innacurate.  If it is a directory try -r')
    for file in files:
        ent = syn.get(file, limitSearch=args.limitSearch)
        print '%s.%i\t%s' %(ent.id, ent.versionNumber, file)


def cat(args, syn):
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except (AttributeError, ValueError) as ex1:
        ## Different OS's have different signals defined. In particular,
        ## SIGPIPE doesn't exist one Windows. The docs have this to say,
        ## "On Windows, signal() can only be called with SIGABRT, SIGFPE,
        ## SIGILL, SIGINT, SIGSEGV, or SIGTERM. A ValueError will be raised
        ## in any other case."
        pass
    entity = syn.get(args.id)
    if 'files' in entity:
        for file in entity['files']:
            with open(os.path.join(entity['cacheDir'], file)) as input:
                for line in input:
                    print line


def list(args, syn):
    """List entities in a Project or Folder"""
    syn._list(args.id, recursive=args.recursive, long_format=args.long, show_modified=args.modified)

                    
def show(args, syn):
    """Show metadata for an entity."""
    
    ent = syn.get(args.id, downloadFile=False)
    syn.printEntity(ent)
    sys.stdout.write('Provenance:\n')
    try:
        prov = syn.getProvenance(ent)
        print prov
    except SynapseHTTPError as e:
        print '  No Activity specified.\n'

    
def delete(args, syn):
    syn.delete(args.id)
    print 'Deleted entity: %s' % args.id



def create(args, syn):
    entity={'name': args.name,
            'parentId': args.parentid,
            'description':args.description,
            'concreteType': u'org.sagebionetworks.repo.model.%s' %args.type}
    entity=syn.createEntity(entity)
    print 'Created entity: %s\t%s\n' %(entity['id'],entity['name'])


# def update(args, syn):
#     warnings.warn('This method is deprecated', DeprecationWarning)
#     entity = syn.get(args.id)
#     entity.path = args.file
#     entity = syn.store(entity, used=args.used, executed=args.executed)
#     print 'Updated entity: %s\t%s from file: %s\n' %(entity['id'],entity['name'], args.file)


def onweb(args, syn):
    syn.onweb(args.id)


def _convertProvenanceList(usedList, limitSearch, syn):
    if usedList is None:
        return None
    usedList = [syn.get(target, limitSearch=limitSearch) if 
                (os.path.isfile(target) if isinstance(target, basestring) else False) else target for 
                target in usedList]
    return usedList


def setProvenance(args, syn):
    """Set provenance information on a synapse entity."""
    
    activity = Activity(name=args.name, description=args.description)

    if args.used:
        for item in _convertProvenanceList(args.used, args.limitSearch, syn):
            activity.used(item)
    if args.executed:
        for item in _convertProvenanceList(args.executed, args.limitSearch, syn):
            activity.used(item, wasExecuted=True)
    activity = syn.setProvenance(args.id, activity)

    # Display the activity record, if -o or -output specified
    if args.output:
        if args.output=='STDOUT':
            sys.stdout.write(json.dumps(activity))
            sys.stdout.write('\n')
        else:
            with open(args.output, 'w') as f:
                f.write(json.dumps(activity))
                f.write('\n')
    else:
        print 'Set provenance record %s on entity %s\n' % (str(activity['id']), str(args.id))


def getProvenance(args, syn):
    activity = syn.getProvenance(args.id)

    if args.output is None or args.output=='STDOUT':
        print json.dumps(activity,sort_keys=True, indent=2)
    else:
        with open(args.output, 'w') as f:
            f.write(json.dumps(activity))
            f.write('\n')
    
    
def submit(args, syn):
    '''
    Method to allow challenge participants to submit to an evaluation queue
    
    Examples:
    1. #submit to a eval Queue by eval ID , uploading the submission file
    synapse submit --evalID 2343117 -f ~/testing/testing.txt --pid syn2345030 --used syn2351967 --executed syn2351968
    
    2. support for deprecated --evaluation option
    synapse submit --evaluation 'ra_challenge_Q1_leaderboard' -f ~/testing/testing.txt --pid syn2345030 --used syn2351967 --executed syn2351968
    synapse submit --evaluation 2343117 -f ~/testing/testing.txt --pid syn2345030 --used syn2351967 --executed syn2351968
    
    '''
    
    #backward compatibility support
    if args.evaluation is not None:
        sys.stdout.write('[Warning]: Use of --evaluation is deprecated. Use -evalId or -evalName \n')
        #check if evaluation is a number, if so it is assumed to be a evaluationId else it is a evaluationName
        try:
            args.evaluationID = str(int(args.evaluation))
        except ValueError:
            args.evaluationName = args.evaluation
    
    #set the user teamname to username if none is specified
    if args.teamName is None:
        args.teamName = syn.getUserProfile()['userName']
        
    # checking if user has entered a evaluation ID or evaluation Name
    if args.evaluationID is None and args.evaluationName is None:
        raise ValueError('Evaluation ID or Evaluation Name is required\n')
    elif args.evaluationID is not None and args.evaluationName is not None:
        sys.stderr.write('[Warning]: Both Evaluation ID & Evaluation Name are specified \n EvaluationID will be used\n')
    elif args.evaluationID is None: #get evalID from evalName
        try:
            args.evaluationID = syn.getEvaluationByName(args.evaluationName)['id']
        except Exception as e:
            raise ValueError('could not find evaluationID for evaluationName: %s \n' % args.evaluationName)
    
    
    # checking if a entity id or file was specified by the user
    if args.entity is None and args.file is None:
        raise ValueError('Either entityID or filename is required for a submission\n')
    elif args.entity is not  None and args.file is not None:
        sys.stderr.write('[Warning]: Both entityID and filename are specified \n entityID will be used\n')
    elif args.entity is None: #upload the the file to synapse and get synapse entity id for the file
        if args.parentId is None:
            raise ValueError('parentID required with a file upload\n')
        if not os.path.exists(args.file):
            raise IOError('file path %s not valid \n' % args.file)
        # //ideally this should be factored out
        try:
            synFile = syn.store(synapseclient.File(path=args.file,parent=args.parentId),
                                used=_convertProvenanceList(args.used, args.limitSearch, syn),
                                executed=_convertProvenanceList(args.executed, args.limitSearch, syn))
            args.entity = synFile.id
        except Exception as e:
            raise SynapseError('Unable to upload file %s to synapse \n' % args.file)
    try:    
        submission = syn.submit(args.evaluationID, args.entity, name=args.name, teamName=args.teamName)
        sys.stdout.write('Submitted (id: %s) entity: %s\t%s to Evaluation: %s\n' \
            % (submission['id'], submission['entityId'], submission['name'], submission['evaluationId']))
    except Exception as e:
        raise SynapseError('Unable to successfully submit to the evaluation %s \n' % args.evaluationID)
        
        
def login(args, syn):
    """Log in to Synapse, optionally caching credentials"""
    syn.login(args.synapseUser, args.synapsePassword, rememberMe=args.rememberMe)


def build_parser():
    """Builds the argument parser and returns the result."""
    
    parser = argparse.ArgumentParser(description='Interfaces with the Synapse repository.')
    parser.add_argument('--version',  action='version',
            version='Synapse Client %s' % synapseclient.__version__)
    parser.add_argument('-u', '--username',  dest='synapseUser',
            help='Username used to connect to Synapse')
    parser.add_argument('-p', '--password', dest='synapsePassword',
            help='Password used to connect to Synapse')
    parser.add_argument('--debug', dest='debug',  action='store_true')
    parser.add_argument('-s', '--skip-checks', dest='skip_checks', action='store_true',
            help='suppress checking for version upgrade messages and endpoint redirection')

    subparsers = parser.add_subparsers(title='commands',
            description='The following commands are available:',
            help='For additional help: "synapse <COMMAND> -h"')

    parser_get = subparsers.add_parser('get',
            help='downloads a dataset from Synapse')
    parser_get.add_argument('--limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for files if using a path.')
    parser_get.add_argument('id',  metavar='syn123', type=str,
            help='Synapse ID of form syn123 of desired data object')
    parser_get.set_defaults(func=get)

    parser_store = subparsers.add_parser('store', #Python 3.2+ would support alias=['store']
            help='uploads and adds a dataset to Synapse')
    parser_store.add_argument('--parentid', '--parentId', '-parentid', '-parentId', metavar='syn123', type=str, required=False,
            help='Synapse ID of project or folder where to upload data (must be specified if --id is not used.')
    parser_store.add_argument('--id', metavar='syn123', type=str, required=False,
            help='Optional Id of entity in Synapse to be updated.')
    parser_store.add_argument('--name', '-name', metavar='NAME', type=str, required=False,
            help='Name of data object in Synapse')
    parser_store.add_argument('--description', '-description', metavar='DESCRIPTION', type=str, 
            help='Description of data object in Synapse.')
    parser_store.add_argument('--type', type=str, default='File', 
            help='Type of object, such as "File", "Folder", or '
                 '"Project", to create in Synapse. Defaults to "File"')
    parser_store.add_argument('--used', '-used', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path from which the '
                  'specified entity is derived'))
    parser_store.add_argument('--executed', '-executed', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path that was executed '
                  'to generate the specified entity is derived'))
    parser_store.add_argument('--limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for provenance files.')
    parser_store.add_argument('--file', type=str, help=argparse.SUPPRESS)
    parser_store.add_argument('FILE', nargs='?', type=str,
            help='file to be added to synapse.')
    parser_store.set_defaults(func=store)

    parser_add = subparsers.add_parser('add', #Python 3.2+ would support alias=['store']
            help='uploads and adds a dataset to Synapse')
    parser_add.add_argument('--parentid', '--parentId', '-parentid', '-parentId', metavar='syn123', type=str, required=False,
            help='Synapse ID of project or folder where to upload data (must be specified if --id is not used.')
    parser_add.add_argument('--id', metavar='syn123', type=str, required=False,
            help='Optional Id of entity in Synapse to be updated.')
    parser_add.add_argument('--name', '-name', metavar='NAME', type=str, required=False,
            help='Name of data object in Synapse')
    parser_add.add_argument('--description', '-description', metavar='DESCRIPTION', type=str, 
            help='Description of data object in Synapse.')
    parser_add.add_argument('-type', type=str, default='File', help=argparse.SUPPRESS)
    parser_add.add_argument('--used', '-used', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path from which the '
                  'specified entity is derived'))
    parser_add.add_argument('--executed', '-executed', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path that was executed '
                  'to generate the specified entity is derived'))
    parser_add.add_argument('--limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for provenance files.')
    parser_add.add_argument('--file', type=str, help=argparse.SUPPRESS)
    parser_add.add_argument('FILE', nargs='?', type=str,
            help='file to be added to synapse.')
    parser_add.set_defaults(func=store)

    parser_associate = subparsers.add_parser('associate',
            help='Associate local files with the files stored in Synapse')
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
    parser_delete.set_defaults(func=delete)

    parser_query = subparsers.add_parser('query',
            help='Performs SQL like queries on Synapse')
    parser_query.add_argument('queryString', metavar='string', type=str, nargs='*',
            help='A query string, see https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+'
                 'API#RepositoryServiceAPI-QueryAPI for more information')
    parser_query.set_defaults(func=query)
    
    parser_submit = subparsers.add_parser('submit',
            help='submit an entity or a file for evaluation')
    parser_submit.add_argument('--evaluationID', '--evalID', type=str,
            help='Evaluation ID where the entity/file will be submitted')
    parser_submit.add_argument('--evaluationName', '--evalN', type=str,
            help='Evaluation Name where the entity/file will be submitted')
    parser_submit.add_argument('--evaluation', type=str,
            help=argparse.SUPPRESS)  #mainly to maintain the backward compatibility
    parser_submit.add_argument('--entity', '--eid', type=str,
            help='Synapse ID of the entity to be submitted')
    parser_submit.add_argument('--file', '-f', type=str,
            help='File to be submitted to the challenge')
    parser_submit.add_argument('--parentId', '--pid', type=str,
            help='Synapse ID of project or folder where to upload data')
    parser_submit.add_argument('--name', type=str,
            help='Name of the submission')
    parser_submit.add_argument('--teamName', '--team', type=str,
            help='Publicly displayed name of team for the submission[defaults to username]')
    parser_submit.add_argument('--used', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path from which the '
                  'specified entity is derived'))
    parser_submit.add_argument('--executed', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path that was executed '
                  'to generate the specified entity is derived'))
    parser_submit.add_argument('--limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for provenance files.')
    parser_submit.set_defaults(func=submit)

    parser_show = subparsers.add_parser('show', help='show metadata for an entity')
    parser_show.add_argument('id', metavar='syn123', type=str,
            help='Synapse ID of form syn123 of desired synapse object')
    parser_show.add_argument('--limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for provenance files.')
    parser_show.set_defaults(func=show)

    
    parser_cat = subparsers.add_parser('cat', help='prints a dataset from Synapse')
    parser_cat.add_argument('id', metavar='syn123', type=str,
            help='Synapse ID of form syn123 of desired data object')
    parser_cat.set_defaults(func=cat)

    parser_list = subparsers.add_parser('list',
            help='List Synapse entities contained by the given Project or Folder. Note: May not be supported in future versions of the client.')
    parser_list.add_argument('id', metavar='syn123', type=str,
            help='Synapse ID of a project or folder')
    parser_list.add_argument('-r', '--recursive', action='store_true', default=False, required=False,
            help='recursively list contents of the subtree descending from the given Synapse ID')
    parser_list.add_argument('-l', '--long', action='store_true', default=False, required=False,
            help='List synapse entities in long format')
    parser_list.add_argument('-m', '--modified', action='store_true', default=False, required=False,
            help='List modified by and modified date')
    parser_list.set_defaults(func=list)

    parser_set_provenance = subparsers.add_parser('set-provenance',
            help='create provenance records')
    parser_set_provenance.add_argument('-id', metavar='syn123', type=str, required=True,
            help='Synapse ID of entity whose provenance we are accessing.')
    parser_set_provenance.add_argument('-name', metavar='NAME', type=str, required=False,
            help='Name of the activity that generated the entity')
    parser_set_provenance.add_argument('-description',
            metavar='DESCRIPTION', type=str, required=False,
            help='Description of the activity that generated the entity')
    parser_set_provenance.add_argument('-o', '-output', metavar='OUTPUT_FILE', dest='output',
            const='STDOUT', nargs='?', type=str,
            help='Output the provenance record in JSON format')
    parser_set_provenance.add_argument('-used', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path from which the '
                  'specified entity is derived'))
    parser_set_provenance.add_argument('-executed', metavar='target', type=str, nargs='*',
            help=('Synapse ID of a data entity, a url, or a file path that was executed '
                  'to generate the specified entity is derived'))
    parser_set_provenance.add_argument('-limitSearch', metavar='projId', type=str, 
            help='Synapse ID of a container such as project or folder to limit search for provenance files.')
    parser_set_provenance.set_defaults(func=setProvenance)

    parser_get_provenance = subparsers.add_parser('get-provenance',
            help='show provenance records')
    parser_get_provenance.add_argument('-id', metavar='syn123', type=str, required=True,
            help='Synapse ID of entity whose provenance we are accessing.')
    parser_get_provenance.add_argument('-o', '-output', metavar='OUTPUT_FILE', dest='output',
            const='STDOUT', nargs='?', type=str,
            help='Output the provenance record in JSON format')
    parser_get_provenance.set_defaults(func=getProvenance)


    parser_create = subparsers.add_parser('create',
            help='Creates folders or projects on Synapse')
    parser_create.add_argument('-parentid', '-parentId', metavar='syn123', type=str, required=False,
            help='Synapse ID of project or folder where to place folder [not used with project]')
    parser_create.add_argument('-name', metavar='NAME', type=str, required=True,
            help='Name of folder/project.')
    parser_create.add_argument('-description', metavar='DESCRIPTION', type=str,
            help='Description of project/folder')
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

    parser_onweb = subparsers.add_parser('onweb',
            help='opens Synapse website for Entity')
    parser_onweb.add_argument('id', type=str, help='Synapse id')
    parser_onweb.set_defaults(func=onweb)

    ## the purpose of the login command (as opposed to just using the -u and -p args) is
    ## to allow the command line user to cache credentials
    parser_login = subparsers.add_parser( 'login',
            help='login to Synapse and (optionally) cache credentials')
    parser_login.add_argument('-u', '--username', dest='synapseUser',
            help='Username used to connect to Synapse')
    parser_login.add_argument('-p', '--password', dest='synapsePassword',
            help='Password used to connect to Synapse')
    parser_login.add_argument('--rememberMe', '--remember-me', dest='rememberMe', action='store_true', default=False,
            help='Cache credentials for automatic authentication on future interactions with Synapse')
    parser_login.set_defaults(func=login)

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


def main():
    args = build_parser().parse_args()
    synapseclient.USER_AGENT['User-Agent'] = "synapsecommandlineclient " + synapseclient.USER_AGENT['User-Agent']
    syn = synapseclient.Synapse(debug=args.debug, skip_checks=args.skip_checks)
    syn.login(args.synapseUser, args.synapsePassword, silent=True)
    perform_main(args, syn)


if __name__ == "__main__":
    main()

