"""
***************************
Synapse command line client
***************************

For help, type synapse -h.

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


def query(args, syn):
    """
    """
    #TODO: Should use loop over multiple returned values if return is too long
    results = syn.query(' '.join(args.queryString))

    results = results['results']
    if len(results)==0:  #No results found
        return
    headings = {}
    for r in results:
        for c in r:
            headings[c] = True
    print '\t'.join(headings)
    for result in results:
        out = []
        for k in headings:
            out.append(str(result.get(k, "")))
        print "\t".join(out)

def get(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    ent = syn.downloadEntity(args.id)
    if 'files' in ent:
        for f in ent['files']:
            src = os.path.join(ent['cacheDir'], f)
            dst = os.path.join('.', f.replace(".R_OBJECTS/",""))
            sys.stderr.write('creating %s\n' %dst)
            if not os.path.exists(os.path.dirname(dst)):
                os.mkdir(dst)
            shutil.copyfile(src, dst)
    else:
        sys.stderr.write('WARNING: No files associated with entity %s\n' % (args.id,))
        syn.printEntity(ent)
    return ent

def cat(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    ent = syn.downloadEntity(args.id)
    if 'files' in ent:
        for f in ent['files']:
            with open(os.path.join(ent['cacheDir'], f)) as fp:
                for l in fp:
                    sys.stdout.write(l)

def show(args, syn):
    """
    show metadata for an entity
    """
    ent = syn.getEntity(args.id)
    syn.printEntity(ent)

def delete(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    syn.deleteEntity(args.id)
    sys.stderr.write('Deleted entity: %s\n' % args.id)

    
def upload(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    if args.type == 'File': args.type = 'FileEntity'
    entity={'name': args.name,
            'parentId': args.parentid,
            'description':args.description,
            'entityType': u'org.sagebionetworks.repo.model.%s' %args.type}

    entity = syn.uploadFile(entity, args.file, used=args.used, executed=args.executed)

    sys.stderr.write('Created entity: %s\t%s from file: %s\n' %(entity['id'], entity['name'], args.file))
    return(entity)


def create(args, syn):
    """

    Arguments:
    - `args`:
    """
    if args.type == 'File': args.type = 'FileEntity'
    entity={'name': args.name,
            'parentId': args.parentid,
            'description':args.description,
            'entityType': u'org.sagebionetworks.repo.model.%s' %args.type}
    entity=syn.createEntity(entity)
    sys.stderr.write('Created entity: %s\t%s\n' %(entity['id'],entity['name']))
    return(entity)


def update(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    entity=syn.getEntity(args.id)
    entity = syn.uploadFile(entity, args.file)
    sys.stderr.write('Updated entity: %s\t%s from file: %s\n' %(entity['id'],entity['name'], args.file))


def onweb(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    syn.onweb(args.id)


def setProvenance(args, syn):
    """
    set provenance information on a synapse entity
    """
    activity = Activity(name=args.name, description=args.description)
    if args.used:
        for item in args.used:
            activity.used(item)
    if args.executed:
        for item in args.executed:
            activity.used(item, wasExecuted=True)
    activity = syn.setProvenance(args.id, activity)

    ## display the activity record, if -o or -output specified
    if args.output:
        if args.output=='STDOUT':
            sys.stdout.write(json.dumps(activity))
            sys.stdout.write('\n')
        else:
            with open(args.output, 'w') as f:
                f.write(json.dumps(activity))
                f.write('\n')
    else:
        sys.stdout.write('Set provenance record %s on entity %s\n' % (str(activity['id']), str(args.id),))


def getProvenance(args, syn):
    activity = syn.getProvenance(args.id)

    if args.output is None or args.output=='STDOUT':
        sys.stdout.write(json.dumps(activity))
        sys.stdout.write('\n')
    else:
        with open(args.output, 'w') as f:
            f.write(json.dumps(activity))
            f.write('\n')


def main():
    parser = argparse.ArgumentParser(description='Interfaces with the Synapse repository.')
    parser.add_argument('--version', action='version', version='Synapse Client %s' % synapseclient.__version__)
    parser.add_argument('-u', '--username', dest='synapseUser', help='Username used to connect to Synapse')
    parser.add_argument('-p', '--password', dest='synapsePassword', help='Password used to connect to Synapse')
    parser.add_argument('--debug', dest='debug', action='store_true')


    subparsers = parser.add_subparsers(title='subcommands', description='valid subcommands',
                                       help='additional help')


    #parser_login = subparsers.add_parser('login', help='login to Synapse')
    #parser_login.add_argument('synapseUser', metavar='USER', type=str, help='Synapse username')
    #parser_login.add_argument('synapsePassword', metavar='PASSWORD', type=str, help='Synapse password')

    
    parser_query = subparsers.add_parser('query', help='Performs SQL like queries on Synapse')
    parser_query.add_argument('queryString', metavar='string', type=str, nargs='*',
                         help='A query string, see https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+API#RepositoryServiceAPI-QueryAPI for more information')
    parser_query.set_defaults(func=query)

    parser_get = subparsers.add_parser('get', help='downloads a dataset from Synapse')
    parser_get.add_argument('id', metavar='syn123', type=str, 
                         help='Synapse ID of form syn123 of desired data object')
    parser_get.set_defaults(func=get)

    parser_get = subparsers.add_parser('show', help='show metadata for an entity')
    parser_get.add_argument('id', metavar='syn123', type=str, 
                         help='Synapse ID of form syn123 of desired synapse object')
    parser_get.set_defaults(func=show)

    parser_cat = subparsers.add_parser('cat', help='prints a dataset from Synapse')
    parser_cat.add_argument('id', metavar='syn123', type=str,
                         help='Synapse ID of form syn123 of desired data object')
    parser_cat.set_defaults(func=cat)

    parser_add = subparsers.add_parser('add', help='uploads and adds a dataset to Synapse')
    parser_add.add_argument('-parentid', '-parentId', metavar='syn123', type=str, required=True, 
                         help='Synapse ID of project or folder where to upload data.')
    #TODO make so names can have white space
    parser_add.add_argument('-name', metavar='NAME', type=str, required=False,
                         help='Name of data object in Synapse')
    #TODO make sure that description can have whitespace
    parser_add.add_argument('-description', metavar='DESCRIPTION', type=str, 
                         help='Description of data object in Synapse.')
    parser_add.add_argument('-type', type=str, default='File',
                         help='Type of object to create in synapse. Defaults to "File". Deprecated object types include "Data" and "Code".')
    parser_add.add_argument('-used', metavar='TargetID', type=str, nargs='*',
                         help='ID of a target data entity from which the specified entity is derived')
    parser_add.add_argument('-executed', metavar='TargetID', type=str, nargs='*',
                         help='ID of a code entity from which the specified entity is derived')
    parser_add.add_argument('file', type=str,
                         help='file to be added to synapse.')
    parser_add.set_defaults(func=upload)


    parser_set_provenance = subparsers.add_parser('set-provenance', help='create provenance records')
    parser_set_provenance.add_argument('-id', metavar='syn123', type=str, required=True,
                         help='Synapse ID of entity whose provenance we are accessing.')
    parser_set_provenance.add_argument('-name', metavar='NAME', type=str, required=False,
                         help='Name of the activity that generated the entity')
    parser_set_provenance.add_argument('-description', metavar='DESCRIPTION', type=str, required=False, 
                         help='Description of the activity that generated the entity')
    parser_set_provenance.add_argument('-o', '-output', metavar='OUTPUT_FILE', dest='output',
                         const='STDOUT', nargs='?', type=str,
                         help='Output the provenance record in JSON format')
    parser_set_provenance.add_argument('-used', metavar='TargetID', type=str, nargs='*',
                         help='ID of a target data entity from which the specified entity is derived')
    parser_set_provenance.add_argument('-executed', metavar='TargetID', type=str, nargs='*',
                         help='ID of a code entity from which the specified entity is derived')
    parser_set_provenance.set_defaults(func=setProvenance)


    parser_get_provenance = subparsers.add_parser('get-provenance', help='show provenance records')
    parser_get_provenance.add_argument('-id', metavar='syn123', type=str, required=True,
                         help='Synapse ID of entity whose provenance we are accessing.')
    parser_get_provenance.add_argument('-o', '-output', metavar='OUTPUT_FILE', dest='output',
                         const='STDOUT', nargs='?', type=str,
                         help='Output the provenance record in JSON format')
    parser_get_provenance.set_defaults(func=getProvenance)


    parser_create = subparsers.add_parser('create', help='Creates folders or projects on Synapse')
    parser_create.add_argument('-parentid', '-parentId', metavar='syn123', type=str, required=False, 
                         help='Synapse ID of project or folder where to place folder [not used with project]')
    #TODO make so names can have white space
    parser_create.add_argument('-name', metavar='NAME', type=str, required=True,
                         help='Name of folder/project.')
    #TODO make sure that description can have whitespace
    parser_create.add_argument('-description', metavar='DESCRIPTION', type=str, 
                         help='Description of project/folder')
    parser_create.add_argument('type', type=str,
                         help='Type of object to create in synapse one of {Project, Folder}')
    parser_create.set_defaults(func=create)


    parser_update = subparsers.add_parser('update', help='uploads a new file to an existing Synapse Entity')
    parser_update.add_argument('-id', metavar='syn123', type=str, required=True,
                         help='Synapse ID of entity to be updated')
    parser_update.add_argument('file', type=str,
                         help='file to be added to synapse.')
    parser_update.set_defaults(func=update)

    parser_delete = subparsers.add_parser('delete', help='removes a dataset from Synapse')
    parser_delete.add_argument('id', metavar='syn123', type=str,
                         help='Synapse ID of form syn123 of desired data object')
    parser_delete.set_defaults(func=delete)


    parser_onweb = subparsers.add_parser('onweb', help='opens Synapse website for Entity')
    parser_onweb.add_argument('id', type=str,
                         help='Synapse id')
    parser_onweb.set_defaults(func=onweb)

    args = parser.parse_args()

    #TODO Perform proper login either prompt for info or use parameters
    ## if synapseUser and synapsePassword are not given, try to use cached session token
    syn = synapseclient.Synapse(debug=args.debug)
    syn.login(args.synapseUser, args.synapsePassword)

    #Perform the requested action
    if 'func' in args:
        try:
            args.func(args, syn)
        except Exception as ex:
            sys.stderr.write(utils.synapse_error_msg(ex))

            if args.debug:
                raise



## call main method if this file is run as a script
if __name__ == "__main__":
    main()

