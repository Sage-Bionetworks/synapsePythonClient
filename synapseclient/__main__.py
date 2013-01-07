import argparse
import os
import shutil
import sys
import synapseclient
import webbrowser
import version_check

def query(args, syn):
    """
    """
    #TODO: Should use loop over multiple returned values if return is too long 
    results = syn.query(' '.join(args.queryString))

    results = results['results']
    headings = results[0].keys()
    print '\t'.join(headings)
    for result in results:
        for k in headings:
            print result[k],'\t',
        print 

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
        sys.stderr.write('WARNING: No files associated with entity %s\n' % (entity['id'],))
        syn.printEntity(ent)
    return ent

def cat(args, syn):
    """
    
    Arguments:
    - `args`:
    """
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
    ent = syn.deleteEntity(args.id)

    
def upload(args, syn):
    """
    
    Arguments:
    - `args`:
    """
    entity={'name': args.name, 
            'parentId': args.parentid, 
            'description':args.description, 
            'entityType': u'org.sagebionetworks.repo.model.Data'}
    entity=syn.createEntity(entity)
    entity = syn.uploadFile(entity, args.file)
    sys.stderr.write('Created entity: %s\t%s from file: %s\n' %(entity['id'],entity['name'], args.file))
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
    webbrowser.open("https://synapse.sagebase.org/#Synapse:%s" %args.id)

def main():
    parser = argparse.ArgumentParser(description='Interfaces with the Synapse repository.')
    parser.add_argument('--version', action='version', version='Synapse Client %s' % synapseclient.__version__)
    parser.add_argument('-u', '--username', dest='synapseUser', help='Username used to connect to Synapse')
    parser.add_argument('-p', '--password', dest='synapsePassword', help='Password used to connect to Synapse')

    subparsers = parser.add_subparsers(title='subcommands', description='valid subcommands',
                                       help='additional help')


    parser_login = subparsers.add_parser('login', help='login to Synapse')
    parser_login.add_argument('synapseUser', metavar='USER', type=str, help='Synapse username')
    parser_login.add_argument('synapsePassword', metavar='PASSWORD', type=str, help='Synapse password')

    
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
    parser_add.add_argument('-parentid', metavar='syn123', type=str, required=True, 
                         help='Synapse ID of project or file where to upload data object')
    #TODO make so names can have white space
    parser_add.add_argument('-name', metavar='This is a data name', type=str, required=True,
                         help='Synapse ID of project or file where to upload data object')
    #TODO make sure that description can have whitespace
    parser_add.add_argument('-description', metavar='syn123', type=str, 
                         help='Synapse ID of project or file where to upload data object')
    parser_add.add_argument('file', type=str,
                         help='file to be added to synapse.')
    parser_add.set_defaults(func=upload)


    parser_update = subparsers.add_parser('update', help='uploads a new file to an existing Synapse Entity')
    parser_update.add_argument('-id', metavar='syn123', type=str, 
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
    syn = synapseclient.Synapse(debug=False)
    syn.login(args.synapseUser, args.synapsePassword)

    #Perform the requested action
    if 'func' in args:
        args.func(args, syn)

    # #print qry


## call main method if this file is run as a script
if __name__ == "__main__":
    main()

