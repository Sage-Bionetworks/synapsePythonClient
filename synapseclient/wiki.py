"""
****
Wiki
****
A Wiki page requires a title, markdown and an owner object.  
For example::

    entity = syn.get('syn123456')

    content = \"\"\"
    My Wiki Page
    ============

    A **bold** statement in markdown!

    An attached image:
    ${image?fileName=foo.png&align=none}
    \"\"\"

    wiki = Wiki(title='My Wiki Page', owner=entity, markdown=content)

    ## temporary work-around
    fileHandle = syn._uploadFileToFileHandleService('/path/to/foo.png')
    wiki.attachmentFileHandleIds = [fileHandle['id']]

    wiki = syn.store(wiki)
   
~~~~~~~~~~
Wiki Class
~~~~~~~~~~

.. autoclass:: synapseclient.wiki.Wiki
   :members:

"""

from exceptions import ValueError
import sys
import json

from synapseclient.dict_object import DictObject
from synapseclient.utils import guess_object_type, id_of

class Wiki(DictObject):
    """
    Represents a wiki page in Synapse with content specified in markdown.
    
    :param title:         Title of Wiki
    :param owner:         Parent Entity or Evaluation that Wiki belongs to
    :param markdown:      Content of Wiki
    :param attachmentIds: List of file handle IDs representing attached files, such as image files
    """
    
    __PROPERTIES = ('title', 'markdown', 'attachmentFileHandleIds', 'id', 'etag', 'createdBy', 'createdOn', 'modifiedBy', 'modifiedOn', 'parentWikiId')

    def __init__(self, **kwargs):
        #Verify that the parameters are correct
        if not 'owner' in kwargs:
            sys.stderr.write('Wiki constructor must have an owner specified')
            raise ValueError

        super(Wiki, self).__init__(kwargs)
        self.ownerType=guess_object_type(self.owner)
        self.ownerId=id_of(self.owner)
        del self['owner']
        

    def json(self):
        """Returns the JSON represenation of the Wiki object."""
        return json.dumps({k:v for k,v in self.iteritems() 
                           if k in self.__PROPERTIES})


    def getURI(self):
        """TODO_Sphinx"""
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

    def postURI(self):
        """TODO_Sphinx"""
        return '/%s/%s/wiki' % (self.ownerType, self.ownerId)

    def putURI(self):
        """TODO_Sphinx"""
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

    def deleteURI(self):
        """TODO_Sphinx"""
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

        

