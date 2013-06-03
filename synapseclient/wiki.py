from exceptions import ValueError
import sys
import json

from synapseclient.dict_object import DictObject
from synapseclient.utils import guess_object_type, id_of



class Wiki(DictObject):
    """
    Represent a wiki page in Synapse with content specified in markdown.

    A wiki page requires a title, markdown and an owner object.

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



    """
    __PROPERTIES = ('title', 'markdown', 'attachmentFileHandleIds', 'id', 'etag', 'createdBy', 'createdOn', 'modifiedBy', 'modifiedOn', 'parentWikiId')

    def __init__(self, **kwargs):
        """Creates a wiki page atached to a owner entity or evaluation

        Arguments:
        - `title`: Title of wiki
        - `owner`: parent entity of evaluation that wiki belongs to
        - `markdown`: Content of wiki
        - `attachmentIds`: List of file handle ids representing attached files such as image files
        """
        #Verify that the parameters are correct
        if not 'owner' in kwargs:
            sys.stderr.write('Wiki constructor must have an owner specified')
            raise ValueError

        super(Wiki, self).__init__(kwargs)
        self.ownerType=guess_object_type(self.owner)
        self.ownerId=id_of(self.owner)
        del self['owner']
        

    def json(self):
        """Returns json represenation of object"""
        return json.dumps({k:v for k,v in self.iteritems() 
                           if k in self.__PROPERTIES})


    def getURI(self):
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

    def postURI(self):
        return '/%s/%s/wiki' % (self.ownerType, self.ownerId)

    def putURI(self):
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

    def deleteURI(self):
        return '/%s/%s/wiki/%s' % (self.ownerType, self.ownerId, self.id)

        

