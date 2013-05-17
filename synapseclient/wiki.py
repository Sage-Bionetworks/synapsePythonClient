from exceptions import ValueError
import sys
import json

from synapseclient.dict_object import DictObject
from synapseclient.utils import guess_object_type, id_of



class Wiki(DictObject):
    """Keeps track of an evaluation in Synapse.  Allowing for
    submissions, retrieval and scoring.
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

        

