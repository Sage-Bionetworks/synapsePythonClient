import sys
from exceptions import ValueError

from dict_object import DictObject


class Evaluation(DictObject):
    """Keeps track of an evaluation in Synapse.  Allowing for
    submissions, retrieval and scoring.
    """

    @classmethod
    def getByNameURI(cls, name):
        return '/evaluation/name/%s' %name
    
    @classmethod
    def getURI(cls, id):
        return '/evaluation/%s' %id


    def __init__(self, **kwargs):
        """Builds an Synapse evaluation object based on information of:

        Arguments:
        - `name`: Name of evaluation
        - `description`: A short description describing the evaluation
        - `status`: One of ['OPEN', 'PENDING', 'CLOSED'] default 'OPEN'
        - `contentSource` : content Source 
        """
        kwargs['status'] = kwargs.get('status', 'OPEN')
        kwargs['contentSource'] = kwargs.get('contentSource', '')
        if  kwargs['status'] not in ['OPEN', 'PENDING', 'CLOSED']:
            sys.stderr.write('\nEvaluation Status must be one of [OPEN, PENDING, CLOSED]\n\n')
            raise ValueError
        super(Evaluation, self).__init__(kwargs)


    def postURI(self):
        return '/evaluation'

    def putURI(self):
        return '/evaluation/%s' %self.id

    def deleteURI(self):
        return '/evaluation/%s' %self.id


class Submission(DictObject):

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s' %id


    def __init__(self, **kwargs):
        """Builds an Synapse submission object based on information of:

        Arguments:
        """
        super(Submission, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission?etag=%s' %self.etag

    def putURI(self):
        return '/evaluation/submission/%s' %self.id

    def deleteURI(self):
        return '/evaluation/submission/%s' %self.id


class SubmissionStatus(DictObject):

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s/status' %id


    def __init__(self, **kwargs):
        """Builds an Synapse submission object based on information of:

        Arguments:
        """
        super(SubmissionStatus, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def putURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def deleteURI(self):
        return '/evaluation/submission/%s/status' %self.id


            
