import sys
from exceptions import ValueError

from synapseclient.dict_object import DictObject


class Evaluation(DictObject):
    """
    Keeps track of an evaluation in Synapse.  Allowing for
    submissions, retrieval and scoring.

    Evaluations can be retrieved from Synapse by ID::

        evaluation = syn.getEvaluation(1901877)
    
    :param name:          Name of the evaluation
    :param description:   A short description describing the evaluation
    :param status:        One of {'OPEN', 'PLANNED', 'CLOSED', 'COMPLETED'}.  Defaults to 'OPEN'
    :param contentSource: Source of the evaluation's content
    """

    @classmethod
    def getByNameURI(cls, name):
        return '/evaluation/name/%s' %name
    
    @classmethod
    def getURI(cls, id):
        return '/evaluation/%s' %id


    def __init__(self, **kwargs):
        kwargs['status'] = kwargs.get('status', 'OPEN')
        kwargs['contentSource'] = kwargs.get('contentSource', '')
        if  kwargs['status'] not in ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED']:
            sys.stderr.write('\nEvaluation Status must be one of [OPEN, PLANNED, CLOSED, COMPLETED]\n\n')
            raise ValueError
        super(Evaluation, self).__init__(kwargs)


    def postURI(self):
        return '/evaluation'

    def putURI(self):
        return '/evaluation/%s' %self.id

    def deleteURI(self):
        return '/evaluation/%s' %self.id


class Submission(DictObject):
    """
    Builds an Synapse submission object

    :param entityId:      Synapse ID of the Entity to submit
    :param evaluationId:  ID of the Evaluation to which the Entity is to be submitted
    :param versionNumber: Version number of the submitted Entity
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s' %id


    def __init__(self, **kwargs):
        if not ('evaluationId' in kwargs and 
                'entityId' in kwargs and
                'versionNumber' in kwargs):
            raise KeyError

        super(Submission, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission?etag=%s' %self.etag

    def putURI(self):
        return '/evaluation/submission/%s' %self.id

    def deleteURI(self):
        return '/evaluation/submission/%s' %self.id


class SubmissionStatus(DictObject):
    """
    Builds an Synapse submission status object

    :param etag:       TODO_Sphinx
    :param id:         TODO_Sphinx
    :param modifiedOn: TODO_Sphinx
    :param score:      TODO_Sphinx
    :param status:     TODO_Sphinx
                       Status can be one of {'OPEN', 'CLOSED', 'SCORED', 'INVALID'}.
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s/status' %id


    def __init__(self, **kwargs):
        super(SubmissionStatus, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def putURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def deleteURI(self):
        return '/evaluation/submission/%s/status' %self.id