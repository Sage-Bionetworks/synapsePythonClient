"""
***********
Evaluations
***********

An evaluation object represents a collection of Synapse entities that will be
processed in a particular way. This could mean scoring entries in a challenge
or executing a processing pipeline.

Evaluations can be retrieved by ID::

    evaluation = syn.getEvaluation(1901877)

Entities may be submitted for evaluation. The `syn.submit` method returns a
`Submission` object::

    entity = syn.get(synapse_id)
    submission = syn.submit(evaluation, entity)

The submission has a `Status`::

    status = syn.getSubmissionStatus(submission)

See:
    Synapse.getEvaluation
    Synapse.submit
    Synapse.addEvaluationParticipant
    Synapse.getSubmissions
    Synapse.getSubmission
    Synapse.getSubmissionStatus
"""
import sys
from exceptions import ValueError

from synapseclient.dict_object import DictObject


class Evaluation(DictObject):
    """Keeps track of an evaluation in Synapse.  Allowing for
    submissions, retrieval and scoring.

    Evaluations can be retrieved from Synapse by ID::

        evaluation = syn.getEvaluation(1901877)

    see:
        synapseclient.evaluation
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
        - `status`: One of ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED'] default 'OPEN'
        - `contentSource` : content Source 
        """
        kwargs['status'] = kwargs.get('status', 'OPEN')
        kwargs['contentSource'] = kwargs.get('contentSource', '')
        if  kwargs['status'] not in ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED']:
            sys.stderr.write('\nEvaluation Status must be one of [OPEN, PLANNED, CLOSED, COMPLETED]\n\n')
            raise ValueError
        if not kwargs['contentSource'].startswith('syn'):   #Verify that synapse Id given
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
    A Submission object is returned from the `Synapse.submit` method.

    Fields: createdOn, entityBundleJSON, entityId, evaluationId, id, name, userId, versionNumber
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s' %id


    def __init__(self, **kwargs):
        """Builds an Synapse submission object. These objects will be returned
        from the `Synapse.submit` method, rather than created by end users.

        Arguments:
        - `entityId`: Synapse ID of the entity to submit
        - `evaluationId`: ID of the evaluation to which the entity is to be submitted
        - `versionNumber`: Version number of the submitted entity
        """
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
    A SubmissionStatus object is returned from the `Synapse.getSubmissionStatus`
    method.

    Fields: etag, id, modifiedOn, score, status

    Status can be one of {'OPEN', 'CLOSED', 'SCORED', 'INVALID'}.
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s/status' %id


    def __init__(self, **kwargs):
        """Builds an Synapse submission object.
        """
        super(SubmissionStatus, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def putURI(self):
        return '/evaluation/submission/%s/status' %self.id

    def deleteURI(self):
        return '/evaluation/submission/%s/status' %self.id


            
