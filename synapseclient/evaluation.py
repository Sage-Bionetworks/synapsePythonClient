"""
***********
Evaluations
***********

An evaluation_ object represents a collection of Synapse Entities that will be processed in a particular way.  This
could mean scoring Entries in a challenge or executing a processing pipeline.

Imports::

    from synapseclient import Evaluation, Submission, SubmissionStatus

Evaluations can be retrieved by ID::

    evaluation = syn.getEvaluation(1901877)

Like entities, evaluations are access controlled via ACLs. The :py:func:`synapseclient.Synapse.getPermissions` and
:py:func:`synapseclient.Synapse.setPermissions` methods work for evaluations:

    access = syn.getPermissions(evaluation, user_id)

The :py:func:`synapseclient.Synapse.submit` method returns a Submission_ object::

    entity = syn.get(synapse_id)
    submission = syn.submit(evaluation, entity, name='My Data', team='My Team')

The Submission object can then be used to check the `status <#submission-status>`_ of the submission::

    status = syn.getSubmissionStatus(submission)

The status of a submission may be:
    - **INVALID** the submitted entity is in the wrong format
    - **SCORED** in the context of a challenge or competition
    - **OPEN** indicating processing *has not* completed
    - **CLOSED** indicating processing *has* completed

Submission status objects can be updated, usually by changing the *status* and *score* fields, and stored back to
Synapse using :py:func:`synapseclient.Synapse.store`::

    status.score = 0.99
    status.status = 'SCORED'
    status = syn.store(status)

See:

- :py:func:`synapseclient.Synapse.getEvaluation`
- :py:func:`synapseclient.Synapse.getEvaluationByContentSource`
- :py:func:`synapseclient.Synapse.getEvaluationByName`
- :py:func:`synapseclient.Synapse.submit`
- :py:func:`synapseclient.Synapse.getSubmissions`
- :py:func:`synapseclient.Synapse.getSubmission`
- :py:func:`synapseclient.Synapse.getSubmissionStatus`
- :py:func:`synapseclient.Synapse.getPermissions`
- :py:func:`synapseclient.Synapse.setPermissions`

~~~~~~~~~~
Evaluation
~~~~~~~~~~

.. autoclass:: synapseclient.evaluation.Evaluation
   :members: __init__
   
~~~~~~~~~~
Submission
~~~~~~~~~~

.. autoclass:: synapseclient.evaluation.Submission
   :members: __init__
   
~~~~~~~~~~~~~~~~~
Submission Status
~~~~~~~~~~~~~~~~~

.. autoclass:: synapseclient.evaluation.SubmissionStatus
   :members: __init__

"""

from synapseclient.core.models.dict_object import DictObject


class Evaluation(DictObject):
    """
    An Evaluation Submission queue, allowing submissions, retrieval and scoring.
    
    :param name:                            Name of the evaluation
    :param description:                     A short description of the evaluation
    :param contentSource:                   Synapse Project associated with the evaluation
    :param submissionReceiptMessage:        Message to display to users upon submission
    :param submissionInstructionsMessage:   Message to display to users detailing acceptable formatting for submissions.

    `To create an Evaluation <http://docs.synapse.org/rest/org/sagebionetworks/evaluation/model/Evaluation.html>`_
    and store it in Synapse::

        evaluation = syn.store(Evaluation(
            name="Q1 Final",
            description="Predict progression of MMSE scores for final scoring",
            contentSource="syn2290704"))

    The contentSource field links the evaluation to its :py:class:`synapseclient.entity.Project`.
    (Or, really, any synapse ID, but sticking to projects is a good idea.)

    `Evaluations <http://docs.synapse.org/rest/org/sagebionetworks/evaluation/model/Evaluation.html>`_ can be retrieved
    from Synapse by ID::

        evaluation = syn.getEvaluation(1901877)

    ...by the Synapse ID of the content source (associated entity)::

        evaluation = syn.getEvaluationByContentSource('syn12345')

    ...or by the name of the evaluation::

        evaluation = syn.getEvaluationByName('Foo Challenge Question 1')

    """

    @classmethod
    def getByNameURI(cls, name):
        return '/evaluation/name/%s' % name

    @classmethod
    def getURI(cls, id):
        return '/evaluation/%s' % id

    def __init__(self, **kwargs):
        kwargs['status'] = kwargs.get('status', 'OPEN')
        kwargs['contentSource'] = kwargs.get('contentSource', '')
        if kwargs['status'] not in ['OPEN', 'PLANNED', 'CLOSED', 'COMPLETED']:
            raise ValueError('Evaluation Status must be one of [OPEN, PLANNED, CLOSED, COMPLETED]')
        if not kwargs['contentSource'].startswith('syn'):  # Verify that synapse Id given
            raise ValueError('The "contentSource" parameter must be specified as a Synapse Entity when creating an'
                             ' Evaluation')
        super(Evaluation, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation'

    def putURI(self):
        return '/evaluation/%s' % self.id

    def deleteURI(self):
        return '/evaluation/%s' % self.id

    def getACLURI(self):
        return '/evaluation/%s/acl' % self.id

    def putACLURI(self):
        return '/evaluation/acl'


class Submission(DictObject):
    """
    Builds an Synapse submission object.

    :param name:             Name of submission
    :param entityId:         Synapse ID of the Entity to submit
    :param evaluationId:     ID of the Evaluation to which the Entity is to be submitted
    :param versionNumber:    Version number of the submitted Entity
    :param submitterAlias:   A pseudonym or team name for a challenge entry
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s' % id

    def __init__(self, **kwargs):
        if not ('evaluationId' in kwargs and 
                'entityId' in kwargs and
                'versionNumber' in kwargs):
            raise KeyError

        super(Submission, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission?etag=%s' % self.etag

    def putURI(self):
        return '/evaluation/submission/%s' % self.id

    def deleteURI(self):
        return '/evaluation/submission/%s' % self.id


class SubmissionStatus(DictObject):
    """
    Builds an Synapse submission status object.

    :param score:      The score of the submission
    :param status:     Status can be one of {'OPEN', 'CLOSED', 'SCORED', 'INVALID'}.
    """

    @classmethod
    def getURI(cls, id):
        return '/evaluation/submission/%s/status' % id

    def __init__(self, **kwargs):
        super(SubmissionStatus, self).__init__(kwargs)

    def postURI(self):
        return '/evaluation/submission/%s/status' % self.id

    def putURI(self):
        return '/evaluation/submission/%s/status' % self.id

    def deleteURI(self):
        return '/evaluation/submission/%s/status' % self.id

