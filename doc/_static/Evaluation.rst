******************
Synapse Evaluation
******************

An evaluation object represents a collection of Synapse Entities that will be
processed in a particular way.  This could mean scoring Entries in a challenge
or executing a processing pipeline.

Evaluations can be retrieved by ID::

    evaluation = syn.getEvaluation(1901877)

Entities may be submitted for evaluation. 
The :py:func:`synapseclient.Synapse.submit` method returns a
:py:class:`synapseclient.evaluation.Submission` object::

    entity = syn.get(synapse_id)
    submission = syn.submit(evaluation, entity)

The Submission has a :py:class:`synapseclient.evaluation.SubmissionStatus`::

    status = syn.getSubmissionStatus(submission)

See:
- :py:func:`synapseclient.Synapse.getEvaluation`
- :py:func:`synapseclient.Synapse.submit`
- :py:func:`synapseclient.Synapse.addEvaluationParticipant`
- :py:func:`synapseclient.Synapse.getSubmissions`
- :py:func:`synapseclient.Synapse.getSubmission`
- :py:func:`synapseclient.Synapse.getSubmissionStatus`
    
.. autoclass:: synapseclient.evaluation.Evaluation
   :members:
   
~~~~~~~~~~
Submission
~~~~~~~~~~

.. autoclass:: synapseclient.evaluation.Submission
   :members:
   
~~~~~~~~~~~~~~~~~
Submission Status
~~~~~~~~~~~~~~~~~

.. autoclass:: synapseclient.evaluation.SubmissionStatus
   :members: