# Submissions, SubmissionStatuses, SubmissionBundles
Users can work with Submissions on the python client, since these objects are part of the Evaluation API data model. A user in a Synapse Evaluation can submit a Synapse Entity as a Submission to that Evaluation. Submission data is owned by the parent Evaluation, and is immutable.

The data model includes additional objects to support scoring of Submissions and convenient data access:

- **SubmissionStatus**: An object used to track scoring information for a single Submission. This object is intended to be modified by the users (or test harnesses) managing the Evaluation.
- **SubmissionBundle**: A convenience object to transport a Submission and its accompanying SubmissionStatus in a single web service call.

This tutorial will demonstrate how to work with all 3 object types using the python client for 2 different use-cases:

1. Participating in a Synapse challenge
1. Organizing a Synapse challenge


## Tutorial Purpose
In this tutorial:

As a participant of a Synapse challenge, you will

1. Make a submission to an existing evaluation queue on Synapse
1. Fetch your existing submission
1. Count your submissions
1. Fetch all of your submissions from an existing evaluation queue on Synapse
1. Check the status of your submission
1. Cancel your submission

As an organizer of a Synapse challenge, you will

1. Annotate a submission to score it
1. Batch-update submission statuses
1. Fetch the submission bundle for a given submission
1. Allow cancellation of submissions
1. Delete submissions

## Prerequisites
*

## 1.
