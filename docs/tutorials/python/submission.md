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
* You have completed the [Evaluation](./evaluation.md) tutorial, or have an existing Evaluation on Synapse to work from
* You have an existing entity with which to make a submission (can be a [File](./file.md) or Docker Repository)
* You have the correct permissions on the Evaluation queue for your desired tutorial section (participant or organizer)

## 1. Participating in a Synapse challenge

### 1. Make a submission to an existing evaluation queue on Synapse

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=32-54}
```

### 2. Fetch your existing submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=56-71}
```

### 3. Count your submissions

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=72-88}
```

### 4. Fetch all of your submissions from an existing evaluation queue on Synapse

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=90-101}
```

### 5. Check the status of your submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=103-125}
```

### 6. Cancel your submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=126-143}
```

## 2. Organizing a Synapse challenge

### 1. Annotate a submission to score it

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=33-60}
```

### 2. Batch-update submission statuses

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=62-99}
```

### 3. Fetch the submission bundle for a given submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=101-136}
```

### 4. Allow cancellation of submissions

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=138-177}
```

### 5. Delete submissions

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=179-209}
```

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me (source code for Participant)</summary>

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!}
```
</details>

<details class="quote">
  <summary>Click to show me (source code for Organizer)</summary>

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!}
```
</details>

## References
- [Evaluation][synapseclient.models.Evaluation]
- [File][synapseclient.models.File]
- [Submission][synapseclient.models.Submission]
- [SubmissionStatus][synapseclient.models.SubmissionStatus]
- [SubmissionBundle][synapseclient.models.SubmissionBundle]
- [syn.login][synapseclient.Synapse.login]
