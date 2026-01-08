# Submissions, SubmissionStatuses, SubmissionBundles
## What are Submissions?

In Synapse, a Submission is your entry to a challenge or evaluation queue. When you participate in a computational challenge or collaborative project, you submit your work (such as predictions, models, or analysis results) as a Submission to be evaluated and scored.

## Key Concepts

Before working with Submissions, it's helpful to understand how they fit into Synapse:

- **Entity**: Your actual work stored in Synapse (like a File containing predictions or a Docker image with your model)
- **Evaluation**: A queue that accepts and organizes submissions for a specific challenge or project
- **Submission**: The object associated with submitting your Entity to an Evaluation queue, creating a record that can be tracked and scored

## How Submissions Work

When you submit an Entity to an Evaluation:

- The Submission creates an immutable record linking your Entity to that Evaluation
- The Evaluation owns this Submission record (not you as the submitter)
- Organizers can add scores and feedback through a SubmissionStatus object
- You can track your Submissions and view their statuses

## Related Objects

The Python client provides three object types for working with Submissions:

- **Submission**: Represents your entry in an Evaluation queue
- **SubmissionStatus**: Tracks scoring information and feedback for a Submission
- **SubmissionBundle**: Combines a Submission and its SubmissionStatus for convenient access

## What You'll Learn

This tutorial covers two perspectives:

1. **Participating in challenges**: Making and tracking your submissions
1. **Organizing challenges**: Scoring and managing submissions from participants

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


Script setup:

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=13-30}
```

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
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=72-82}
```

### 4. Fetch all of your submissions from an existing evaluation queue on Synapse

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=82-95}
```

### 5. Check the status of your submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=97-119}
```

### 6. Cancel your submission

```python
{!docs/tutorials/python/tutorial_scripts/submission_participant.py!lines=120-137}
```

## 2. Organizing a Synapse challenge


Script setup:

```python
{!docs/tutorials/python/tutorial_scripts/submission_organizer.py!lines=12-31}
```

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
