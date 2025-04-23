# SubmissionView

SubmissionViews in Synapse allow you to aggregate and query submissions from one or more
evaluation queues in a tabular format. These views are useful for managing submissions in
challenges, tracking scoring progress, and analyzing submission data across different
evaluation queues.

This tutorial will walk you through the basics of working with SubmissionViews
using the Synapse Python client.

## Tutorial Purpose
In this tutorial, you will:

1. Set up and create an evaluation queue
2. Create a SubmissionView for the evaluation queue
3. Create and submit a file to the evaluation queue
4. Query and update the submission status
5. Modify the SubmissionView scope
6. Create a snapshot of the view
7. Query the snapshot

## Prerequisites
* This tutorial assumes that you have a Synapse project.
* Pandas must also be installed as shown in the [installation documentation](../installation.md).

## 1. Set up and create an evaluation queue

Before creating a SubmissionView, we need to log in to Synapse, retrieve your project,
and create an evaluation queue that will be used in the view.

You will want to replace `"My uniquely named project about Alzheimer's Disease"` with
the name of your project.

The name of the Evaluation must also be globally unique. Please update
`"Test Evaluation Queue for Alzheimer conference"` with a new value.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=13-44}
```

## 2. Create a SubmissionView for the evaluation queue

Next, we will create a SubmissionView that includes the evaluation queue we just created
in its scope. We'll also add custom columns for metrics that will be used for scoring
submissions.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=46-82}
```

## 3. Create and submit a file to the evaluation queue

Now let's create a test file and submit it to our evaluation queue. For convenience,
we'll use a temporary file that will be automatically cleaned up after execution.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=84-105}
```

## 4. Query and update the submission status

After submitting a file, we can query the SubmissionView to see our submission and update
its status with scoring metrics.

Note: Due to Synapse's eventual consistency model, we need to wait briefly for the
submission to appear in the view.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=107-126}
```

<details class="example">
  <summary>The result of querying your SubmissionView should include your submission with its details:</summary>
```
Query results:
    ROW_ID  ROW_VERSION                              ROW_ETAG  metric_A  metric_B  ... submitteralias     entityid  entityversion  dockerrepositoryname  dockerdigest
0  9751779            0  e7c37ec7-e5e8-435d-b378-dc2d6bddad21       NaN       NaN  ...  Participant 1  syn66272627              1                   NaN           NaN
```

After updating the submission status:

```
Submission status: SCORED
```
</details>

## 5. Modify the SubmissionView scope

As your challenge evolves, you might need to add more evaluation queues to your SubmissionView.
Here's how to create another evaluation queue and add it to your view's scope.

The name of the Evaluation must also be globally unique. Please update
`"Second Test Evaluation Queue for Alzheimer conference"` with a new value.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=128-143}
```

## 6. Create a snapshot of the view

SubmissionViews support creating snapshots, which capture the state of all submissions at a
specific point in time. This is useful for archiving or comparing submission states.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=145-152}
```

## 7. Query the snapshot

After creating a snapshot, you can query it to retrieve the state of submissions at the
time the snapshot was created. This is useful for historical analysis or auditing.

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!lines=153-162}
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/submissionview.py!}
```
</details>

## References
- [SubmissionView][synapseclient.models.SubmissionView]
- [Evaluation][synapseclient.evaluation]
- [syn.submit][synapseclient.Synapse.submit]
- [syn.getSubmissionStatus][synapseclient.Synapse.getSubmissionStatus]
- [syn.getSubmission][synapseclient.Synapse.getSubmission]
- [syn.getSubmissions][synapseclient.Synapse.getSubmissions]
- [syn.getSubmissionBundles][synapseclient.Synapse.getSubmissionBundles]
- [syn.store][synapseclient.Synapse.store]
- [Column][synapseclient.models.Column]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html)
