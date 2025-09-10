# Evaluations
An Evaluation is essentially a container that organizes and manages the submission, assessment, and scoring of data, models, or other research artifacts.
It allows teams to set up challenges where participants contribute their work, and those contributions can be systematically reviewed or scored.

This tutorial will walk you through the basics of working with Virtual Tables
using the Synapse Python client.

## Tutorial Purpose
In this tutorial you will:

1. Create and update an Evaluation on Synapse
1. Update the ACL of an Evaluation on Synapse
1. Retrieve and delete all Evaluations from a given Project

## Prerequisites
* Make sure that you have completed the [Project](./project.md) tutorial, or have an existing Project on Synapse to work from

## 1. Create and update an Evaluation on Synapse

In this first part, we'll be showing you how to interact with an Evaluation object as well as introducing you to its two core functionalities `store()` and `get()`.

```python
{!docs/tutorials/python/tutorial_scripts/evaluation.py!lines=5-46}
```

## 2. Update the ACL of an Evaluation on Synapse

Like Synapse entities, Evaluations have ACLs that can be used to control who has access to your evaluations and what level of access they have. Working with the ACLs of an Evaluation object is slightly different, as its not an attribute of the model, rather its a dictionary you'll need to modify separately. Let's see an example of how this looks:

```python
{!docs/tutorials/python/tutorial_scripts/evaluation.py!lines=48-55}
```

## 3. Retrieve and delete all Evaluations from a given Project

Now we will show how you can retrieve lists of Evaluation objects, rather than retrieving them one-by-one with `get()`. This is a powerful tool if you want to perform the same action on all the evaluations in a given project, for example, like what we're about to do here:

```python
{!docs/tutorials/python/tutorial_scripts/evaluation.py!lines=57-63}
```

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/file.py!}
```
</details>

## References
- [Evaluation][synapseclient.models.Evaluation]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
