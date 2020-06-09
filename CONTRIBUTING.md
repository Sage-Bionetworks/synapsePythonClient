# Contributing

Welcome, and thanks for your interest in contributing to the Synapse Python client!

By contributing, you are agreeing that we may redistribute your work under this [license](LICENSE.md).

## I don't want to read this whole thing I just have a question!

> **Note:** Please don't file an issue to ask a question. You'll get faster results by using the resources below.

We have an official forum and a detailed FAQ and where the community and maintainers can chime in with helpful advice if you have questions.

* [Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default)
* [Synapse FAQ](http://docs.synapse.org/articles/faq.html)

## How to contribute

### Reporting bugs or feature requests

Bug reports and feature requests can be made in two ways. The first (preferred) method is by posting a question in the [Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default). The second is by opening an [issue](https://github.com/Sage-Bionetworks/synapsePythonClient/issues) in this repository. In either case, providing enough details for the developers to verify and troubleshoot your issue is paramount:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible. If you are following examples from somewhere (e.g., the [Synapse Docs site](http://docs.synapse.org)) provide a link.
* **Provide specific examples to demonstrate the steps**. Include copy/pasteable snippets. If you are providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**

After a bug report is received, expect a Sage Bionetworks staff member to contact you through the submission method you chose ([Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default)or [Github issue](https://github.com/Sage-Bionetworks/synapsePythonClient/issues)). After ascertaining there is enough detail for the bug report or feature request, a JIRA issue will be opened. If you want to work on fixing the issue or feature yourself, follow the next sections!

### Fixing bugs and improvements

The open work items (bugs and new features) are tracked in JIRA in the [SYNPY Project](https://sagebionetworks.jira.com/projects/SYNPY/issues). Issues marked as `Open` are ready for your contributions!

### Fork and clone this repository

See the [Github docs](https://help.github.com/articles/fork-a-repo/) for how to make a copy (a fork) of a repository to your own Github account.

Then, [clone the repository](https://help.github.com/articles/cloning-a-repository/) to your local machine so you can begin making changes.

Add this repository as an [upstream remote](https://help.github.com/en/articles/configuring-a-remote-for-a-fork) on your local git repository so that you are able to fetch the latest commits.

On your local machine make sure you have the latest version of the `develop` branch:

```
git checkout develop
git pull upstream develop
```

### The development life cycle

1. Pull the latest content from the `develop` branch of this central repository (not your fork).
1. Create a feature branch which off the `develop` branch. The branch should be named the same as the JIRA issue you are working on (e.g., `SYNPY-1234`).
1. After completing work and testing locally (see below), push to your fork.
1. In Github, create a pull request from the feature branch of your fork to the `develop` branch of the central repository.

> *A Sage Bionetworks engineer must review and accept your pull request.* A code review (which happens with both the contributor and the reviewer present) is required for contributing. This can be performed remotely (e.g., Skype, Hangout, or other video or phone conference).

The status of an issue can be tracked in JIRA. Once an issue has passed a code review with a Sage Bionetworks engineer, he/she will update it's status in JIRA appropriately.

### Testing

All code added to the client must have tests. These might include unit tests (to test specific functionality of code that was added to support fixing the bug or feature), integration tests (to test that the feature is usable - e.g., it should have complete the expected behavior as reported in the feature request or bug report), or both.

The Python client uses [`nose`](http://nose.readthedocs.io/) to run tests. The test code is located in the [test](./test) subdirectory.

Here's how to run the test suite:

> *Note:* The entire set of tests takes approximately 20 minutes to run.

```
# Unit tests
nosetests -vs tests/unit

# Integration tests
nosetests -vs tests/integration
```

To test a specific feature, specify the full path to the function to run:

```
# Test table query functionality from the command line client
nosetests -vs tests/integration/test_command_line_client.py:test_table_query
````

### Code style

The Synapse Python Client uses [`flake8`](https://pypi.org/project/flake8/) to enforce 
[`PEP8`](https://legacy.python.org/dev/peps/pep-0008/) style consistency and to check for possible errors.
You can verify your code matches these expectations by running the **flake8** command from the project root directory:

```
# ensure that you have the flake8 package installed
pip install flake8

flake8
```
