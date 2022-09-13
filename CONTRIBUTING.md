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
* **Describe the exact steps which reproduce the problem** in as many details as possible. If you are following examples from somewhere (e.g., the [Synapse Docs site](https://help.synapse.org/docs/index.html)) provide a link.
* **Provide specific examples to demonstrate the steps**. Include copy/pasteable snippets. If you are providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**

After a bug report is received, expect a Sage Bionetworks staff member to contact you through the submission method you chose ([Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default)or [Github issue](https://github.com/Sage-Bionetworks/synapsePythonClient/issues)). After ascertaining there is enough detail for the bug report or feature request, a JIRA issue will be opened. If you want to work on fixing the issue or feature yourself, follow the next sections!

### The development life cycle

Developing on the Python client starts with picking a issue to work on in JIRA! The open work items (bugs and new features) are tracked in JIRA in the [SYNPY Project](https://sagebionetworks.jira.com/projects/SYNPY/issues). Issues marked as `Open` are ready for your contributions! Please make sure your assign yourself the ticket and check with the maintainers if the issue you picked is suitable.

#### Fork and clone this repository

1. See the [Github docs](https://help.github.com/articles/fork-a-repo/) for how to make a copy (a fork) of a repository to your own Github account.
1. [Clone the repository](https://help.github.com/articles/cloning-a-repository/) to your local machine so you can begin making changes.
1. Add this repository as an [upstream remote](https://help.github.com/en/articles/configuring-a-remote-for-a-fork) on your local git repository so that you are able to fetch the latest commits.  ("Fetch the latest commits" means you are able to update your forked repository with the latest changes from the original repository)
1. On your local machine make sure you have the latest version of the `develop` branch:

    ```
    git remote add upstream https://github.com/Sage-Bionetworks/synapsePythonClient.git
    git checkout develop
    git pull upstream develop
    ```

#### Development

Now that you have chosen a JIRA ticket and have your own fork of this repository.  It's time to start development!

> **Note**
>
> To ensure the most fluid development, try not to push to your develop or main branch.


1. (Assuming you have followed all 4 steps above in the "fork and clone this repository" section). Navigate to your cloned repository on your computer/server.
1. Make sure your `develop` branch is up to date with the `Sage-Bionetworks/synapsePythonClient` develop branch

    ```
    cd synapsePythonClient
    git remote add upstream https://github.com/Sage-Bionetworks/synapsePythonClient.git
    git checkout develop
    git pull upstream develop
    ```

1. Create a feature branch which off the `develop` branch. The branch should be named the same as the JIRA issue you are working on (e.g., `SYNPY-1234-{feature-here}`). I recommend adding details of the actual feature in the branch name so that you don't need to go back and forth between JIRA and GitHub.

    ```
    git checkout develop
    git checkout -b SYNPY-1234-{feature-here}
    ```

1. At this point, you have only created the branch locally, you need to push this to your fork on GitHub.

    ```
    git push --set-upstream origin SYNPY-1234-{feature-here}
    ```

1. You should now be able to see the branch on GitHub. Make commits as you deem necessary. It helps to provide useful commit messages - a commit message saying 'Update' is a lot less helpful than saying 'Remove X parameter because it was unused'.  Try to avoid using `git commit -am` unless you know for a fact that those commits are grouped.

    > **Note**
    >
    > It is good practice to run `git diff` or `git status` to first view your changes prior to pushing!

    ```
    git commit changed_file.txt -m "Remove X parameter because it was unused"
    git push
    ```

1. (Make sure you have follow instructions in "Install development dependencies") Once you have made your additions or changes, make sure you write tests and run the test suite. More information on testing below.
1. Once you have completed all the steps above, in Github, create a pull request from the feature branch of your fork to the `develop` branch of `Sage-Bionetworks/synapsePythonClient`.

    > **Note**
    >
    > *A code maintainer must review and accept your pull request. A code review ideally happens with both the contributor and the reviewer present, but is not strictly required for contributing. This can be performed remotely (e.g., Zoom, Hangout, or other video or phone conference).
    >
    > The status of an issue can be tracked in JIRA. Once an issue has passed a code review with a Sage Bionetworks engineer, he/she will update it's status in JIRA appropriately.

#### Testing

All code added to the client must have tests. These might include unit tests (to test specific functionality of code that was added to support fixing the bug or feature), integration tests (to test that the feature is usable - e.g., it should have complete the expected behavior as reported in the feature request or bug report), or both.

The Python client uses [`pytest`](https://docs.pytest.org/en/latest/) to run tests. The test code is located in the [test](./test) subdirectory.

Here's how to run the test suite:

> *Note:* The entire set of tests takes approximately 20 minutes to run.

```
# Unit tests
pytest -vs tests/unit

# Integration tests
pytest -vs tests/integration
```

To test a specific feature, specify the full path to the function to run:

```
# Test table query functionality from the command line client
pytest -vs tests/integration/synapseclient/test_command_line_client.py::test_table_query
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

### Repository Admins

- [Release process](https://sagebionetworks.jira.com/wiki/spaces/SYNPY/pages/643498030/Synapse+Python+Client+Staging+Validation+Production)
