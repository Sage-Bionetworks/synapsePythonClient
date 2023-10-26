# Contributing

Welcome, and thanks for your interest in contributing to the Synapse Python client!

By contributing, you are agreeing that we may redistribute your work under this [license](LICENSE.md).

# Table of contents
- [How to contribute](#how-to-contribute)
   * [Reporting bugs or feature requests](#reporting-bugs-or-feature-requests)
- [Getting started](#getting-started)
    + [Fork and clone this repository](#fork-and-clone-this-repository)
    + [Installing the Python Client in a virtual environment with pipenv](#installing-the-python-client-in-a-virtual-environment-with-pipenv)
    + [Set up pre-commit](#set-up-pre-commit)
    + [Authentication](#authentication)
- [The development life cycle](#the-development-life-cycle)
   * [Development](#development)
   * [Testing](#testing)
      + [Integration testing agaisnt the `dev` synapse server](#integration-testing-agaisnt-the-dev-synapse-server)
   * [Code style](#code-style)
   * [Repository Admins](#repository-admins)

## I don't want to read this whole thing I just have a question!

> **Note:** Please don't file an issue to ask a question. You'll get faster results by using the resources below.

We have an official forum and a detailed FAQ and where the community and maintainers can chime in with helpful advice if you have questions.

* [Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default)
* [Synapse FAQ](https://help.synapse.org/docs/FAQ.2047967233.html)

## How to contribute

### Reporting bugs or feature requests

Bug reports and feature requests can be made in two ways. The first (preferred) method is by posting a question in the [Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default). The second is by opening an [issue](https://github.com/Sage-Bionetworks/synapsePythonClient/issues) in this repository. In either case, providing enough details for the developers to verify and troubleshoot your issue is paramount:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible. If you are following examples from somewhere (e.g., the [Synapse Docs site](https://help.synapse.org/docs/index.html)) provide a link.
* **Provide specific examples to demonstrate the steps**. Include copy/pasteable snippets. If you are providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**

After a bug report is received, expect a Sage Bionetworks staff member to contact you through the submission method you chose ([Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default)or [Github issue](https://github.com/Sage-Bionetworks/synapsePythonClient/issues)). After ascertaining there is enough detail for the bug report or feature request, a JIRA issue will be opened. If you want to work on fixing the issue or feature yourself, follow the next sections!

## Getting started
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

#### Installing the Python Client in a virtual environment with pipenv
Perform the following one-time steps to set up your local environment.

1. This package uses Python, if you have not already, please install [pyenv](https://github.com/pyenv/pyenv#installation) to manage your Python versions. Versions supported by this package are all versions >=3.8 and <=3.11. If you do not install `pyenv` make sure that Python and `pip` are installed correctly and have been added to your PATH by running `python3 --version` and `pip3 --version`. If your installation was successful, your terminal will return the versions of Python and `pip` that you installed.  **Note**: If you have `pyenv` it will install a specific version of Python for you.

2. Install `pipenv` by running `pip install pipenv`.
    - If you already have `pipenv` installed, ensure that the version is >=2023.9.8 to avoid compatibility issues.

3. Install `synapseclient` locally using pipenv:

    * pipenv
      ```bash
      # Verify you are at the root directory for the cloned repository (ie: `cd synapsePythonClient`)
      # To develop locally you want to add --dev
      pipenv install --dev
      # Set your active session to the virtual environment you created
      pipenv shell
      # Note: The 'Python Environment Manager' extension in vscode is reccomended here
      ```

4. Once completed you are ready to start developing. Commands run through the CLI, or through an IDE like visual studio code within the virtual environment will have all required dependencies automatically installed. Try running `synapse -h` in your shell to read over the available CLI commands. Or view the `Usage as a library` section in the README.md to get started using the library to write more python.

#### Set up pre-commit
Once your virtual environment is created the `pre-commit` command will be available through your terminal. Install `pre-commit` into your git hooks via:
```
pre-commit install
```
When you commit your files via git it will automatically use the `.pre-commit-config.yaml` to run various checks to enforce style.

If you want to manually run all pre-commit hooks use:
```
pre-commit run --all-files
```

#### Authentication
Learn about the multiple ways one can login to Synapse [here](https://python-docs.synapse.org/build/html/getting_started/credentials.html).

## The development life cycle

Developing on the Python client starts with picking a issue to work on in JIRA! The open work items (bugs and new features) are tracked in JIRA in the [SYNPY Project](https://sagebionetworks.jira.com/projects/SYNPY/issues). Issues marked as `Open` are ready for your contributions! Please make sure your assign yourself the ticket and check with the maintainers if the issue you picked is suitable.

### Development

Now that you have chosen a JIRA ticket and have your own fork of this repository.  It's time to start development!

> **Note**
>
> To ensure the most fluid development, try not to push to your develop or main branch.


1. (Assuming you have followed all 4 steps above in the "fork and clone this repository" section). Navigate to your cloned repository on your computer/server.

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
    # Make sure that you have setup `pre-commit` as noted in the getting started section
    git commit changed_file.txt -m "Remove X parameter because it was unused"
    git push
    ```

1. Once you have made your additions or changes, make sure you write tests and run the test suite. More information on testing below.
1. Once you have completed all the steps above, in Github, create a pull request from the feature branch of your fork to the `develop` branch of `Sage-Bionetworks/synapsePythonClient`.

    > **Note**
    >
    > *A code maintainer must review and accept your pull request. A code review ideally happens with both the contributor and the reviewer present, but is not strictly required for contributing. This can be performed remotely (e.g., Zoom, Hangout, or other video or phone conference).
    >
    > The status of an issue can be tracked in JIRA. Once an issue has passed a code review with a Sage Bionetworks engineer, he/she will update it's status in JIRA appropriately.

### Testing

All code added to the client must have tests. These might include unit tests (to test specific functionality of code that was added to support fixing the bug or feature), integration tests (to test that the feature is usable - e.g., it should have complete the expected behavior as reported in the feature request or bug report), or both.

The Python client uses [`pytest`](https://docs.pytest.org/en/latest/) to run tests. The test code is located in the [test](./test) subdirectory.

Here's how to run the test suite:

> *Note:* The entire set of tests takes approximately 20 minutes to run.

```
# Unit tests
pytest -vs tests/unit

# Integration tests - The integration tests should be run against the `dev` synapse server
pytest -vs tests/integration
```

To test a specific feature, specify the full path to the function to run:

```
# Test table query functionality from the command line client
pytest -vs tests/integration/synapseclient/test_command_line_client.py::test_table_query
````

#### Integration testing against the `dev` synapse server
The easiest way to specify the HTTP endpoints to use for all synapse requests is to modify the `~/.synapseConfig` file and modify a few key-value pairs such as below. Not this is also where you will specify the dev authentication:
```
[authentication]
username=<dev username>
authtoken=<dev authtoken>

[endpoints]
repoEndpoint=https://repo-dev.dev.sagebase.org/repo/v1
authEndpoint=https://repo-dev.dev.sagebase.org/auth/v1
fileHandleEndpoint=https://repo-dev.dev.sagebase.org/file/v1
```

#### Integration testing for external collaborators
As an external collaborator you will not have access to a development account and environment to run the integration tests agaisnt. Either request that a Sage Bionetworks staff member run your integration tests via a pull request, or, contact us via the [Help Forum](https://www.synapse.org/#!SynapseForum:default) to requisition a development account for integration testing only.

### Code style

The Synapse Python Client uses [`flake8`](https://pypi.org/project/flake8/) to enforce
[`PEP8`](https://legacy.python.org/dev/peps/pep-0008/) style consistency and to check for possible errors.
You can verify your code matches these expectations by running the **flake8** command from the project root directory:

```
# ensure that you have the flake8 package installed
# Note: This is not required if using the pipenv virtual environment
pip install flake8

flake8
```

### Repository Admins

- [Release process](https://sagebionetworks.jira.com/wiki/spaces/SYNPY/pages/643498030/Synapse+Python+Client+Staging+Validation+Production)
