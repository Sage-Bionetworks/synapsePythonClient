# Installation

By following the instructions below, you are installing the `synapseclient`, `synapseutils` and the command line client.

## PyPI

The [synapseclient](https://pypi.python.org/pypi/synapseclient/) package is available from PyPI. It can be installed or upgraded with pip. Due to the nature of Python, we highly recommend you set up your python environment with [conda](https://www.anaconda.com/products/distribution) or [pyenv](https://github.com/pyenv/pyenv) and create virtual environments to control your Python dependencies for your work.

- conda: Please follow instructions [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) to manage environments:

```bash
conda create -n synapseclient python=3.9
conda activate synapseclient
(sudo) pip install (--upgrade) synapseclient[pandas, pysftp]
```

- pyenv: Use [virtualenv](https://virtualenv.pypa.io/en/latest/) to manage your python environment:

```bash
pyenv install -v 3.9.13
pyenv global 3.9.13
python -m venv env
source env/bin/activate
(sudo) python3 -m pip3 install (--upgrade) synapseclient[pandas, pysftp]
```

The dependencies on pandas and pysftp are optional. The Synapse `synapseclient.table` feature integrates with Pandas. Support for sftp is required for users of SFTP file storage. Both require native libraries to be compiled or installed separately from prebuilt binaries.

## Local

Source code and development versions are [available on Github](https://github.com/Sage-Bionetworks/synapsePythonClient). Installing from source:

```bash
git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
cd synapsePythonClient
```

You can stay on the master branch to get the latest stable release or check out the develop branch or a tagged revision:

```bash
git checkout <branch or tag>
```

Next, either install the package in the site-packages directory `pip install .` or `pip install -e .` to make the installation follow the head without having to reinstall:

```bash
pip install .
```
