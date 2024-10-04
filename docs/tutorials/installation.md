# Installation

By following the instructions below, you are installing the `synapseclient`, `synapseutils` and the command line client.

## TL;DR For Experts
1. Set up your Python development environment in your preferred manner (e.g. with `conda`, `pyenv`, etc).
2. Run
```
pip install --upgrade synapseclient
```
3. Verify your installation
```
pip show synapseclient
```

## Installation Guide For: PyPI Users

The [synapseclient](https://pypi.python.org/pypi/synapseclient/) package is available from PyPI. It can be installed or upgraded with pip. Due to the nature of Python, we highly recommend you set up your python environment with [conda](https://www.anaconda.com/products/distribution) or [pyenv](https://github.com/pyenv/pyenv) and create virtual environments to control your Python dependencies for your work.

- conda: Please follow instructions [here](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) to manage environments:

```bash
conda create -n synapseclient python=3.9
conda activate synapseclient

# Here are a few ways to install the client. Choose the one that fits your use-case
# sudo may optionally be needed depending on your setup

pip install --upgrade synapseclient
pip install --upgrade "synapseclient[pandas]"
pip install --upgrade "synapseclient[pandas, pysftp, boto3]"
```

> **NOTE** <br>
> The `synapseclient` package may require loading shared libraries located in your system's `/usr/local/lib` directory. Some users working
with `conda` have experienced issues with shared libraries not being found due to the system searching in the wrong locations. Although
not recommended, one solution for this is manually configuring the `LD_LIBRARY_PATH` environment variable to point
to the `/usr/local/lib` directory. [See here](https://github.com/conda/conda/issues/12800) for more context on this solution, and for alternatives.

----

- pyenv: Use [virtualenv](https://virtualenv.pypa.io/en/latest/) to manage your python environment:

```bash
pyenv install -v 3.9.13
pyenv global 3.9.13
python -m venv env
source env/bin/activate

# Here are a few ways to install the client. Choose the one that fits your use-case
# sudo may optionally be needed depending on your setup

python -m pip install --upgrade synapseclient
python -m pip install --upgrade "synapseclient[pandas]"
python -m pip install --upgrade "synapseclient[pandas, pysftp, boto3]"

python3 -m pip3 install --upgrade synapseclient
python3 -m pip3 install --upgrade "synapseclient[pandas]"
python3 -m pip3 install --upgrade "synapseclient[pandas, pysftp, boto3]"
```

The dependencies on pandas, pysftp, and boto3 are optional. The Synapse `synapseclient.table` feature integrates with Pandas. Support for sftp is required for users of SFTP file storage. Both require native libraries to be compiled or installed separately from prebuilt binaries.

## Installation Guide For: Git Users

Source code and development versions are [available on Github](https://github.com/Sage-Bionetworks/synapsePythonClient). Installing from source:

```bash
git clone https://github.com/Sage-Bionetworks/synapsePythonClient.git
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
