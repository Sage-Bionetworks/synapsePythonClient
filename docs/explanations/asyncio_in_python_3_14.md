# Asyncio and Python 3.14+ Changes

## Overview

Starting with Python 3.14, changes to the asyncio implementation affect how the Synapse Python Client works in Jupyter notebooks and other async contexts. This document explains what changed, why, and how it impacts your code.

**TL;DR:** Python 3.14+ breaks the `nest_asyncio` workaround. In Jupyter notebooks, you must now use async methods with `await` (e.g., `await obj.get_async()` instead of `obj.get()`). Regular scripts are unaffected.

## Background: How Jupyter Notebooks Run Code

Jupyter notebooks run all code within an active asyncio event loop. This means:

- **Async functions** must be called with `await my_async_function()`
- **Synchronous functions** can be called normally with `my_function()`
- **You cannot use** `asyncio.run()` because asyncio intentionally prevents nested event loops (to avoid threading and deadlock issues)

## What Changed in Python 3.14

### Previous Behavior (Python 3.13 and earlier)

The Synapse Python Client used a library called `nest_asyncio` to work around asyncio's nested event loop restriction. This allowed us to:

1. Detect whether code was running in a notebook (with an active event loop) or a regular script
2. Automatically call async functions from synchronous wrapper functions
3. Provide a seamless synchronous API that worked in both environments

**This meant you could write:**
```python
from synapseclient import Synapse
from synapseclient.models import Project

syn = Synapse()
syn.login()

# This worked in notebooks AND regular scripts
my_project = Project(name="My Project").get()
```

### New Behavior (Python 3.14+)

Python 3.14 changed the asyncio implementation in ways that break `nest_asyncio`. The library can no longer safely monkey-patch asyncio.

**Impact:** The automatic async-to-sync conversion no longer works in notebooks on Python 3.14+.

> **Why the change?** Python 3.14 improved asyncio's safety to prevent deadlocks and race conditions. The `nest_asyncio` workaround bypassed these safety mechanisms and now causes failures in HTTP connection pooling. Rather than risk silent failures, the library raises clear errors when async methods should be used.

## How This Affects Your Code

### In Regular Python Scripts

**No changes needed.** Synchronous methods continue to work as before:

```python
from synapseclient import Synapse
from synapseclient.models import Project

syn = Synapse()
syn.login()

# This still works in regular scripts
my_project = Project(name="My Project").get()
```

### In Jupyter Notebooks (Python 3.14+)

**You must use async methods directly** with `await`:

```python
from synapseclient import Synapse
from synapseclient.models import Project

syn = Synapse()
syn.login()

# Use the async version with await
my_project = await Project(name="My Project").get_async()
```

### In Other Async Contexts

If you're calling Synapse Python Client methods from within an async function, use the async methods:

```python
import asyncio
from synapseclient import Synapse
from synapseclient.models import Project

syn = Synapse()
syn.login()

async def main():
    # Use async methods with await
    my_project = await Project(name="My Project").get_async()

asyncio.run(main())
```

## Error Messages

If you try to use synchronous methods in an async context on Python 3.14+, you'll see an error like:

```
RuntimeError: Python 3.14+ detected an active event loop, which prevents automatic async-to-sync conversion.
This is a limitation of asyncio in Python 3.14+.

To resolve this, use the async method directly:
  • Instead of: result = obj.method_name()
  • Use: result = await obj.method_name_async()

For Jupyter/IPython notebooks: You can use 'await' directly in cells.
For other async contexts: Ensure you're in an async function and use 'await'.
```

## Quick Reference

| Environment | Python 3.13 and earlier | Python 3.14+ |
|-------------|------------------------|--------------|
| Regular Python script | `obj.method()` | `obj.method()` ✓ |
| Jupyter notebook | `obj.method()` | `await obj.method_async()` |
| Inside async function | `await obj.method_async()` | `await obj.method_async()` |

## Finding Async Methods

For most synchronous methods, there is a corresponding async version with `_async` suffix:

- `get()` → `get_async()`
- `store()` → `store_async()`
- `delete()` → `delete_async()`

Check the API documentation for the complete list of async methods available.
