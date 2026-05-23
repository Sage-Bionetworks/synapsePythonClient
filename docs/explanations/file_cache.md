# File Cache Mechanism

This document explains how the Synapse Python Client caches files on the local
filesystem. The cache exists to avoid repeatedly downloading (or re-uploading)
large or numerous files that are already available locally, and to share a
consistent on-disk layout across Synapse clients written in different
languages.

The cache is part of the client's internal infrastructure
([`synapseclient.core.cache`][]) — end users do not normally interact with it
directly. Understanding it is, however, useful when:

- diagnosing unexpected re-downloads or skipped downloads,
- choosing an `ifcollision` mode for [`syn.get`][synapseclient.Synapse.get] or
  [`File.get_async`][synapseclient.models.File.get_async],
- changing the cache root location, or
- writing tools that share or migrate a Synapse cache directory.

---

## On This Page

- **[Motivation](#motivation)** — why the cache exists and what problem it solves
- **[On-disk layout](#on-disk-layout)** — where the cache lives and how it is organised
- **[The cache map](#the-cache-map)** — what the `.cacheMap` file contains
- **[Cache operations](#cache-operations)** — store and get behaviour by case
- **[Collision handling](#collision-handling)** — how `ifcollision` decides what to do
- **[File locking](#file-locking)** — how concurrent clients coordinate
- **[Configuration](#configuration)** — how to change the cache location

---

## Motivation

Synapse tracks the *canonical* location of a file (for example in Amazon S3)
together with the file's MD5 hash, but it does not know where any particular
user has downloaded the file on their local machine. A client therefore needs
its own mechanism to remember:

- which files it has downloaded,
- where each downloaded copy lives on disk,
- whether each on-disk copy is still up to date.

The cache also serves the inverse case: when a `File` object is re-uploaded,
the client must decide whether the local file actually changed since the last
upload, or whether the upload can be skipped.

To answer these questions the client maintains a per-file-handle **Cache
Map**: a JSON document whose keys are local file paths and whose values record
the last-modified timestamp (and, in the current implementation, the MD5) of
the file at the time it was cached.

---

## On-disk layout

By default the cache lives at `~/.synapseCache`. The path can be overridden in
the `.synapseConfig` file — see [Configuration](#configuration).

Each cached file is stored at:

```
CACHE_ROOT/[Intermediate Folder]/[File Handle ID]/[File Name]
```

where:

| Component | Description |
|-----------|-------------|
| `CACHE_ROOT` | Configurable root directory. Defaults to `~/.synapseCache`. |
| `[Intermediate Folder]` | `File Handle ID` mod 1000. Reduces fan-out so no single directory contains more than ~1000 entries when the cache holds many files. |
| `[File Handle ID]` | The Synapse FileHandle ID used to upload/download the file. Each version of a file has its own FileHandle, and therefore its own cache directory. |
| `[File Name]` | The file name from the FileHandle. If `ifcollision="keep.both"` produces a clash, the name is suffixed with a number, e.g. `file.txt` → `file(1).txt`. |

For example, file handle `1234567` would live at:

```
~/.synapseCache/567/1234567/genotypedata.csv
```

The intermediate folder `567` comes from `1234567 % 1000`.

Alongside the cached file in `[File Handle ID]/` sits a `.cacheMap` file
recording every known on-disk location for that file handle.

---

## The cache map

A `.cacheMap` file is JSON whose keys are absolute, normalised file paths and
whose values describe what was true about that file when it was last cached:

```json
{
  "/path/to/file.txt": {
    "modified_time": "2026-03-14T15:09:26.000Z",
    "content_md5": "9e107d9d372bb6826bd81d3542a419d6"
  },
  "/alt/folder/file.txt": {
    "modified_time": "2026-04-06T15:36:41.000Z",
    "content_md5": "9e107d9d372bb6826bd81d3542a419d6"
  }
}
```

A few invariants enforced by [`cache.py`](#) are worth calling out:

- **Timestamps are ISO-8601 in UTC** (`%Y-%m-%dT%H:%M:%S.000Z`). The client
  always writes `.000` milliseconds so that a Cache Map written by one client
  compares correctly against the integer-second `mtime` reported by another.
- **Path separators are `/`** regardless of platform. Windows paths are
  normalised before being written into the map so the file is portable across
  systems sharing the same cache directory.
- **Multiple entries per file handle are allowed.** A single FileHandle ID may
  legitimately have several on-disk copies — for example because the user
  downloaded it to different `download_location`s, or because
  `ifcollision="keep.both"` produced a renamed copy. The map records all of
  them.
- **Legacy entries are read transparently.** Older clients wrote the
  modified-time string directly as the value (no surrounding object). The
  current client reads both shapes; new writes always use the object form
  with `modified_time` and `content_md5` keys.

A file is treated as a valid cache hit only when *both* the on-disk `mtime`
matches the cached `modified_time` *and* (if present) the cached
`content_md5` matches the MD5 of the file on disk. Either check failing —
or the file being missing — invalidates the entry.

---

## Cache operations

### Storing a file (upload)

| Case | What happens |
|------|--------------|
| A new file is uploaded to Synapse for the first time. | The file is uploaded; a new Cache Map entry is added under the new FileHandle ID, keyed by the local path. |
| A `File` object that has already been uploaded is stored again, with the same local path. | The Cache Map entry for that path is consulted. If `modified_time` and `content_md5` still match the file on disk, **no upload occurs**. Otherwise, the file is re-uploaded, generating a new FileHandle ID, and a new Cache Map entry is created under that new FileHandle. The old entry is left untouched, because other in-memory `File` objects may still reference the previous on-disk copy. |

### Getting a file (download)

| Case | What happens |
|------|--------------|
| The file has not been downloaded locally. | The file is downloaded to the chosen location (or the default cache location if none was specified), and an entry is added to the Cache Map for that FileHandle. |
| The file has been downloaded before, but to a *different* target location. | The Cache Map is consulted for the FileHandle ID. If any existing on-disk copy is still unmodified, it is **copied** to the new target location and a new Cache Map entry is added for that new path. Otherwise the file is downloaded fresh. The new `File` object does **not** silently re-point at the original cached copy — having multiple in-memory `File` objects mutating one on-disk file would cause surprising behaviour. |
| The file has been downloaded before to the *same* target location. | The Cache Map entry for that exact path is consulted. If `modified_time` (and `content_md5`, when present) still match, no download occurs. If the file is missing on disk, it is re-downloaded. If the file is present but modified, behaviour depends on `ifcollision` — see below. |

---

## Collision handling

When `get()` is asked to download to a path that already contains a
*modified* (i.e. cache-invalid) local file, the `ifcollision` parameter
chooses the behaviour:

| Mode | Behaviour |
|------|-----------|
| `"overwrite.local"` | The file is downloaded to the target path, overwriting the local copy. The Cache Map entry is updated with the new `modified_time` and `content_md5`. |
| `"keep.local"` | No download occurs. The returned `File` references the locally-modified file at the given location. This is how an interrupted edit-and-upload workflow can resume without losing local changes (see [Example: lose session after editing](#example-lose-session-after-editing)). |
| `"keep.both"` | The file is downloaded to the target directory under a modified name (e.g. `file.txt` → `file(1).txt`), and a second entry for the same FileHandle ID is added to the Cache Map. |

---

## File locking

Multiple processes (and even multiple clients in different languages) may
share a single cache directory. To prevent torn writes of `.cacheMap`, every
read or write of the cache map must be performed while holding a lock on it.

The locking protocol is intentionally simple and language-agnostic, so that
the R client, the Python client, and any third-party tooling can interoperate
without relying on platform-specific file-locking APIs:

1. To lock `<filename>`, a client creates an empty directory named
   `<filename>.lock` in the file's parent folder. Directory creation is
   atomic and mutually exclusive on every supported platform, which gives the
   `mkdir` call the semantics of a mutex.
2. A successful lock holder has exclusive access for **10 seconds** from the
   time the lock directory was created. After that, any other client is
   free to delete the lock directory and proceed — the original holder is
   then obligated to stop touching the file unless it re-acquires the lock.
   The age cap prevents a crashed process from leaving a permanent stale
   lock.
3. A client that cannot acquire the lock within **70 seconds** raises an
   appropriate exception rather than blocking indefinitely.
4. When the client finishes its critical section it removes the lock
   directory.

This is implemented in [`synapseclient.core.lock.Lock`][] and applied around
every read and write of `.cacheMap` in [`synapseclient.core.cache.Cache`][].

---

## Configuration

The cache location is read from the `[cache]` section of the
`.synapseConfig` file:

```ini
[cache]
location = ~/.synapseCache
```

`~` and environment variables are expanded. If the directory does not exist
it is created the first time the cache is initialised. See the
[configuration tutorial](../tutorials/configuration.md#cache) for the full
list of `.synapseConfig` options.

---

## Worked examples

### Example: cache hit avoids a re-download

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()

# First call: downloads from Synapse, populates the cache.
file = syn.get("syn12345")

# Second call (same session or a later one): the .cacheMap entry still
# matches the on-disk file's mtime and MD5, so no network download occurs.
file_again = syn.get("syn12345")
```

### Example: lose session after editing

This is the canonical motivation for `ifcollision="keep.local"`. A user
downloads a script, edits it locally, loses their session, and then wants to
re-upload the edited copy without first overwriting it with the server's
version:

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()

# Initial download — cache entry created.
code = syn.get("syn12345")

# ... user edits the file on disk, session ends ...

# New session. The on-disk file is now modified relative to the cache entry.
# "keep.local" tells get() not to clobber the user's edits.
code = syn.get("syn12345", ifCollision="keep.local")

# store() compares the on-disk file to the cache and sees that it has
# changed, so it uploads the edited copy as a new version.
syn.store(code)
```

### Example: downloading the same file to two locations

```python
from synapseclient import Synapse

syn = Synapse()
syn.login()

# First download goes to the cache directory.
file = syn.get("syn12345")
# /Users/me/.synapseCache/567/1234567/genotypedata.csv

# A second download requests a different target directory. Because the
# original on-disk copy is still unmodified, the client copies it locally
# rather than re-downloading. Both paths are recorded in the .cacheMap
# under FileHandle 1234567.
file_copy = syn.get("syn12345", downloadLocation="~/scratch/")
# /Users/me/scratch/genotypedata.csv
```

---

## See also

- [Configuration tutorial](../tutorials/configuration.md#cache) — how to set
  the cache location in `.synapseConfig`
- [`synapseclient.core.cache.Cache`][] — internal API for the cache
- [`synapseclient.Synapse.get`][] / [`synapseclient.models.File.get_async`][]
  — the public download entry points that consult the cache
