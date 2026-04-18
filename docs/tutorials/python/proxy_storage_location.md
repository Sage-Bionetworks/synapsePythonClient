# Proxy Storage Locations in Synapse

A proxy storage location delegates file access to a proxy server that controls
authentication and access to the underlying storage. Synapse stores only the
metadata; the proxy server handles the actual file retrieval.

This tutorial demonstrates how to create a proxy storage location, register a
file via a `ProxyFileHandle`, and associate it with a Synapse File entity.

[Read more about Custom Storage Locations](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)

## Tutorial Purpose

In this tutorial you will:

1. Set up and get a project
2. Create a proxy storage location and assign it to a folder
3. Register a file by creating a `ProxyFileHandle` via the REST API
4. Associate the `ProxyFileHandle` with a Synapse File entity

## Prerequisites

* Make sure that you have completed the [Installation](../installation.md) and
  [Authentication](../authentication.md) setup.
* You must have a [Project](./project.md) created and replace the one used in
  this tutorial.
* A running proxy server with a shared secret key. See the
  [Synapse Proxy Storage documentation](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)
  for proxy server requirements.

## 1. Set up and get project

```python
--8<-- "docs/tutorials/python/tutorial_scripts/proxy_storage_location.py!lines:setup"
```

## 2. Create a proxy storage location

Create a `StorageLocation` of type `PROXY`, providing your proxy server URL and
the shared secret key. Setting `benefactor_id` to the project or folder ensures that
access control is inherited from the project or folder. Assign it to a folder so that
files uploaded there are served through the proxy.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/proxy_storage_location.py!lines:create_proxy_storage_location"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Created proxy storage location: 12345
  Proxy URL: https://my-proxy-server.example.com
  Benefactor ID: syn123456
```
</details>

## 3. Register a file via ProxyFileHandle

Files in proxy storage are **not** uploaded through the UI or Python client. Instead, you
register a file that already exists on the proxy server by posting a
`ProxyFileHandle` to the Synapse file service. You provide the file's MD5,
size, and the relative path used by the proxy to serve it.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/proxy_storage_location.py!lines:create_proxy_file_handle"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
{"id": ..., "etag":..., ..., "filePath":...}
```
</details>

## 4. Associate the ProxyFileHandle with a File entity

Create a `File` entity using the `data_file_handle_id` returned above. Synapse
stores the metadata and uses the `ProxyFileHandle` to serve downloads through
your proxy server.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/proxy_storage_location.py!lines:associate_proxy_file_handle"
```

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/proxy_storage_location.py!}
```
</details>

## References used in this tutorial

- [StorageLocation][synapseclient.models.StorageLocation]
- [StorageLocationType][synapseclient.models.StorageLocationType]
- [Folder][synapseclient.models.Folder]
- [File][synapseclient.models.File]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [Custom Storage Locations Documentation](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)

## See also

- [Storage Locations Tutorial](./storage_location.md) — How to create and manage all storage location types
- [Storage Location Architecture](../../explanations/storage_location_architecture.md) — In-depth architecture diagrams and design documentation
