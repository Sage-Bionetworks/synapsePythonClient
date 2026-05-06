# Using the Synapse MCP Server

The [Synapse MCP server](https://github.com/Sage-Bionetworks/synapse-mcp) implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) and lets AI assistants (Claude, GitHub Copilot, Cursor, and others) directly query Synapse — search for datasets, inspect entity metadata, explore project hierarchies, and trace provenance — without you writing any code.

The server is implemented in Python and built on top of this `synapseclient` package, so its behavior and capabilities mirror what you can do programmatically through the Python client.

!!! warning "Terms of Service"
    Using the Synapse MCP server with consumer AI services that store conversation data may violate the Synapse Terms of Service prohibition on data redistribution. Prefer enterprise deployments with data-residency guarantees or self-hosted models when working with sensitive or restricted datasets.

---

## Installation

### Remote server (recommended)

The hosted MCP server at `https://mcp.synapse.org/mcp` authenticates via OAuth2 — no token management required.

#### "Claude Code (CLI)"

    ```bash
    claude mcp add --transport http synapse -- https://mcp.synapse.org/mcp
    ```

    On first use, Claude Code will open a browser window to complete the OAuth2 login.

#### "Claude Desktop"

    1. Open **Settings → Connectors → Add custom connector**
    2. Enter the URL: `https://mcp.synapse.org/mcp`
    3. Save and restart Claude Desktop

#### "VS Code / GitHub Copilot"

    Add to your `settings.json` or `.vscode/mcp.json`:

    ```json
    {
      "mcp": {
        "servers": {
          "synapse": {
            "type": "http",
            "url": "https://mcp.synapse.org/mcp"
          }
        }
      }
    }
    ```

### Local installation

For air-gapped environments or development, you can run the server locally using a [Personal Access Token (PAT)](https://www.synapse.org/#!PersonalAccessTokens:0).

```bash
git clone https://github.com/Sage-Bionetworks/synapse-mcp.git
cd synapse-mcp
pip install -e .
export SYNAPSE_PAT="your_personal_access_token"
synapse-mcp
```

Configure your MCP client to point to `http://localhost:8000/mcp` (or the port shown in the startup output).

---

## Available tools

For the full and up-to-date list of tools, see the [synapse-mcp repository](https://github.com/Sage-Bionetworks/synapse-mcp). At the time of writing, the server exposes tools including:

- `search_synapse` — full-text search across public and private entities
- `get_entity` — fetch core metadata for any entity by Synapse ID
- `get_entity_annotations` — retrieve custom annotation key/value pairs
- `get_entity_children` — list children within a project or folder
- `get_entity_provenance` — inspect the activity log and inputs/outputs for an entity version

---

## Example prompts

Once the MCP server is connected, you can interact with Synapse in natural language. Here are some useful prompts to try:

**Discover data**

```
Search Synapse for RNA-seq datasets related to Alzheimer's Disease.
```

```
What files are in the project syn12345678?
```

**Inspect metadata**

```
What are the annotations on syn9876543?
```

```
Show me the provenance for the latest version of syn11223344.
```

**Explore a project**

```
List all folders and files in syn5678901 and summarize what the project contains.
```

**Combine with code generation**

```
Find the Synapse ID for the ROSMAP bulk RNA-seq dataset, then write Python code
using synapseclient to download it and load it into a pandas DataFrame.
```

---

## Feature requests and feedback

Have an idea for a new MCP tool or want to report a bug? [Open a support ticket](https://sagebionetworks.jira.com/servicedesk/customer/portal/9/group/16/create/206) via the Sage Bionetworks service desk.
