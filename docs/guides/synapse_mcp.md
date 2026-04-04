# Using the Synapse MCP Server

The [Synapse MCP server](https://github.com/Sage-Bionetworks/synapse-mcp) implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) and lets AI assistants (Claude, GitHub Copilot, Cursor, and others) directly query Synapse — search for datasets, inspect entity metadata, explore project hierarchies, and trace provenance — without you writing any code.

!!! warning "Terms of Service"
    Using the Synapse MCP server with consumer AI services that store conversation data may violate the Synapse Terms of Service prohibition on data redistribution. Prefer enterprise deployments with data-residency guarantees or self-hosted models when working with sensitive or restricted datasets.

---

## Installation

### Remote server (recommended)

The hosted MCP server at `https://mcp.synapse.org/mcp` authenticates via OAuth2 — no token management required.

=== "Claude Code (CLI)"

    ```bash
    claude mcp add --transport http synapse -- https://mcp.synapse.org/mcp
    ```

    On first use, Claude Code will open a browser window to complete the OAuth2 login.

=== "Claude Desktop"

    1. Open **Settings → Connectors → Add custom connector**
    2. Enter the URL: `https://mcp.synapse.org/mcp`
    3. Save and restart Claude Desktop

=== "VS Code / GitHub Copilot"

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

Once connected, your AI assistant gains access to the following tools:

| Tool | What it does |
|------|-------------|
| `search_synapse` | Full-text search across all public (and your private) entities — filter by name, entity type, and parent |
| `get_entity` | Fetch core metadata for any entity by Synapse ID (projects, folders, files, tables, etc.) |
| `get_entity_annotations` | Retrieve the custom annotation key/value pairs attached to an entity |
| `get_entity_children` | List all children within a project or folder |
| `get_entity_provenance` | Inspect the activity log and inputs/outputs for a specific entity version |

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

Have an idea for a new MCP tool or want to report a bug? Use one of the channels below:

- **Request a feature or report a bug** — [Open a support ticket](https://sagebionetworks.jira.com/servicedesk/customer/portal/9/group/16/create/206) via the Sage Bionetworks service desk
- **GitHub issues** — [synapse-mcp/issues](https://github.com/Sage-Bionetworks/synapse-mcp/issues) for technical bugs and pull requests
- **Discussion forum** — [Synapse Help Forum](https://www.synapse.org/#!SynapseForum:default) for general questions
