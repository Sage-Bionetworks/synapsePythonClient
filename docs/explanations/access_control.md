# Access Control

Access Control Lists (ACLs) in Synapse are the foundation of data security and sharing. They determine who can access your data and what actions they can perform. By default, all entities in Synapse are private to your user account, but they can be easily shared with specific users, groups, or made publicly accessible.

## Core Concepts

### Access Control Lists (ACLs)
An ACL is a set of permissions that define what actions specific users or groups can perform on a Synapse entity. Each ACL entry specifies:

- **Principal**: The user, team, or special group being granted permissions
- **Permissions**: The specific actions they can perform

### Benefactors and Inheritance
Every entity in Synapse has a **benefactor** - the entity from which it inherits its permissions:

- **Default Inheritance**: New entities inherit permissions from their parent container
- **Local ACLs**: When you create custom sharing settings, the entity becomes its own benefactor
- **Permission Resolution**: Synapse traverses up the hierarchy to find the benefactor with ACL settings

### Permission Types
Synapse supports several permission types that can be combined:

- **READ**: View entity metadata and basic information
- **DOWNLOAD**: Download files and data (requires READ)
- **UPDATE**: Modify entity metadata and upload new file versions
- **CREATE**: Create new entities within containers
- **DELETE**: Delete entities
- **CHANGE_PERMISSIONS**: Modify ACL permissions
- **CHANGE_SETTINGS**: Modify entity settings
- **MODERATE**: Moderate forum discussions (for projects)

Other synapse items like `Submission` and `Evaluation` support different permission types not covered in this document.

### Special Principal IDs
Synapse provides special principals for common sharing scenarios:

- **Authenticated users**: All authenticated Synapse users (e.g., 273948)
- **Public group**: Public access (anyone on the internet) (e.g., 273949)
- **Anonymous user**: The principal used for requests that are not logged in (e.g., 273950)
- **Specific User ID**: Individual Synapse users (e.g., #######)
- **Team ID**: Synapse teams for group-based permissions (e.g., #######)

#### Special principal IDs depend on the realm
The IDs of the special principals are not fixed. They depend on the security realm
of the user. The IDs shown on this page are the values for the default realm
(`realmId` 0). If your account belongs to a different realm, these IDs are different.

To look up the special principal IDs for your realm, call the
[GET /realm/principals](https://rest-docs.synapse.org/rest/GET/realm/principals.html)
REST endpoint. A request that is not logged in returns the values for the default
realm:

```bash
curl https://repo-prod.prod.sagebase.org/repo/v1/realm/principals
```

```json
{
  "realmId": "0",
  "anonymousUser": "273950",
  "publicGroup": "273949",
  "authenticatedUsers": "273948",
  "administrativeGroup": "2"
}
```

If you are logged in, the endpoint returns the values for your own realm. The
Python client does not have a dedicated method for this endpoint, but you can call
it with [synapseclient.Synapse.restGET][]:

```python
import synapseclient

syn = synapseclient.login()

realm_principals = syn.restGET("/realm/principals")
public_group_id = realm_principals["publicGroup"]
authenticated_users_id = realm_principals["authenticatedUsers"]
print(realm_principals)
```

## Common Use Cases

### Research Collaboration
- **Internal Teams**: Grant READ/DOWNLOAD to collaborators
- **Data Owners**: Maintain full permissions (including UPDATE/DELETE)
- **Public Data**: Use the public group principal (273949 in the default realm) for open datasets

### Hierarchical Access Control
- **Project Level**: Set broad permissions for the entire project
- **Folder Level**: Override with more restrictive permissions for sensitive data
- **File Level**: Fine-grained control for specific datasets

### Data Governance
- **Sensitive Data**: Create local ACLs with restricted access
- **Compliance**: Use teams for role-based access control
- **Auditing**: Use `list_acl()` to review permission structures

## Best Practices

1. **Use Inheritance**: Leverage the default inheritance model when possible for easier management
2. **Minimal Local ACLs**: Only create custom ACLs when you need different permissions than the parent
3. **Team-Based Permissions**: Use Synapse teams for group permissions rather than individual users
4. **Regular Audits**: Periodically review ACLs using `list_acl()` to ensure appropriate access
5. **Dry Run Testing**: Use `dry_run=True` when deleting permissions to preview changes
6. **Documentation**: Document your permission structure for team understanding

## Security Considerations

- **Principle of Least Privilege**: Grant only the minimum permissions necessary
- **Regular Reviews**: Audit permissions regularly, especially for sensitive data
- **Team Management**: Use teams to simplify permission management and reduce errors
- **Public Access**: Be cautious when granting public access (the public group, 273949 in the default realm) to ensure data is appropriate for public consumption

## Learn More

For hands-on experience with ACL management, follow the comprehensive [Sharing Settings Tutorial](../tutorials/python/sharing_settings.md), which demonstrates all aspects of permission management using real examples.

## API References

### Object-Oriented Models
- [get_permissions()][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_permissions]
- [get_acl()][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_acl]
- [set_permissions()][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.set_permissions]
- [list_acl()][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.list_acl]
- [delete_permissions()][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.delete_permissions]

### Legacy API Methods
- [synapseclient.Synapse.getPermissions][]
- [synapseclient.Synapse.setPermissions][]
