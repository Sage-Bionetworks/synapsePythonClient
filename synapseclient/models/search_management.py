"""Search management dataclasses.

These dataclasses model the org-level search management resources used by
SearchIndex entities (TextAnalyzer, ColumnAnalyzerOverride, SynonymSet,
SearchConfiguration, SearchConfigBinding) and the query/response types for
querying a SearchIndex's OpenSearch index.

The query model is a thin pass-through over the OpenSearch ``_search`` request
body: ``SearchQuery`` carries the allowlisted top-level keys (``query``,
``post_filter``, ``aggregations``, ``highlight``, ``collapse``, ``rescore``,
``sort``, ``_source``, ``from``, ``size``, ``search_after``). Each slot is typed
against the OpenSearch query DSL by the ``TypedDict`` shapes in
`synapseclient.models.search_dsl` -- start at
[Query][synapseclient.models.search_dsl.Query]. The analyzer-resource
``settings`` / ``definition`` / ``analyzer`` slots are raw OpenSearch JSON
objects; the Synapse API does not constrain them further.

Each analyzer resource belongs to an Organization and is referenced by qualified
name (``{organizationName}-{name}``). Resources are publicly readable;
create/update operations are restricted to Sage Bionetworks employees.

REST controller: <https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.SearchManagementController>
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient.api import (
    bind_search_config_to_entity,
    clear_search_config_binding,
    create_column_analyzer_override,
    create_search_configuration,
    create_synonym_set,
    create_text_analyzer,
    get_column_analyzer_override,
    get_search_config_binding,
    get_search_configuration,
    get_synonym_set,
    get_text_analyzer,
    list_column_analyzer_overrides,
    list_search_configurations,
    list_synonym_sets,
    list_text_analyzers,
    update_column_analyzer_override,
    update_search_configuration,
    update_synonym_set,
    update_text_analyzer,
)
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.mixins.enum_coercion import ForwardCompatibleStrEnum
from synapseclient.models.protocols.search_management_protocol import (
    SearchConfigBindingSynchronousProtocol,
)
from synapseclient.models.search_dsl import (
    Aggregation,
    AnalyzerRef,
    FieldCollapse,
    Highlight,
    Query,
    Rescore,
    ScalarValue,
    SourceFilter,
)
from synapseclient.models.table_components import SelectColumn

if TYPE_CHECKING:
    from synapseclient import Synapse


class SearchIndexState(ForwardCompatibleStrEnum):
    """The state of a SearchIndex's OpenSearch index. A state Synapse returns
    that is not declared here is preserved as-is rather than raising."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"


class SearchQueryPart(str, Enum):
    """Optional response parts for a SearchQuery beyond default HITS.

    These are values for the `responseParts` field on a SearchIndexQuery.
    """

    HITS = "HITS"
    TOTAL_HITS = "TOTAL_HITS"
    SELECT_COLUMNS = "SELECT_COLUMNS"


class OrgScopedResourceProtocol(Protocol):
    """Synchronous interface shared by the org-scoped search-management resources
    (TextAnalyzer, ColumnAnalyzerOverride, SynonymSet, SearchConfiguration)."""

    def store(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Create this resource, or update it if it already has an ID."""
        return self

    def get(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Fetch this resource from Synapse by its ID."""
        return self

    def list(
        self,
        organization_name: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["Self"]:
        """List resources of this type, optionally filtered by organization."""
        return []


@async_to_sync
class OrgScopedResource(OrgScopedResourceProtocol):
    """Base implementing the create/get/update/list lifecycle shared by the
    org-scoped search-management resources.

    Subclasses set the ``_CREATE_FN`` / ``_GET_FN`` / ``_UPDATE_FN`` / ``_LIST_FN``
    class attributes to the matching ``api.search_services`` functions and implement
    ``fill_from_dict`` / ``to_synapse_request``. These resources have no delete
    endpoint; ``name`` and ``organizationName`` are immutable after creation.
    """

    _CREATE_FN: Callable = None
    _GET_FN: Callable = None
    _UPDATE_FN: Callable = None
    _LIST_FN: Callable = None

    async def store_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Self":
        """Create this resource, or update it if it already has an ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated with the server-assigned ID, etag, and timestamps.
        """
        cls = type(self)
        if self.id:
            result = await cls._UPDATE_FN(
                self.id, self.to_synapse_request(), synapse_client=synapse_client
            )
        else:
            result = await cls._CREATE_FN(
                self.to_synapse_request(), synapse_client=synapse_client
            )
        return self.fill_from_dict(result)

    async def get_async(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Fetch this resource from Synapse by its ID.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the Synapse response.

        Raises:
            ValueError: If the ``id`` attribute has not been set.
        """
        if not self.id:
            raise ValueError(f"{type(self).__name__} must have an id set to call get.")
        cls = type(self)
        result = await cls._GET_FN(self.id, synapse_client=synapse_client)
        return self.fill_from_dict(result)

    @classmethod
    async def list_async(
        cls,
        organization_name: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["Self"]:
        """List resources of this type, paginating over all pages.

        Arguments:
            organization_name: If provided, only resources in this organization are
                returned; otherwise resources across all organizations are listed.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of every matching resource across all result pages.
        """
        results: List["Self"] = []
        next_page_token = None
        while True:
            page = await cls._LIST_FN(
                organization_name=organization_name,
                next_page_token=next_page_token,
                synapse_client=synapse_client,
            )
            for item in page.get("results", []) or []:
                results.append(cls().fill_from_dict(item))
            next_page_token = page.get("nextPageToken", None)
            if not next_page_token:
                break
        return results


@dataclass
class TextAnalyzer(OrgScopedResource):
    """A shareable, named OpenSearch custom analyzer. Used to configure how text
    is tokenized for a search index.

    A TextAnalyzer belongs to an Organization, referenced by `organization_name`.
    Find an Organization you already have access to with
    `synapseclient.models.organization.list_organizations()`, or create one with
    `synapseclient.models.Organization` before creating a TextAnalyzer.

    Represents a [Synapse TextAnalyzer](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/TextAnalyzer.html).

    Note: TextAnalyzer has no delete endpoint on the Synapse REST API. Once
    created, it cannot be removed, and its owning Organization can no longer be
    deleted either. Choose `organization_name` and `name` deliberately.

    Example: Create a TextAnalyzer.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import TextAnalyzer

        syn = Synapse()
        syn.login()

        analyzer = TextAnalyzer(
            organization_name="my.existing.organization",
            name="stemmed_english",
            description="English analyzer with stemming and lowercase filtering",
            settings={
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "porter_stem"],
                    }
                }
            },
        )
        analyzer = analyzer.store()
        print(f"Created TextAnalyzer: {analyzer.id} ({analyzer.qualified_name})")
        ```

    Example: Get an existing TextAnalyzer by ID.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import TextAnalyzer

        syn = Synapse()
        syn.login()

        analyzer = TextAnalyzer(id="12345").get()
        print(analyzer.name, analyzer.settings)
        ```

    Example: Update an existing TextAnalyzer.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import TextAnalyzer

        syn = Synapse()
        syn.login()

        analyzer = TextAnalyzer(id="12345").get()
        analyzer.description = "Updated description"
        analyzer.settings["analyzer"]["default"]["filter"].append("asciifolding")
        analyzer = analyzer.store()
        print(f"Updated TextAnalyzer etag: {analyzer.etag}")
        ```

    Example: List TextAnalyzers in an Organization.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import TextAnalyzer

        syn = Synapse()
        syn.login()

        analyzers = TextAnalyzer.list(organization_name="my.existing.organization")
        for analyzer in analyzers:
            print(analyzer.id, analyzer.qualified_name)
        ```
    """

    _CREATE_FN = staticmethod(create_text_analyzer)
    _GET_FN = staticmethod(get_text_analyzer)
    _UPDATE_FN = staticmethod(update_text_analyzer)
    _LIST_FN = staticmethod(list_text_analyzers)

    id: Optional[str] = None
    """The unique immutable ID for this text analyzer."""

    organization_name: Optional[str] = None
    """The name of the organization that owns this analyzer.
    Immutable after creation."""

    name: Optional[str] = None
    """The name of this analyzer. Must start with a letter and
    contain only letters, digits, and underscores; unique within the
    organization."""

    description: Optional[str] = None
    """Optional description of this analyzer."""

    settings: Optional[Dict[str, Any]] = None
    """Required. JSON object holding the *contents of* the `settings.analysis`
    block of an OpenSearch
    [create-index](https://docs.opensearch.org/latest/api-reference/index-apis/create-index/)
    request body. Allowed root keys are `char_filter`, `tokenizer`, `filter`,
    and `analyzer`; the inner `analyzer` map must declare exactly one `default`
    entry (and optionally `default_search`). A `{"$ref": "{org}-{name}"}` entry
    inside the `filter` registry resolves to a SynonymSet at index-build time.
    Carried as a raw JSON object -- the Synapse API does not constrain it
    further. See [analyzers](https://docs.opensearch.org/latest/analyzers/)
    for the available tokenizers, token filters, and character filters."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. The eTag changes every time this analyzer is updated;
    it is used to detect when a client's copy is out-of-date."""

    created_on: Optional[str] = None
    """The date on which this analyzer was created."""

    created_by: Optional[str] = None
    """The ID of the Synapse user who created this analyzer."""

    modified_on: Optional[str] = None
    """The date on which this analyzer was last modified."""

    modified_by: Optional[str] = None
    """The ID of the Synapse user who last modified this analyzer."""

    @property
    def qualified_name(self) -> Optional[str]:
        """The qualified name '{organizationName}-{name}' used to reference
        this analyzer from a SearchConfiguration."""
        if self.organization_name and self.name:
            return f"{self.organization_name}-{self.name}"
        return None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.id = data.get("id", None)
        self.organization_name = data.get("organizationName", None)
        self.name = data.get("name", None)
        self.description = data.get("description", None)
        self.settings = data.get("settings", None)
        self.etag = data.get("etag", None)
        self.created_on = data.get("createdOn", None)
        self.created_by = data.get("createdBy", None)
        self.modified_on = data.get("modifiedOn", None)
        self.modified_by = data.get("modifiedBy", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "id": self.id,
            "organizationName": self.organization_name,
            "name": self.name,
            "description": self.description,
            "settings": self.settings,
            "etag": self.etag,
        }
        delete_none_keys(body)
        return body


@dataclass
class ColumnAnalyzerOverrideEntry:
    """Assigns one TextAnalyzer to one column.

    Represents a [Synapse ColumnAnalyzerOverrideEntry](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/ColumnAnalyzerOverrideEntry.html).
    """

    column_name: Optional[str] = None
    """The name of the column to override. Silently skipped at index-build time if the column is not present in the target SearchIndex's schema — a single override bundle can therefore be applied across several indexes that share some column names"""

    analyzer: Optional[Union[AnalyzerRef, Dict[str, Any]]] = None
    """The analyzer to use for this column. Either a reference to a saved
    TextAnalyzer written as `{"$ref": "{organizationName}-{name}"}` (see
    [AnalyzerRef][synapseclient.models.search_dsl.AnalyzerRef]), or an inline
    OpenSearch [`settings.analysis`](https://docs.opensearch.org/latest/analyzers/)
    block."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.column_name = data.get("columnName", None)
        self.analyzer = data.get("analyzer", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "columnName": self.column_name,
            "analyzer": self.analyzer,
        }
        delete_none_keys(body)
        return body


@dataclass
class ColumnAnalyzerOverride(OrgScopedResource):
    """A shareable bundle of per-column analyzer assignments. Each entry binds one column to an analyzer;

    A ColumnAnalyzerOverride belongs to an Organization, referenced by
    `organization_name`. Find an Organization you already have access to with
    `synapseclient.models.organization.list_organizations()`, or create one with
    `synapseclient.models.Organization` before creating a ColumnAnalyzerOverride.

    Represents a [Synapse ColumnAnalyzerOverride](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/ColumnAnalyzerOverride.html).

    Note: ColumnAnalyzerOverride has no delete endpoint on the Synapse REST API.
    Once created, it cannot be removed, and its owning Organization can no
    longer be deleted either. Choose `organization_name` and `name` deliberately.

    Example: Create a ColumnAnalyzerOverride.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride, ColumnAnalyzerOverrideEntry

        syn = Synapse()
        syn.login()

        override = ColumnAnalyzerOverride(
            organization_name="my.existing.organization",
            name="disease_column_overrides",
            description="Use a keyword analyzer for the disease_code column",
            overrides=[
                ColumnAnalyzerOverrideEntry(
                    column_name="disease_code",
                    analyzer={"analyzer": {"default": {"type": "keyword"}}},
                ),
            ],
        )
        override = override.store()
        print(f"Created ColumnAnalyzerOverride: {override.id} ({override.qualified_name})")
        ```

    Example: Get an existing ColumnAnalyzerOverride by ID.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride

        syn = Synapse()
        syn.login()

        override = ColumnAnalyzerOverride(id="12345").get()
        print(override.name, override.overrides)
        ```

    Example: Update an existing ColumnAnalyzerOverride.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride, ColumnAnalyzerOverrideEntry

        syn = Synapse()
        syn.login()

        override = ColumnAnalyzerOverride(id="12345").get()
        override.overrides.append(
            ColumnAnalyzerOverrideEntry(
                column_name="title",
                analyzer={"analyzer": {"default": {"type": "standard"}}},
            )
        )
        override = override.store()
        print(f"Updated ColumnAnalyzerOverride etag: {override.etag}")
        ```

    Example: List ColumnAnalyzerOverrides in an Organization.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import ColumnAnalyzerOverride

        syn = Synapse()
        syn.login()

        overrides = ColumnAnalyzerOverride.list(organization_name="my.existing.organization")
        for override in overrides:
            print(override.id, override.qualified_name)
        ```
    """

    _CREATE_FN = staticmethod(create_column_analyzer_override)
    _GET_FN = staticmethod(get_column_analyzer_override)
    _UPDATE_FN = staticmethod(update_column_analyzer_override)
    _LIST_FN = staticmethod(list_column_analyzer_overrides)

    id: Optional[str] = None
    """The unique ID of this column analyzer override."""

    organization_name: Optional[str] = None
    """The name of the Organization this resource belongs to. Immutable after
    creation."""

    name: Optional[str] = None
    """The resource name. Must start with a letter and contain only letters,
    digits, and underscores. Unique within the organization and immutable
    after creation. Used as part of the qualified name
    ({organizationName}-{name}) when referenced by other resources."""

    description: Optional[str] = None
    """Optional description."""

    overrides: Optional[List[ColumnAnalyzerOverrideEntry]] = field(default_factory=list)
    """The per-column analyzer assignments -- see ColumnAnalyzerOverrideEntry."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme."""

    created_on: Optional[str] = None
    """The date this resource was created."""

    created_by: Optional[str] = None
    """The ID of the user that created this resource."""

    modified_on: Optional[str] = None
    """The date this resource was last modified."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this resource."""

    @property
    def qualified_name(self) -> Optional[str]:
        if self.organization_name and self.name:
            return f"{self.organization_name}-{self.name}"
        return None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.id = data.get("id", None)
        self.organization_name = data.get("organizationName", None)
        self.name = data.get("name", None)
        self.description = data.get("description", None)
        self.overrides = [
            ColumnAnalyzerOverrideEntry().fill_from_dict(o)
            for o in data.get("overrides", []) or []
        ]
        self.etag = data.get("etag", None)
        self.created_on = data.get("createdOn", None)
        self.created_by = data.get("createdBy", None)
        self.modified_on = data.get("modifiedOn", None)
        self.modified_by = data.get("modifiedBy", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "id": self.id,
            "organizationName": self.organization_name,
            "name": self.name,
            "description": self.description,
            "overrides": (
                [o.to_synapse_request() for o in self.overrides]
                if self.overrides
                else None
            ),
            "etag": self.etag,
        }
        delete_none_keys(body)
        return body


@dataclass
class SynonymSet(OrgScopedResource):
    """A shareable OpenSearch synonym_graph (or legacy synonym) token filter.
    Referenced by qualified name `{organizationName}-{name}` from a TextAnalyzer's
    `settings.filter` registry map via `{"$ref": "{organizationName}-{name}"}`.

    A SynonymSet belongs to an Organization, referenced by `organization_name`.
    Find an Organization you already have access to with
    `synapseclient.models.organization.list_organizations()`, or create one with
    `synapseclient.models.Organization` before creating a SynonymSet.

    Represents a [Synapse SynonymSet](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SynonymSet.html).

    Note: SynonymSet has no delete endpoint on the Synapse REST API. Once
    created, it cannot be removed, and its owning Organization can no longer be
    deleted either. Choose `organization_name` and `name` deliberately.

    Example: Create a SynonymSet.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SynonymSet

        syn = Synapse()
        syn.login()

        synonyms = SynonymSet(
            organization_name="my.existing.organization",
            name="disease_synonyms",
            description="Common synonyms for disease terms",
            definition={
                "type": "synonym_graph",
                "synonyms": [
                    # comma-separated = equivalence set, matches either way
                    "tumor, neoplasm, cancer",
                    # "=>" = one-way mapping, "AD" expands to the right side only
                    "AD => Alzheimer's disease",
                ],
            },
        )
        synonyms = synonyms.store()
        print(f"Created SynonymSet: {synonyms.id} ({synonyms.qualified_name})")
        ```

    Example: Get an existing SynonymSet by ID.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SynonymSet

        syn = Synapse()
        syn.login()

        synonyms = SynonymSet(id="12345").get()
        print(synonyms.name, synonyms.definition)
        ```

    Example: Update an existing SynonymSet.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SynonymSet

        syn = Synapse()
        syn.login()

        synonyms = SynonymSet(id="12345").get()
        synonyms.definition["synonyms"].append("MI, myocardial infarction, heart attack")
        synonyms = synonyms.store()
        print(f"Updated SynonymSet etag: {synonyms.etag}")
        ```

    Example: List SynonymSets in an Organization.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SynonymSet

        syn = Synapse()
        syn.login()

        synonym_sets = SynonymSet.list(organization_name="my.existing.organization")
        for synonyms in synonym_sets:
            print(synonyms.id, synonyms.qualified_name)
        ```
    """

    _CREATE_FN = staticmethod(create_synonym_set)
    _GET_FN = staticmethod(get_synonym_set)
    _UPDATE_FN = staticmethod(update_synonym_set)
    _LIST_FN = staticmethod(list_synonym_sets)

    id: Optional[str] = None
    """The unique ID of this synonym set."""

    organization_name: Optional[str] = None
    """The name of the Organization this resource belongs to. Immutable after
    creation."""

    name: Optional[str] = None
    """The resource name. Must start with a letter and contain only letters,
    digits, and underscores. Unique within the organization and immutable
    after creation. Used as part of the qualified name
    ({organizationName}-{name}) when referenced by other resources."""

    description: Optional[str] = None
    """Optional description of the synonym set."""

    definition: Optional[Dict[str, Any]] = None
    """Required. The full OpenSearch token filter definition as a JSON object,
    exactly as documented for the
    [`synonym_graph`](https://docs.opensearch.org/latest/analyzers/token-filters/synonym-graph/)
    / [`synonym`](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/)
    token filters, e.g. `{"type": "synonym_graph", "synonyms":
    ["tumor, neoplasm, cancer", "AD => Alzheimer's disease"]}`. Carried as a raw
    JSON object -- the Synapse API does not constrain it further."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme."""

    created_on: Optional[str] = None
    """The date this resource was created."""

    created_by: Optional[str] = None
    """The ID of the user that created this resource."""

    modified_on: Optional[str] = None
    """The date this resource was last modified."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this resource."""

    @property
    def qualified_name(self) -> Optional[str]:
        if self.organization_name and self.name:
            return f"{self.organization_name}-{self.name}"
        return None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.id = data.get("id", None)
        self.organization_name = data.get("organizationName", None)
        self.name = data.get("name", None)
        self.description = data.get("description", None)
        self.definition = data.get("definition", None)
        self.etag = data.get("etag", None)
        self.created_on = data.get("createdOn", None)
        self.created_by = data.get("createdBy", None)
        self.modified_on = data.get("modifiedOn", None)
        self.modified_by = data.get("modifiedBy", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "id": self.id,
            "organizationName": self.organization_name,
            "name": self.name,
            "description": self.description,
            "definition": self.definition,
            "etag": self.etag,
        }
        delete_none_keys(body)
        return body


@dataclass
class SearchConfiguration(OrgScopedResource):
    """Bundles the index-wide default analyzer and per-column overrides used to
    build a SearchIndex.

    A SearchConfiguration belongs to an Organization, referenced by
    `organization_name`. Find an Organization you already have access to with
    `synapseclient.models.organization.list_organizations()`, or create one with
    `synapseclient.models.Organization` before creating a SearchConfiguration.

    Represents a [Synapse SearchConfiguration](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchConfiguration.html).

    Note: SearchConfiguration has no delete endpoint on the Synapse REST API.
    Once created, it cannot be removed, and its owning Organization can no
    longer be deleted either. Choose `organization_name` and `name` deliberately.

    Example: Create a SearchConfiguration.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchConfiguration

        syn = Synapse()
        syn.login()

        config = SearchConfiguration(
            organization_name="my.existing.organization",
            name="default_config",
            description="Default search configuration for this project's indexes",
            # Reference a previously-created TextAnalyzer by its qualified name...
            default_analyzer={"$ref": "my.existing.organization-stemmed_english"},
        )
        config = config.store()
        print(f"Created SearchConfiguration: {config.id} ({config.qualified_name})")
        ```

    Example: Get an existing SearchConfiguration by ID.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchConfiguration

        syn = Synapse()
        syn.login()

        config = SearchConfiguration(id="12345").get()
        print(config.name, config.default_analyzer)
        ```

    Example: Update an existing SearchConfiguration.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchConfiguration

        syn = Synapse()
        syn.login()

        config = SearchConfiguration(id="12345").get()
        config.column_analyzer_overrides.append(
            {"$ref": "my.existing.organization-abstract_overrides"}
        )
        config = config.store()
        print(f"Updated SearchConfiguration etag: {config.etag}")
        ```

    Example: List SearchConfigurations in an Organization.
        &nbsp;

        ```python
        from synapseclient import Synapse
        from synapseclient.models import SearchConfiguration

        syn = Synapse()
        syn.login()

        configs = SearchConfiguration.list(organization_name="my.existing.organization")
        for config in configs:
            print(config.id, config.qualified_name)
        ```
    """

    _CREATE_FN = staticmethod(create_search_configuration)
    _GET_FN = staticmethod(get_search_configuration)
    _UPDATE_FN = staticmethod(update_search_configuration)
    _LIST_FN = staticmethod(list_search_configurations)

    id: Optional[str] = None
    """The unique ID of this search configuration."""

    organization_name: Optional[str] = None
    """The name of the Organization this resource belongs to. Immutable after
    creation."""

    name: Optional[str] = None
    """The resource name. Must start with a letter and contain only letters,
    digits, and underscores. Unique within the organization and immutable
    after creation. Used as part of the qualified name
    ({organizationName}-{name}) when referenced by other resources."""

    description: Optional[str] = None
    """Optional description."""

    default_analyzer: Optional[Union[AnalyzerRef, Dict[str, Any]]] = None
    """Optional. The analyzer that supplies this index's `analysis.analyzer.default`
    slot. Either a reference to a saved TextAnalyzer written as
    `{"$ref": "{organizationName}-{name}"}` (see
    [AnalyzerRef][synapseclient.models.search_dsl.AnalyzerRef]), or an inline
    OpenSearch [`settings.analysis`](https://docs.opensearch.org/latest/analyzers/)
    block."""
    column_analyzer_overrides: Optional[List[Union[AnalyzerRef, Dict[str, Any]]]] = (
        field(default_factory=list)
    )
    """Optional ordered list of ColumnAnalyzerOverride entries. Each entry is
    either a reference `{"$ref": "{organizationName}-{name}"}` (see
    [AnalyzerRef][synapseclient.models.search_dsl.AnalyzerRef]) or an inline
    ColumnAnalyzerOverride literal."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme."""

    created_on: Optional[str] = None
    """The date this resource was created."""

    created_by: Optional[str] = None
    """The ID of the user that created this resource."""

    modified_on: Optional[str] = None
    """The date this resource was last modified."""

    modified_by: Optional[str] = None
    """The ID of the user that last modified this resource."""

    @property
    def qualified_name(self) -> Optional[str]:
        if self.organization_name and self.name:
            return f"{self.organization_name}-{self.name}"
        return None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.id = data.get("id", None)
        self.organization_name = data.get("organizationName", None)
        self.name = data.get("name", None)
        self.description = data.get("description", None)
        self.default_analyzer = data.get("defaultAnalyzer", None)
        self.column_analyzer_overrides = data.get("columnAnalyzerOverrides", []) or []
        self.etag = data.get("etag", None)
        self.created_on = data.get("createdOn", None)
        self.created_by = data.get("createdBy", None)
        self.modified_on = data.get("modifiedOn", None)
        self.modified_by = data.get("modifiedBy", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "id": self.id,
            "organizationName": self.organization_name,
            "name": self.name,
            "description": self.description,
            "defaultAnalyzer": self.default_analyzer,
            "columnAnalyzerOverrides": self.column_analyzer_overrides or None,
            "etag": self.etag,
        }
        delete_none_keys(body)
        return body


@dataclass
@async_to_sync
class SearchConfigBinding(SearchConfigBindingSynchronousProtocol):
    """Attaches a SearchConfiguration to an entity. When a SearchIndex is built,
    the effective SearchConfiguration is resolved by walking up the entity
    hierarchy (entity -> folder -> project) and using the first binding found.

    Represents a [Synapse SearchConfigBinding](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchConfigBinding.html).
    """

    bind_id: Optional[str] = None
    """The unique ID of this binding."""

    search_configuration_id: Optional[str] = None
    """The ID of the SearchConfiguration bound to this entity."""

    object_id: Optional[str] = None
    """The ID of the entity this configuration is bound to."""

    object_type: Optional[str] = None
    """The type of the object this configuration is bound to."""

    created_by: Optional[str] = None
    """The ID of the user that created this binding."""

    created_on: Optional[str] = None
    """The date this binding was created."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.bind_id = data.get("bindId", None)
        self.search_configuration_id = data.get("searchConfigurationId", None)
        self.object_id = data.get("objectId", None)
        self.object_type = data.get("objectType", None)
        self.created_by = data.get("createdBy", None)
        self.created_on = data.get("createdOn", None)
        return self

    async def store_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Self":
        """Bind ``search_configuration_id`` to the entity ``object_id``. Replaces
        any existing binding on that entity.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the created SearchConfigBinding.

        Raises:
            ValueError: If ``object_id`` or ``search_configuration_id`` is not set.

        Example: Bind a SearchConfiguration to a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            binding = SearchConfigBinding(
                object_id="syn12345",
                search_configuration_id="6789",
            )
            binding = binding.store()
            print(f"Bound SearchConfiguration {binding.search_configuration_id} "
                  f"to {binding.object_id}")
            ```
        """
        if not self.object_id:
            raise ValueError("SearchConfigBinding must have an object_id set.")
        if not self.search_configuration_id:
            raise ValueError(
                "SearchConfigBinding must have a search_configuration_id set."
            )
        result = await bind_search_config_to_entity(
            self.object_id,
            self.search_configuration_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(result)

    async def get_async(self, *, synapse_client: Optional["Synapse"] = None) -> "Self":
        """Get the effective binding for the entity ``object_id``, resolved by
        walking up the entity hierarchy.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Itself, populated from the resolved SearchConfigBinding.

        Raises:
            ValueError: If ``object_id`` is not set.

        Example: Get the effective binding for a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            binding = SearchConfigBinding(object_id="syn12345").get()
            print(binding.search_configuration_id)
            ```
        """
        if not self.object_id:
            raise ValueError("SearchConfigBinding must have an object_id set.")
        result = await get_search_config_binding(
            self.object_id, synapse_client=synapse_client
        )
        return self.fill_from_dict(result)

    async def delete_async(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """Clear the binding on the entity ``object_id``.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If ``object_id`` is not set.

        Example: Clear the binding on a Project.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SearchConfigBinding

            syn = Synapse()
            syn.login()

            SearchConfigBinding(object_id="syn12345").delete()
            ```
        """
        if not self.object_id:
            raise ValueError("SearchConfigBinding must have an object_id set.")
        await clear_search_config_binding(self.object_id, synapse_client=synapse_client)


@dataclass
class SearchQuery:
    """The body of an OpenSearch [`_search`](https://docs.opensearch.org/latest/api-reference/search-apis/search/)
    request, narrowed to the top-level keys Synapse accepts. Each slot's
    contents are pass-through [OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/),
    typed by the `TypedDict` shapes in `synapseclient.models.search_dsl`.

    Represents a [Synapse SearchQuery](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchQuery.html).
    """

    query: Optional[Query] = None
    """Required. The [OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/)
    clause -- see [Query][synapseclient.models.search_dsl.Query] for every
    supported clause kind. Use [`{"match_all": {}}`](https://docs.opensearch.org/latest/query-dsl/match-all/)
    to match all documents. See also
    [query vs. filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)."""

    post_filter: Optional[Query] = None
    """Optional. Same DSL shape as `query` (see
    [Query][synapseclient.models.search_dsl.Query]), applied *after*
    aggregations are computed. For the distinction between filtering hits and
    filtering aggregations, see
    [query vs. filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)."""

    aggregations: Optional[Dict[str, Aggregation]] = None
    """Optional. Map of caller-chosen name to
    [aggregation][synapseclient.models.search_dsl.Aggregation] definition. The
    raw aggregation result comes back on `SearchIndexQuery.aggregation_results`.
    For the facet-counting pattern (a `terms` aggregation alongside
    `post_filter`), see the
    [faceted search tutorial](https://docs.opensearch.org/latest/tutorials/faceted-search/#maintaining-facet-options-during-filtering)."""

    highlight: Optional[Highlight] = None
    """Optional. Adds per-field
    [snippet fragments](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/)
    (matched terms wrapped in `<em>` / `</em>` by default) to each hit's
    highlights."""

    collapse: Optional[FieldCollapse] = None
    """Optional. [Groups the result list](https://docs.opensearch.org/latest/search-plugins/searching-data/collapse-search/)
    so only one hit is returned per distinct value of a field."""

    rescore: Optional[Rescore] = None
    """Optional. [Re-ranks](https://docs.opensearch.org/latest/query-dsl/rescore/)
    the top hits returned by `query` using a secondary scoring query."""

    sort: Optional[List[Any]] = None
    """Optional. Result [ordering](https://docs.opensearch.org/latest/search-plugins/searching-data/sort/),
    in native OpenSearch sort shape (a string column name,
    `{column: "asc|desc"}`, or `{column: {order: ..., mode: ..., missing: ...}}`)
    applied in order. Only the `field` and `_score` sort kinds are accepted --
    script and geo-distance sorts are rejected server-side. The pseudo-column
    `_score` sorts by relevance. When omitted, results are sorted by relevance
    descending."""

    source: Optional[SourceFilter] = None
    """Optional. [Source filter](https://docs.opensearch.org/latest/search-plugins/searching-data/retrieve-specific-fields/)
    selecting which columns are returned on each hit. Serialized as
    `_source`."""

    from_: Optional[int] = None
    """Optional. Zero-based
    [pagination](https://docs.opensearch.org/latest/search-plugins/searching-data/paginate/)
    offset; default 0. Ignored when `search_after` is supplied. Serialized as
    `from`."""

    size: Optional[int] = None
    """Optional. Maximum number of hits to return per
    [page](https://docs.opensearch.org/latest/search-plugins/searching-data/paginate/).
    Default 25. Maximum 100 (larger values are silently capped)."""

    search_after: Optional[List[Optional[ScalarValue]]] = None
    """Optional. Opaque
    [cursor](https://docs.opensearch.org/latest/search-plugins/searching-data/paginate/)
    emitted as `next_search_after` on the previous response. Pass back
    unchanged. When supplied, `from_` is ignored."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.query = data.get("query", None)
        self.post_filter = data.get("post_filter", None)
        self.aggregations = data.get("aggregations", None)
        self.highlight = data.get("highlight", None)
        self.collapse = data.get("collapse", None)
        self.rescore = data.get("rescore", None)
        self.sort = data.get("sort", None)
        self.source = data.get("_source", None)
        self.from_ = data.get("from", None)
        self.size = data.get("size", None)
        self.search_after = data.get("search_after", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "query": self.query,
            "post_filter": self.post_filter,
            "aggregations": self.aggregations,
            "highlight": self.highlight,
            "collapse": self.collapse,
            "rescore": self.rescore,
            "sort": self.sort,
            "_source": self.source,
            "from": self.from_,
            "size": self.size,
            "search_after": self.search_after,
        }
        delete_none_keys(body)
        return body


@dataclass
class SearchAutocompleteRequest:
    """Body of a synchronous autocomplete request against a SearchIndex. The
    autocomplete endpoint allowlists only `query` (restricted to
    [`prefix`](https://docs.opensearch.org/latest/query-dsl/term/prefix/),
    [`match_phrase_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase-prefix/),
    or [`match_bool_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-bool-prefix/))
    and `_source`.

    Represents a [Synapse SearchAutocompleteRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchAutocompleteRequest.html).
    """

    search_index_id: Optional[str] = None
    """The ID of the SearchIndex entity to query."""

    query: Optional[Query] = None
    """Required. The top-level
    [Query][synapseclient.models.search_dsl.Query] DSL clause; restricted
    server-side to `prefix`, `match_phrase_prefix`, or
    `match_bool_prefix`."""

    source: Optional[SourceFilter] = None
    """Optional. [Source filter](https://docs.opensearch.org/latest/search-plugins/searching-data/retrieve-specific-fields/);
    same shape as `SearchQuery.source`. Serialized as `_source`."""

    def to_synapse_request(self) -> Dict[str, Any]:
        search_query = {
            "query": self.query,
            "_source": self.source,
        }
        delete_none_keys(search_query)
        body = {
            "searchIndexId": self.search_index_id,
            "searchQuery": search_query or None,
        }
        delete_none_keys(body)
        return body


@dataclass
class SearchFieldValue:
    """A name/value pair returned in a SearchHit's `fields`.

    Represents a [Synapse SearchFieldValue](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchFieldValue.html).
    """

    name: Optional[str] = None
    """The column name."""

    value: Optional[str] = None
    """The column value."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.name = data.get("name", None)
        self.value = data.get("value", None)
        return self


@dataclass
class SearchHighlight:
    """A per-field highlight payload on a SearchHit.

    Represents a [Synapse SearchHighlight](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchHighlight.html).
    """

    name: Optional[str] = None
    """The column name."""

    snippets: Optional[List[str]] = field(default_factory=list)
    """Highlighted snippet fragments. Matched terms are wrapped in pre/post tags
    (default `<em>` / `</em>`)."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.name = data.get("name", None)
        self.snippets = data.get("snippets", []) or []
        return self


@dataclass
class SearchHit:
    """A single matching document in a SearchQueryResults response.

    Represents a [Synapse SearchHit](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchHit.html).
    """

    row_id: Optional[int] = None
    """The row ID from the source table."""

    row_version: Optional[int] = None
    """The row version from the source table."""

    score: Optional[float] = None
    """The relevance score for this hit."""

    fields: Optional[List[SearchFieldValue]] = field(default_factory=list)
    """Column name/value pairs for the requested return fields."""

    highlights: Optional[List[SearchHighlight]] = field(default_factory=list)
    """Per-field highlight payloads, if highlight was requested."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.row_id = data.get("rowId", None)
        self.row_version = data.get("rowVersion", None)
        self.score = data.get("score", None)
        self.fields = [
            SearchFieldValue().fill_from_dict(f) for f in data.get("fields", []) or []
        ]
        self.highlights = [
            SearchHighlight().fill_from_dict(h)
            for h in data.get("highlights", []) or []
        ]
        return self


@dataclass
class SearchIndexQuery(AsynchronousCommunicator):
    """An async request to query a SearchIndex's OpenSearch index.

    Inherits from `AsynchronousCommunicator`: call `send_job_and_wait_async()` to
    submit the job, poll the Synapse async job service, and populate response
    fields (`hits`, `total_hits`, `select_columns`, `aggregation_results`,
    `next_search_after`, `offset`) on this same instance.

    The `search_query` body is an OpenSearch
    [`_search`](https://docs.opensearch.org/latest/api-reference/search-apis/search/)
    request -- see [SearchQuery][synapseclient.models.SearchQuery] for the
    allowlisted top-level keys and
    [Query][synapseclient.models.search_dsl.Query] for the
    [query DSL](https://docs.opensearch.org/latest/query-dsl/) clause kinds.

    Represents a [Synapse SearchIndexQuery](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchIndexQuery.html).

    Example: Run a search query.
        &nbsp;

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import SearchIndexQuery, SearchQuery, SearchQueryPart

        async def main():
            Synapse().login()
            query = SearchIndexQuery(
                search_index_id="syn22806626",
                search_query=SearchQuery(
                    query={"match": {"title": {"query": "alzheimer"}}},
                    size=10,
                ),
                response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
            )
            await query.send_job_and_wait_async()
            print(query.total_hits, len(query.hits))

        asyncio.run(main())
        ```
    """

    concrete_type: str = concrete_types.SEARCH_INDEX_QUERY
    """The Synapse concrete type identifying this async request."""

    search_index_id: Optional[str] = None
    """The ID of the SearchIndex entity to query."""

    search_query: Optional[SearchQuery] = None
    """The SearchQuery (OpenSearch `_search` body) to execute against the index."""

    response_parts: Optional[List[SearchQueryPart]] = field(default_factory=list)
    """Optional list of additional response parts beyond default HITS."""

    hits: Optional[List[SearchHit]] = field(default_factory=list)
    """Response: matching documents. Populated after `send_job_and_wait_async()`."""

    total_hits: Optional[int] = None
    """Response: total number of matching documents. Populated when
    SearchQueryPart.TOTAL_HITS is requested."""

    select_columns: Optional[List[SelectColumn]] = field(default_factory=list)
    """Response: columns represented in each hit's fields, in SELECT-clause
    order. Populated when SearchQueryPart.SELECT_COLUMNS is requested."""

    aggregation_results: Optional[Dict[str, Any]] = None
    """Response: the raw OpenSearch
    [aggregations](https://docs.opensearch.org/latest/aggregations/) response,
    with field references rewritten back to column names. Populated whenever the
    request supplied `search_query.aggregations`. Kept as an opaque JSON object
    because its shape mirrors whichever aggregations were requested."""

    next_search_after: Optional[List[Optional[ScalarValue]]] = None
    """Response: opaque
    [cursor](https://docs.opensearch.org/latest/search-plugins/searching-data/paginate/)
    for the next page. Pass back unchanged on the next request as
    `search_query.search_after`. Null when there are no further pages."""

    offset: Optional[int] = None
    """Response: zero-based pagination offset echoed from the request."""

    def to_synapse_request(self) -> Dict[str, Any]:
        """Convert to the SearchIndexQuery body for the async-job /start endpoint."""
        body = {
            "concreteType": self.concrete_type,
            "searchIndexId": self.search_index_id,
            "searchQuery": (
                self.search_query.to_synapse_request() if self.search_query else None
            ),
            "responseParts": (
                [p.value for p in self.response_parts] if self.response_parts else None
            ),
        }
        delete_none_keys(body)
        return body

    def fill_from_dict(self, synapse_response: Dict[str, Any]) -> "Self":
        """Populate response fields from a SearchQueryResults body.

        Called by `AsynchronousCommunicator.send_job_and_wait_async()` once the
        async job completes. Leaves request fields untouched.

        Modeled from [Synapse SearchQueryResults](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchQueryResults.html).
        """
        self.hits = [
            SearchHit().fill_from_dict(h)
            for h in synapse_response.get("hits", []) or []
        ]
        self.total_hits = synapse_response.get("totalHits", None)
        self.select_columns = [
            SelectColumn.fill_from_dict(c)
            for c in synapse_response.get("selectColumns", []) or []
        ]
        self.aggregation_results = synapse_response.get("aggregationResults", None)
        self.next_search_after = synapse_response.get("nextSearchAfter", None)
        self.offset = synapse_response.get("offset", None)
        return self
