"""Search management dataclasses.

These dataclasses model the org-level search management resources used by
SearchIndex entities: TextAnalyzer, ColumnAnalyzerOverride, SynonymSet,
SearchConfiguration, and SearchConfigBinding.

Each resource belongs to an Organization and is referenced by qualified name
(`{organizationName}-{name}`). Resources are publicly readable; create/update
operations are restricted to Sage Bionetworks employees.

REST controller: <https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.SearchManagementController>
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from typing_extensions import Self

from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.table_components import SelectColumn

# ---------- Enums ----------


class SearchIndexState(str, Enum):
    """The state of a SearchIndex's OpenSearch index."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"


class SearchQueryPart(str, Enum):
    """Optional response parts for a SearchQuery beyond default HITS.

    These are values for the `responseParts` field on a SearchIndexQuery.
    """

    HITS = "HITS"
    FACETS = "FACETS"
    TOTAL_HITS = "TOTAL_HITS"
    SELECT_COLUMNS = "SELECT_COLUMNS"


class SearchQueryType(str, Enum):
    """The type of full-text query to execute against a search index.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchQueryType.html>
    """

    SIMPLE_QUERY_STRING = "SIMPLE_QUERY_STRING"
    """Supports +, -, |, quotes, ~, *, () operators. Best for user-facing
    search boxes."""

    MATCH = "MATCH"
    """Standard full-text match. Best for programmatic queries."""

    MULTI_MATCH = "MULTI_MATCH"
    """Matches across multiple fields with configurable boost."""

    MATCH_PHRASE = "MATCH_PHRASE"
    """Exact phrase matching. Terms must appear in order and adjacent."""

    PREFIX = "PREFIX"
    """Prefix matching for autocomplete and type-ahead."""

    WILDCARD = "WILDCARD"
    """Supports * and ? wildcards. Use sparingly."""

    MATCH_ALL = "MATCH_ALL"
    """Returns all documents. Used automatically when queryText is null
    or empty."""


class SortDirection(str, Enum):
    """Sort direction for search results.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SortDirection.html>
    """

    ASC = "ASC"
    DESC = "DESC"


class FacetSortField(str, Enum):
    """Field used to organize facet values in a terms aggregation.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/FacetSortField.html>
    """

    COUNT = "COUNT"
    """Sort by bucket document count. Maps to OpenSearch '_count'."""

    KEY = "KEY"
    """Sort by bucket key value. Maps to OpenSearch '_key'."""


# ---------- Text Analyzer ----------


@dataclass
class TextAnalyzer:
    """A shareable, named OpenSearch
    [custom analyzer](https://docs.opensearch.org/latest/analyzers/custom-analyzer/).

    The `settings` field is a JSON object holding the *contents of* the
    `settings.analysis` block of an OpenSearch
    [create-index](https://docs.opensearch.org/latest/api-reference/index-apis/create-index/)
    request body — **not** the wrapping `{"settings": {"analysis": ...}}`
    envelope, and **not** the full create-index body. Synapse resolves any
    `{"$ref": "{organizationName}-{name}"}` entries (only allowed inside
    the `filter` registry map) against existing SynonymSets at index-build
    time; everything else passes through to AOSS verbatim.

    One TextAnalyzer record exposes one externally-addressable analyzer:
    the inner `analyzer` map must declare exactly one entry named `default`
    (required), and may optionally declare a second entry named
    `default_search` (the asymmetric edge_ngram autocomplete pattern).
    Curators who need additional analyzers create additional TextAnalyzer
    records — each TextAnalyzer is itself shareable across SearchConfigurations.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/TextAnalyzer.html>
    """

    id: Optional[str] = None
    organization_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None
    """Required. JSON object holding the contents of the OpenSearch
    `settings.analysis` block. Allowed root keys: `char_filter`, `tokenizer`,
    `filter`, `analyzer`. The inner `analyzer` map must declare a `default`
    entry (required) and may optionally declare a `default_search` entry;
    any other entry is rejected. Cross-resource references use
    `{"$ref": "{organizationName}-{name}"}` inside the `filter` registry map
    (not in chain arrays) and must resolve to an existing SynonymSet."""
    etag: Optional[str] = None
    created_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_on: Optional[str] = None
    modified_by: Optional[str] = None

    @property
    def qualified_name(self) -> Optional[str]:
        """The qualified name '{organizationName}-{name}' used to reference
        this analyzer from a SearchConfiguration."""
        if self.organization_name and self.name:
            return f"{self.organization_name}-{self.name}"
        return None

    @staticmethod
    def ref(qualified_name: str) -> Dict[str, str]:
        """Return `{"$ref": qualified_name}` for use as a SearchConfiguration
        `defaultAnalyzer` or a ColumnAnalyzerOverrideEntry `analyzer`."""
        return {"$ref": qualified_name}

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


# ---------- Column Analyzer Override ----------


@dataclass
class ColumnAnalyzerOverrideEntry:
    """Per-column analyzer override entry. The referenced TextAnalyzer's
    `analyzer.default` entry drives index-time analysis; if it also declares
    `analyzer.default_search`, that drives search-time analysis."""

    column_name: Optional[str] = None
    analyzer: Optional[Union[Dict[str, Any], str]] = None
    """The TextAnalyzer to bind to this column. Either a `$ref` dict
    `{"$ref": "{organizationName}-{name}"}` pointing at a saved TextAnalyzer
    (preferred — supports reuse), or an inline TextAnalyzer literal pasted
    directly. A bare qualified-name string is normalized to a `$ref` dict on
    serialize. The TextAnalyzer's `analyzer.default` entry drives index-time
    analysis and its `analyzer.default_search` entry (if declared) drives
    search-time analysis."""

    @classmethod
    def from_ref(
        cls, column_name: str, qualified_name: str
    ) -> "ColumnAnalyzerOverrideEntry":
        """Build an entry whose analyzer is a `$ref` to a saved TextAnalyzer."""
        return cls(column_name=column_name, analyzer={"$ref": qualified_name})

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.column_name = data.get("columnName", None)
        self.analyzer = data.get("analyzer", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        analyzer_value: Optional[Union[Dict[str, Any], str]]
        if isinstance(self.analyzer, str):
            analyzer_value = {"$ref": self.analyzer}
        else:
            analyzer_value = self.analyzer
        body = {
            "columnName": self.column_name,
            "analyzer": analyzer_value,
        }
        delete_none_keys(body)
        return body


@dataclass
class ColumnAnalyzerOverride:
    """A shared resource containing per-column analyzer override entries.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/ColumnAnalyzerOverride.html>
    """

    id: Optional[str] = None
    organization_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    overrides: Optional[List[ColumnAnalyzerOverrideEntry]] = field(default_factory=list)
    etag: Optional[str] = None
    created_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_on: Optional[str] = None
    modified_by: Optional[str] = None

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


# ---------- Synonym Set ----------


@dataclass
class SynonymSet:
    """A shareable OpenSearch `synonym_graph` (or legacy `synonym`) token
    filter. Referenced from a TextAnalyzer's `settings.filter` registry map
    via `{"$ref": "{organizationName}-{name}"}`.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SynonymSet.html>
    """

    id: Optional[str] = None
    organization_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    definition: Optional[Dict[str, Any]] = None
    """Required. The full OpenSearch token filter definition as a JSON
    object. Must include `type` of `synonym` or `synonym_graph`. Synonyms are
    supplied inline via the `synonyms` array using OpenSearch's native syntax:
    `a, b, c` for equivalent (bidirectional), `a, b => c, d` for explicit
    (directional)."""
    etag: Optional[str] = None
    created_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_on: Optional[str] = None
    modified_by: Optional[str] = None

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


# ---------- Search Configuration ----------


@dataclass
class SearchConfiguration:
    """A reusable search configuration resource. Points at the default
    TextAnalyzer for the index and lists per-column overrides. Synonyms are
    not wired here — a TextAnalyzer that wants synonyms references a
    SynonymSet directly via `{"$ref": "{org}-{name}"}` inside its own
    `settings.filter` registry map.

    Asymmetric index-time / search-time analysis is expressed inside the
    chosen TextAnalyzer's settings (declare both `analyzer.default` and
    `analyzer.default_search`), not by splitting the configuration into two
    fields.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchConfiguration.html>
    """

    id: Optional[str] = None
    organization_name: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    default_analyzer: Optional[Union[Dict[str, Any], str]] = None
    """Optional. The TextAnalyzer that supplies this index's
    `analysis.analyzer.default` slot. Either a `$ref` dict
    `{"$ref": "{organizationName}-{name}"}` pointing at a saved TextAnalyzer
    (preferred — supports reuse), or an inline TextAnalyzer literal pasted
    directly. A bare qualified-name string is normalized to a `$ref` dict on
    serialize. If the chosen TextAnalyzer also declares an
    `analyzer.default_search` entry, that becomes the index's
    `analysis.analyzer.default_search`. If omitted, each column falls back to
    the system default analyzer for its data type."""
    column_analyzer_overrides: Optional[List[Union[Dict[str, Any], str]]] = field(
        default_factory=list
    )
    """Optional ordered list of ColumnAnalyzerOverride entries. Each entry is
    either a `$ref` dict `{"$ref": "{organizationName}-{name}"}` pointing at
    a saved ColumnAnalyzerOverride, an inline ColumnAnalyzerOverride literal
    (with its own `overrides` list), or a bare qualified-name string that is
    normalized to a `$ref` dict on serialize."""
    etag: Optional[str] = None
    created_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_on: Optional[str] = None
    modified_by: Optional[str] = None

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
        default_analyzer: Optional[Union[Dict[str, Any], str]]
        if isinstance(self.default_analyzer, str):
            default_analyzer = {"$ref": self.default_analyzer}
        else:
            default_analyzer = self.default_analyzer
        overrides: Optional[List[Dict[str, Any]]]
        if self.column_analyzer_overrides:
            overrides = [
                {"$ref": entry} if isinstance(entry, str) else entry
                for entry in self.column_analyzer_overrides
            ]
        else:
            overrides = None
        body = {
            "id": self.id,
            "organizationName": self.organization_name,
            "name": self.name,
            "description": self.description,
            "defaultAnalyzer": default_analyzer,
            "columnAnalyzerOverrides": overrides,
            "etag": self.etag,
        }
        delete_none_keys(body)
        return body


# ---------- Search Config Binding ----------


@dataclass
class SearchConfigBinding:
    """A binding between a SearchConfiguration and an entity.

    Effective configuration for an entity is resolved by walking up the
    hierarchy (entity -> folder -> project).
    """

    bind_id: Optional[str] = None
    search_configuration_id: Optional[str] = None
    object_id: Optional[str] = None
    object_type: Optional[str] = None
    created_by: Optional[str] = None
    created_on: Optional[str] = None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.bind_id = data.get("bindId", None)
        self.search_configuration_id = data.get("searchConfigurationId", None)
        self.object_id = data.get("objectId", None)
        self.object_type = data.get("objectType", None)
        self.created_by = data.get("createdBy", None)
        self.created_on = data.get("createdOn", None)
        return self


# ---------- Search Index Status / Query ----------


@dataclass
class SearchIndexStatus:
    """The build status of a SearchIndex's OpenSearch index."""

    search_index_id: Optional[str] = None
    state: Optional[SearchIndexState] = None
    changed_on: Optional[str] = None
    error_message: Optional[str] = None

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.search_index_id = data.get("searchIndexId", None)
        st = data.get("state", None)
        self.state = SearchIndexState(st) if st else None
        self.changed_on = data.get("changedOn", None)
        self.error_message = data.get("errorMessage", None)
        return self


@dataclass
class KeyValues:
    """Multi-value filter (IN clause) for a column.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/KeyValues.html>
    """

    key: Optional[str] = None
    """The column name to filter on."""

    values: Optional[List[str]] = field(default_factory=list)
    """The values to match."""

    not_: Optional[bool] = None
    """Excludes matching values when enabled; defaults to false. Serialized
    as `not` (a Python keyword)."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.key = data.get("key", None)
        self.values = data.get("values", []) or []
        self.not_ = data.get("not", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "key": self.key,
            "values": self.values or None,
            "not": self.not_,
        }
        delete_none_keys(body)
        return body


@dataclass
class KeyRange:
    """Range filter on a column. At least one of `min_value` or `max_value`
    must be set.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/KeyRange.html>
    """

    key: Optional[str] = None
    """The column name to filter on."""

    min_value: Optional[str] = None
    """Inclusive minimum boundary. Serialized as `min`."""

    max_value: Optional[str] = None
    """Inclusive maximum boundary. Serialized as `max`."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.key = data.get("key", None)
        self.min_value = data.get("min", None)
        self.max_value = data.get("max", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "key": self.key,
            "min": self.min_value,
            "max": self.max_value,
        }
        delete_none_keys(body)
        return body


@dataclass
class FacetRequest:
    """Column to aggregate as a facet.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/FacetRequest.html>
    """

    column_name: Optional[str] = None
    """The name of the column to aggregate."""

    max_value_count: Optional[int] = None
    """Maximum number of facet values to return. Default: 25."""

    sort_field: Optional[FacetSortField] = None
    """Field used to organize facet values."""

    sort_direction: Optional[SortDirection] = None
    """Sort direction for facet values."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.column_name = data.get("columnName", None)
        self.max_value_count = data.get("maxValueCount", None)
        sf = data.get("sortField", None)
        self.sort_field = FacetSortField(sf) if sf else None
        sd = data.get("sortDirection", None)
        self.sort_direction = SortDirection(sd) if sd else None
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "columnName": self.column_name,
            "maxValueCount": self.max_value_count,
            "sortField": self.sort_field.value if self.sort_field else None,
            "sortDirection": (
                self.sort_direction.value if self.sort_direction else None
            ),
        }
        delete_none_keys(body)
        return body


@dataclass
class SortField:
    """Sort specification for search results.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SortField.html>
    """

    column_name: Optional[str] = None
    """The column name to sort by, or '_score' for relevance."""

    direction: Optional[SortDirection] = None
    """The direction to apply when ordering results."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.column_name = data.get("columnName", None)
        d = data.get("direction", None)
        self.direction = SortDirection(d) if d else None
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "columnName": self.column_name,
            "direction": self.direction.value if self.direction else None,
        }
        delete_none_keys(body)
        return body


@dataclass
class SearchQuery:
    """A structured full-text query against a SearchIndex's OpenSearch index.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchQuery.html>
    """

    query_type: Optional[SearchQueryType] = None
    """The type of full-text query to execute. Required by the REST API."""

    query_text: Optional[str] = None
    """The search text. Null or empty matches all documents."""

    query_fields: Optional[List[str]] = field(default_factory=list)
    """Field names supporting boost notation (e.g., 'studyName^3'). Defaults
    to all indexed fields when empty."""

    terms_filters: Optional[List[KeyValues]] = field(default_factory=list)
    """Multi-value filters (IN clause)."""

    range_filters: Optional[List[KeyRange]] = field(default_factory=list)
    """Range filters with min and max."""

    exists_filters: Optional[List[str]] = field(default_factory=list)
    """Columns that must have a non-null value."""

    not_exists_filters: Optional[List[str]] = field(default_factory=list)
    """Columns that must be null or missing."""

    fuzziness: Optional[str] = None
    """Typo tolerance: 'AUTO', '0', '1', or '2'."""

    facet_requests: Optional[List[FacetRequest]] = field(default_factory=list)
    """Columns to aggregate as facets."""

    return_fields: Optional[List[str]] = field(default_factory=list)
    """Columns included in results; all columns returned when empty."""

    sort: Optional[List[SortField]] = field(default_factory=list)
    """Sort order. Default: relevance descending."""

    highlight: Optional[bool] = None
    """Returns highlighted snippets; defaults to false."""

    offset: Optional[int] = None
    """Zero-based pagination offset. Default: 0."""

    limit: Optional[int] = None
    """Results per page. Default: 25, maximum: 100."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        qt = data.get("queryType", None)
        self.query_type = SearchQueryType(qt) if qt else None
        self.query_text = data.get("queryText", None)
        self.query_fields = data.get("queryFields", []) or []
        self.terms_filters = [
            KeyValues().fill_from_dict(f) for f in data.get("termsFilters", []) or []
        ]
        self.range_filters = [
            KeyRange().fill_from_dict(f) for f in data.get("rangeFilters", []) or []
        ]
        self.exists_filters = data.get("existsFilters", []) or []
        self.not_exists_filters = data.get("notExistsFilters", []) or []
        self.fuzziness = data.get("fuzziness", None)
        self.facet_requests = [
            FacetRequest().fill_from_dict(f)
            for f in data.get("facetRequests", []) or []
        ]
        self.return_fields = data.get("returnFields", []) or []
        self.sort = [SortField().fill_from_dict(s) for s in data.get("sort", []) or []]
        self.highlight = data.get("highlight", None)
        self.offset = data.get("offset", None)
        self.limit = data.get("limit", None)
        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        body = {
            "queryType": self.query_type.value if self.query_type else None,
            "queryText": self.query_text,
            "queryFields": self.query_fields or None,
            "termsFilters": (
                [f.to_synapse_request() for f in self.terms_filters]
                if self.terms_filters
                else None
            ),
            "rangeFilters": (
                [f.to_synapse_request() for f in self.range_filters]
                if self.range_filters
                else None
            ),
            "existsFilters": self.exists_filters or None,
            "notExistsFilters": self.not_exists_filters or None,
            "fuzziness": self.fuzziness,
            "facetRequests": (
                [f.to_synapse_request() for f in self.facet_requests]
                if self.facet_requests
                else None
            ),
            "returnFields": self.return_fields or None,
            "sort": (
                [s.to_synapse_request() for s in self.sort] if self.sort else None
            ),
            "highlight": self.highlight,
            "offset": self.offset,
            "limit": self.limit,
        }
        delete_none_keys(body)
        return body


@dataclass
class SearchFieldValue:
    """A name/value pair returned in a SearchHit's `fields` or `highlights`.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchFieldValue.html>
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
class SearchHit:
    """A single matching document in a SearchQueryResults response.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchHit.html>
    """

    row_id: Optional[int] = None
    """The row ID from the source table."""

    row_version: Optional[int] = None
    """The row version from the source table."""

    score: Optional[float] = None
    """The relevance score for this hit."""

    fields: Optional[List[SearchFieldValue]] = field(default_factory=list)
    """Column name/value pairs for the requested return fields."""

    highlights: Optional[List[SearchFieldValue]] = field(default_factory=list)
    """Column name/highlighted snippet pairs, if highlight was requested."""

    def fill_from_dict(self, data: Dict[str, Any]) -> "Self":
        self.row_id = data.get("rowId", None)
        self.row_version = data.get("rowVersion", None)
        self.score = data.get("score", None)
        self.fields = [
            SearchFieldValue().fill_from_dict(f) for f in data.get("fields", []) or []
        ]
        self.highlights = [
            SearchFieldValue().fill_from_dict(h)
            for h in data.get("highlights", []) or []
        ]
        return self


@dataclass
class SearchIndexQuery(AsynchronousCommunicator):
    """An async request to query a SearchIndex's OpenSearch index.

    Inherits from `AsynchronousCommunicator`: call `send_job_and_wait_async()` to
    submit the job, poll the Synapse async job service, and populate response
    fields (`hits`, `total_hits`, `select_columns`, `facets`, `offset`) on this
    same instance.

    REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/table/SearchIndexQuery.html>

    Example: Run a search query.

        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.models import (
            SearchIndexQuery, SearchQuery, SearchQueryPart, SearchQueryType,
        )

        async def main():
            Synapse().login()
            query = SearchIndexQuery(
                search_index_id="syn22806626",
                search_query=SearchQuery(
                    query_type=SearchQueryType.SIMPLE_QUERY_STRING,
                    query_text="alzheimer",
                    limit=10,
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
    """The structured SearchQuery to execute against the index."""

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

    facets: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    """Response: facet aggregation results. Populated when
    SearchQueryPart.FACETS is requested. Kept as raw dicts because
    FacetColumnResult has multiple polymorphic shapes."""

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

        REST: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/search/SearchQueryResults.html>
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
        self.facets = synapse_response.get("facets", []) or []
        self.offset = synapse_response.get("offset", None)
        return self
