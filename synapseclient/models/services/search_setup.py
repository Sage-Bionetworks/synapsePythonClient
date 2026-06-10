"""Helpers to set up SearchIndex-related resources against the v3 SearchManagement API.

These helpers idempotently upsert the chain of resources a SearchIndex needs to
have synonyms applied at search time:

    SynonymSet  ──► TextAnalyzer (clone of a system analyzer + $ref to the
                                   SynonymSet, wired into `analyzer.default_search`)
                ──► SearchConfiguration (default_analyzer = the cloned analyzer,
                                          plus optional per-column system-analyzer
                                          overrides)

The clone deliberately leaves `analyzer.default` (index-time) untouched and
synthesizes `analyzer.default_search` (search-time) as a copy of the default
filter chain with the `{"$ref": "<syn_qname>"}` filter appended. Synonyms are
therefore expanded at query time only and do not bake into the index.
"""

import copy
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

from synapseclient.api.search_services import (
    create_search_configuration,
    create_synonym_set,
    create_text_analyzer,
    list_search_configurations,
    list_synonym_sets,
    list_text_analyzers,
)
from synapseclient.models.search_management import (
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    SearchConfiguration,
    SynonymSet,
    TextAnalyzer,
)
from synapseclient.models.table_components import ColumnType

if TYPE_CHECKING:
    from synapseclient import Synapse

EQUIVALENT = "equivalent"
EXPLICIT = "explicit"

SYSTEM_ANALYZER_ORG = "org.sagebionetworks"

#: Default name of the filter-registry slot the synonym `$ref` is wired into
#: when cloning a system analyzer. Matches the convention used in the local
#: testing scripts and the Java `synonym_graph` filter examples.
DEFAULT_SYNONYM_FILTER_SLOT = "synonyms"

SynonymRule = Tuple[str, List[str]]


# ---------- ColumnType → system analyzer mapping ----------
#
# Python mirror of `ColumnTypeToOpenSearchMapping` in the repo:
# `Synapse-Repository-Services/services/repository-managers/src/main/java/`
# `org/sagebionetworks/repo/manager/search/ColumnTypeToOpenSearchMapping.java`.
# Each entry records the Synapse `ColumnType`, the OpenSearch field category,
# and the qualified name of the system TextAnalyzer used as the per-type
# default. Keep in sync with the Java enum.

#: Names of the bootstrapped system TextAnalyzers (see the Java
#: `TextAnalyzerBootstrapper`). Qualified form is `org.sagebionetworks-<NAME>`.
SYSTEM_ANALYZER_SCIENTIFIC = f"{SYSTEM_ANALYZER_ORG}-SCIENTIFIC"
SYSTEM_ANALYZER_STANDARD = f"{SYSTEM_ANALYZER_ORG}-STANDARD"
SYSTEM_ANALYZER_KEYWORD = f"{SYSTEM_ANALYZER_ORG}-KEYWORD"


COLUMN_TYPE_TO_DEFAULT_ANALYZER_QNAME: Dict[ColumnType, str] = {
    # Text categories — analyzed with SCIENTIFIC by default.
    ColumnType.STRING: SYSTEM_ANALYZER_SCIENTIFIC,
    ColumnType.STRING_LIST: SYSTEM_ANALYZER_SCIENTIFIC,
    ColumnType.MEDIUMTEXT: SYSTEM_ANALYZER_SCIENTIFIC,
    ColumnType.LARGETEXT: SYSTEM_ANALYZER_SCIENTIFIC,
    # LINK is text-shaped but defaults to KEYWORD per the Java mapping.
    ColumnType.LINK: SYSTEM_ANALYZER_KEYWORD,
    # Numeric / date — KEYWORD (no full-text analysis).
    ColumnType.INTEGER: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.INTEGER_LIST: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.DATE: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.DATE_LIST: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.FILEHANDLEID: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.SUBMISSIONID: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.EVALUATIONID: SYSTEM_ANALYZER_KEYWORD,
    # Identifier / boolean / double — KEYWORD.
    ColumnType.ENTITYID: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.USERID: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.ENTITYID_LIST: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.USERID_LIST: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.DOUBLE: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.BOOLEAN: SYSTEM_ANALYZER_KEYWORD,
    ColumnType.BOOLEAN_LIST: SYSTEM_ANALYZER_KEYWORD,
    # JSON — STANDARD.
    ColumnType.JSON: SYSTEM_ANALYZER_STANDARD,
}


def build_column_overrides(
    columns: Mapping[str, ColumnType],
    *,
    default_analyzer_qname: str,
    default_substitutes_system_qname: str = SYSTEM_ANALYZER_SCIENTIFIC,
    system_analyzer_substitutions: Optional[Mapping[str, str]] = None,
) -> List[Tuple[str, str]]:
    """Derive the ColumnAnalyzerOverride list for a SearchConfiguration whose
    `defaultAnalyzer` is `default_analyzer_qname`. The default analyzer is
    assumed to be a synonym-aware clone of `default_substitutes_system_qname`
    (default: SCIENTIFIC). Each column's system-default analyzer (per
    `COLUMN_TYPE_TO_DEFAULT_ANALYZER_QNAME`) is then mapped through
    `system_analyzer_substitutions` (a `{system_qname: replacement_qname}`
    map — typically `{system_qname: synonym_aware_clone_qname}`) before being
    compared to `default_analyzer_qname`. Columns whose effective analyzer
    matches the SearchConfiguration's default inherit the default; everything
    else is pinned via an override.

    Returns an ordered list of `(column_name, analyzer_qname)` pairs."""
    substitutions = dict(system_analyzer_substitutions or {})
    overrides: List[Tuple[str, str]] = []
    for column_name, column_type in columns.items():
        system_default = COLUMN_TYPE_TO_DEFAULT_ANALYZER_QNAME.get(column_type)
        if system_default is None:
            continue
        target = substitutions.get(system_default, system_default)
        if system_default == default_substitutes_system_qname:
            continue
        if target == default_analyzer_qname:
            continue
        overrides.append((column_name, target))
    return overrides


def render_synonym_definition(rules: List[SynonymRule]) -> Dict[str, Any]:
    """Render the rule list into the OpenSearch `synonym_graph` token-filter
    definition. EQUIVALENT rules become bidirectional `"a, b, c"` lines;
    EXPLICIT rules become directional `"first => first, t2, t3"` lines."""
    synonyms: List[str] = []
    for kind, terms in rules:
        if kind == EQUIVALENT:
            synonyms.append(", ".join(terms))
        elif kind == EXPLICIT:
            head = terms[0]
            synonyms.append(f"{head} => {', '.join(terms)}")
        else:
            raise ValueError(f"Unknown synonym rule kind: {kind!r}")
    return {"type": "synonym_graph", "synonyms": synonyms}


async def _find_by_name(
    list_fn,
    organization_name: str,
    name: str,
    *,
    synapse_client: "Synapse",
) -> Optional[Dict[str, Any]]:
    next_page_token = None
    while True:
        page = await list_fn(
            organization_name=organization_name,
            next_page_token=next_page_token,
            synapse_client=synapse_client,
        )
        for item in page.get("results", []) or []:
            if item.get("name") == name:
                return item
        next_page_token = page.get("nextPageToken")
        if not next_page_token:
            return None


async def get_system_analyzer(
    name: str,
    *,
    synapse_client: "Synapse",
) -> TextAnalyzer:
    """Look up a bootstrapped system analyzer by name (e.g. `SCIENTIFIC`,
    `KEYWORD`) under the `org.sagebionetworks` organization."""
    found = await _find_by_name(
        list_text_analyzers,
        SYSTEM_ANALYZER_ORG,
        name,
        synapse_client=synapse_client,
    )
    if found is None:
        raise ValueError(f"System analyzer '{SYSTEM_ANALYZER_ORG}-{name}' not found.")
    return TextAnalyzer().fill_from_dict(found)


def _build_search_chain(default_chain: List[str], slot_name: str) -> List[str]:
    """Build the `default_search` filter chain by reordering the index-time
    `default` chain so that the synonym filter sees normalized but ungraphed
    tokens. `synonym_graph` rejects input from graph token filters
    (`word_delimiter_graph` etc.), so the chain must be:

        [lowercase, <synonym_slot>, <everything else from `default`, in order>]

    `lowercase` is hoisted to the front (its position is irrelevant for
    correctness because it produces single-position tokens), the synonym
    filter goes immediately after it, and any graph-emitting filter from
    the original chain is preserved AFTER the synonym filter — matching the
    working pattern in scripts_v2/local_testing_search_index.py's demo
    `default_search`."""
    new_chain: List[str] = ["lowercase", slot_name]
    for name in default_chain:
        if name != "lowercase":
            new_chain.append(name)
    return new_chain


def clone_settings_with_search_synonyms(
    base_settings: Dict[str, Any],
    synonym_qname: str,
    *,
    filter_slot_name: str = DEFAULT_SYNONYM_FILTER_SLOT,
) -> Dict[str, Any]:
    """Deep-copy a system analyzer's `settings` and add a `default_search` entry
    that mirrors `default`'s filter chain with `{"$ref": synonym_qname}`
    inserted after `lowercase`. Synonyms are therefore applied at search time
    only and see normalized but unstemmed tokens (so multi-word and
    natural-language synonym rules match).

    If `default_search` already exists on the base analyzer, the synonym
    filter is spliced into that chain instead of rebuilding from `default`,
    so analyzers like AUTOCOMPLETE that already declare an asymmetric search
    chain keep their search-time behavior."""
    settings = copy.deepcopy(base_settings or {})
    analyzers = settings.setdefault("analyzer", {})
    if "default" not in analyzers:
        raise ValueError(
            "Base analyzer settings missing required `analyzer.default` entry."
        )
    extra_keys = set(analyzers.keys()) - {"default", "default_search"}
    if extra_keys:
        raise ValueError(
            "Base analyzer settings.analyzer must contain only 'default' and "
            f"optional 'default_search'; got extra keys: {sorted(extra_keys)}"
        )

    filters = settings.setdefault("filter", {})
    if filter_slot_name in filters:
        raise ValueError(
            f"Filter slot {filter_slot_name!r} already used in base analyzer settings."
        )
    filters[filter_slot_name] = {"$ref": synonym_qname}

    base_for_search = analyzers.get("default_search") or analyzers["default"]
    new_search = copy.deepcopy(base_for_search)
    new_search["filter"] = _build_search_chain(
        new_search.get("filter") or [], filter_slot_name
    )
    analyzers["default_search"] = new_search

    return settings


async def ensure_synonym_set(
    organization_name: str,
    name: str,
    *,
    rules: List[SynonymRule],
    description: Optional[str] = None,
    synapse_client: "Synapse",
) -> SynonymSet:
    """Find or create a SynonymSet under the given organization."""
    existing = await _find_by_name(
        list_synonym_sets, organization_name, name, synapse_client=synapse_client
    )
    if existing is not None:
        return SynonymSet().fill_from_dict(existing)

    synonym_set = SynonymSet(
        organization_name=organization_name,
        name=name,
        description=description,
        definition=render_synonym_definition(rules),
    )
    result = await create_synonym_set(
        request=synonym_set.to_synapse_request(),
        synapse_client=synapse_client,
    )
    return SynonymSet().fill_from_dict(result)


async def ensure_synonym_aware_analyzer(
    organization_name: str,
    name: str,
    *,
    base_system_analyzer_name: str,
    synonym_set_qname: str,
    description: Optional[str] = None,
    filter_slot_name: str = DEFAULT_SYNONYM_FILTER_SLOT,
    synapse_client: "Synapse",
) -> TextAnalyzer:
    """Find or create a TextAnalyzer that clones a system analyzer and adds a
    search-time-only synonym filter via `{"$ref": synonym_set_qname}` in the
    cloned analyzer's `settings.filter` registry."""
    existing = await _find_by_name(
        list_text_analyzers, organization_name, name, synapse_client=synapse_client
    )
    if existing is not None:
        return TextAnalyzer().fill_from_dict(existing)

    base = await get_system_analyzer(
        base_system_analyzer_name, synapse_client=synapse_client
    )
    settings = clone_settings_with_search_synonyms(
        base.settings or {},
        synonym_set_qname,
        filter_slot_name=filter_slot_name,
    )
    analyzer = TextAnalyzer(
        organization_name=organization_name,
        name=name,
        description=description,
        settings=settings,
    )
    result = await create_text_analyzer(
        request=analyzer.to_synapse_request(),
        synapse_client=synapse_client,
    )
    return TextAnalyzer().fill_from_dict(result)


async def ensure_search_configuration(
    organization_name: str,
    name: str,
    *,
    default_analyzer_qname: str,
    column_overrides: Optional[List[Tuple[str, str]]] = None,
    description: Optional[str] = None,
    synapse_client: "Synapse",
) -> SearchConfiguration:
    """Find or create a SearchConfiguration that points at the given default
    analyzer (by qualified name) and inlines a ColumnAnalyzerOverride for any
    `(column_name, analyzer_qname)` pairs supplied in `column_overrides`."""
    existing = await _find_by_name(
        list_search_configurations,
        organization_name,
        name,
        synapse_client=synapse_client,
    )
    if existing is not None:
        return SearchConfiguration().fill_from_dict(existing)

    inline_overrides: Optional[List[Dict[str, Any]]] = None
    if column_overrides:
        inline_overrides = [
            ColumnAnalyzerOverride(
                overrides=[
                    ColumnAnalyzerOverrideEntry.from_ref(col, qname)
                    for col, qname in column_overrides
                ],
            ).to_synapse_request()
        ]

    config = SearchConfiguration(
        organization_name=organization_name,
        name=name,
        description=description,
        default_analyzer=default_analyzer_qname,
        column_analyzer_overrides=inline_overrides,
    )
    result = await create_search_configuration(
        request=config.to_synapse_request(),
        synapse_client=synapse_client,
    )
    return SearchConfiguration().fill_from_dict(result)
