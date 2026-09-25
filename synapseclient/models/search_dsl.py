"""Typed shapes for the OpenSearch query DSL accepted by Synapse.

These `TypedDict` types mirror the server-side schema for a SearchIndex query
one-for-one, so a dict literal written inline against a
[SearchQuery][synapseclient.models.SearchQuery] slot is checked by your IDE
against the exact shape the Synapse REST API expects.

Every type is `total=False`: all keys are optional to the type checker, and the
per-field docstrings mark which ones the server requires. A `TypedDict` is a
plain `dict` at runtime, so these annotations add no validation and no overhead,
and a raw dict is always an acceptable value.

Start at [Query][synapseclient.models.search_dsl.Query] for the query clause
kinds and [Aggregation][synapseclient.models.search_dsl.Aggregation] for the
aggregation kinds. Both link out to the relevant
[OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/) reference
for each clause.

Background reading:

- [OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/)
- [Query vs. filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)
- [Term-level vs. full-text queries](https://docs.opensearch.org/latest/query-dsl/term-vs-full-text/)
- [Aggregations](https://docs.opensearch.org/latest/aggregations/)
- [The `_search` API](https://docs.opensearch.org/latest/api-reference/search-apis/search/)
"""

from typing import Any, Dict, List, TypedDict, Union

ScalarValue = Union[str, int, float, bool]
"""A single column value. Which of these is valid depends on the target
column's type in the SearchIndex schema."""


AnalyzerRef = TypedDict("AnalyzerRef", {"$ref": str}, total=False)
"""A reference to a saved [TextAnalyzer][synapseclient.models.TextAnalyzer] or
[SynonymSet][synapseclient.models.SynonymSet] by its qualified name
`{organizationName}-{name}`, written as `{"$ref": "my.org-stemmed_english"}`."""


class ClauseScoringOptions(TypedDict, total=False):
    """Scoring options shared by the OpenSearch query clauses: a relevance
    `boost` and a `_name` label."""

    boost: float
    """Optional. Multiplier for the relevance score of this clause.
    Default `1.0`."""

    _name: str
    """Optional. Label echoed back in matched-queries metadata."""


class FuzzyMatchOptions(TypedDict, total=False):
    """Fuzzy-matching parameters shared by the analyzed full-text match clauses
    (`match`, `match_bool_prefix`, `multi_match`)."""

    fuzziness: Union[int, str]
    """Optional. Allowed [edit distance](https://docs.opensearch.org/latest/query-dsl/term/fuzzy/):
    an integer or `AUTO`."""

    fuzzy_rewrite: str
    """Optional. How the fuzzy query is rewritten internally."""

    fuzzy_transpositions: bool
    """Optional. Whether to count transpositions (ab -> ba) as a single edit.
    Default `true`."""

    prefix_length: int
    """Optional. Number of leading characters left unchanged when fuzzy
    matching."""


class MinimumShouldMatchOption(TypedDict, total=False):
    """The minimum-should-match option shared by the analyzed full-text match
    clauses."""

    minimum_should_match: Union[int, str]
    """Optional. [Minimum number of terms](https://docs.opensearch.org/latest/query-dsl/minimum-should-match/)
    a document must match. An integer or a percentage / formula string."""


class ZeroTermsQueryOption(TypedDict, total=False):
    """The zero-terms-query behavior shared by the analyzed full-text match
    clauses."""

    zero_terms_query: str
    """Optional. Behavior when the analyzer removes all tokens: `none`
    (default) or `all`."""


class MatchFieldOptions(
    ClauseScoringOptions,
    FuzzyMatchOptions,
    MinimumShouldMatchOption,
    ZeroTermsQueryOption,
    total=False,
):
    """Per-field options for a [`match`](https://docs.opensearch.org/latest/query-dsl/full-text/match/)
    full-text clause. Carried as the value of the field-keyed `match` map (the
    map key is the column name)."""

    query: ScalarValue
    """Required. The text (or scalar value on a non-text column) to match. A
    string, number, or boolean depending on the target column type."""

    operator: str
    """Optional. Boolean logic used to combine the analyzed query terms: `or`
    (default) or `and`."""

    analyzer: str
    """Optional. Analyzer used to tokenize the query text. Defaults to the
    field's search analyzer."""

    max_expansions: int
    """Optional. Maximum number of terms the fuzzy expansion will generate."""

    cutoff_frequency: float
    """Optional. Term-frequency threshold above which terms are treated as
    low-importance."""

    auto_generate_synonyms_phrase_query: bool
    """Optional. Whether to auto-generate phrase queries for multi-term
    synonyms. Default `true`."""

    lenient: bool
    """Optional. When `true`, format-based errors (e.g. a text value against a
    numeric field) are ignored."""


class MatchPhraseFieldOptions(ClauseScoringOptions, ZeroTermsQueryOption, total=False):
    """Per-field options for a [`match_phrase`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase/)
    clause. Carried as the value of the field-keyed `match_phrase` map (the map
    key is the column name)."""

    query: ScalarValue
    """Required. The phrase to match. A string (or scalar value on a non-text
    column)."""

    analyzer: str
    """Optional. Analyzer used to tokenize the phrase. Defaults to the field's
    search analyzer."""

    slop: int
    """Optional. Number of positions allowed between matching terms. Default
    `0` (exact phrase)."""


class MatchPhrasePrefixFieldOptions(
    ClauseScoringOptions, ZeroTermsQueryOption, total=False
):
    """Per-field options for a [`match_phrase_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase-prefix/)
    clause. Carried as the value of the field-keyed `match_phrase_prefix` map
    (the map key is the column name)."""

    query: ScalarValue
    """Required. The phrase whose last term is treated as a prefix. A
    string."""

    analyzer: str
    """Optional. Analyzer used to tokenize the phrase. Defaults to the field's
    search analyzer."""

    slop: int
    """Optional. Number of positions allowed between matching terms. Default
    `0`."""

    max_expansions: int
    """Optional. Maximum number of terms the last (prefix) term expands into.
    Default `50`."""


class MatchBoolPrefixFieldOptions(
    ClauseScoringOptions, FuzzyMatchOptions, MinimumShouldMatchOption, total=False
):
    """Per-field options for a [`match_bool_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-bool-prefix/)
    clause. Carried as the value of the field-keyed `match_bool_prefix` map
    (the map key is the column name)."""

    query: ScalarValue
    """Required. The text whose terms are matched, with the final term treated
    as a prefix. A string."""

    operator: str
    """Optional. Boolean logic used to combine the analyzed terms: `or`
    (default) or `and`."""

    analyzer: str
    """Optional. Analyzer used to tokenize the query text. Defaults to the
    field's search analyzer."""

    max_expansions: int
    """Optional. Maximum number of terms the final (prefix) term expands into.
    Default `50`."""


class TermFieldOptions(ClauseScoringOptions, total=False):
    """Per-field options for a [`term`](https://docs.opensearch.org/latest/query-dsl/term/term/)
    term-level clause (exact, non-analyzed match). Carried as the value of the
    field-keyed `term` map (the map key is the column name)."""

    value: ScalarValue
    """Required. The exact value to match. A string, number, boolean, or date
    depending on the target column type."""

    case_insensitive: bool
    """Optional. When `true`, matches the value regardless of case. Default
    `false`."""


class RangeFieldOptions(ClauseScoringOptions, total=False):
    """Per-field options for a [`range`](https://docs.opensearch.org/latest/query-dsl/term/range/)
    term-level clause. Carried as the value of the field-keyed `range` map (the
    map key is the column name)."""

    gte: ScalarValue
    """Optional. Greater-than-or-equal-to bound. A number or date string, per
    the target column type."""

    gt: ScalarValue
    """Optional. Greater-than bound."""

    lte: ScalarValue
    """Optional. Less-than-or-equal-to bound."""

    lt: ScalarValue
    """Optional. Less-than bound."""

    format: str
    """Optional. Date format used to parse the bound values on a date
    column."""

    relation: str
    """Optional. How the range relates to range-typed field values:
    `INTERSECTS` (default), `CONTAINS`, or `WITHIN`."""

    time_zone: str
    """Optional. UTC offset or IANA zone used to interpret date bounds."""


class PrefixFieldOptions(ClauseScoringOptions, total=False):
    """Per-field options for a [`prefix`](https://docs.opensearch.org/latest/query-dsl/term/prefix/)
    term-level clause. Carried as the value of the field-keyed `prefix` map
    (the map key is the column name). A leading `*` or `?` in `value` is
    rejected (it forces a full index scan)."""

    value: ScalarValue
    """Required. The prefix the indexed term must start with. A string (or
    scalar value on a non-text column)."""

    case_insensitive: bool
    """Optional. When `true`, matches the prefix regardless of case. Default
    `false`."""

    rewrite: str
    """Optional. How the multi-term query is rewritten internally."""


class WildcardFieldOptions(ClauseScoringOptions, total=False):
    """Per-field options for a [`wildcard`](https://docs.opensearch.org/latest/query-dsl/term/wildcard/)
    term-level clause. Carried as the value of the field-keyed `wildcard` map
    (the map key is the column name). A leading `*` or `?` in the pattern is
    rejected (it forces a full index scan)."""

    value: ScalarValue
    """Optional. The wildcard pattern (`*` matches any sequence, `?` matches a
    single character). A string. Either `value` or `wildcard` supplies the
    pattern."""

    wildcard: ScalarValue
    """Optional. Alias for `value` -- the wildcard pattern. A string."""

    case_insensitive: bool
    """Optional. When `true`, matches the pattern regardless of case. Default
    `false`."""

    rewrite: str
    """Optional. How the multi-term query is rewritten internally."""


class FuzzyFieldOptions(ClauseScoringOptions, total=False):
    """Per-field options for a [`fuzzy`](https://docs.opensearch.org/latest/query-dsl/term/fuzzy/)
    term-level clause. Carried as the value of the field-keyed `fuzzy` map (the
    map key is the column name)."""

    value: ScalarValue
    """Required. The term to match within the allowed edit distance. A string
    (or scalar value on a non-text column)."""

    fuzziness: Union[int, str]
    """Optional. Allowed [edit distance](https://docs.opensearch.org/latest/query-dsl/term/fuzzy/):
    an integer or `AUTO`."""

    max_expansions: int
    """Optional. Maximum number of terms the fuzzy expansion will generate.
    Default `50`."""

    prefix_length: int
    """Optional. Number of leading characters left unchanged when fuzzy
    matching."""

    transpositions: bool
    """Optional. Whether to count transpositions (ab -> ba) as a single edit.
    Default `true`."""

    rewrite: str
    """Optional. How the multi-term query is rewritten internally."""


class ExistsQuery(ClauseScoringOptions, total=False):
    """An [`exists`](https://docs.opensearch.org/latest/query-dsl/term/exists/)
    term-level clause. Matches documents that have any non-null value for the
    given column."""

    field: str
    """Required. The column that must have a value."""


class MultiMatchQuery(
    ClauseScoringOptions,
    FuzzyMatchOptions,
    MinimumShouldMatchOption,
    ZeroTermsQueryOption,
    total=False,
):
    """A [`multi_match`](https://docs.opensearch.org/latest/query-dsl/full-text/multi-match/)
    full-text clause -- a `match` run across several columns at once."""

    query: ScalarValue
    """Required. The text to match across the listed columns. A string."""

    fields: List[str]
    """Required. The columns to search. Each entry may carry a `^boost` suffix
    (e.g. `title^2`)."""

    type: str
    """Optional. How the per-field matches are combined: `best_fields`
    (default), `most_fields`, `cross_fields`, `phrase`, `phrase_prefix`, or
    `bool_prefix`."""

    operator: str
    """Optional. Boolean logic used to combine the analyzed terms: `or`
    (default) or `and`."""

    tie_breaker: float
    """Optional. Weight (0-1) applied to non-best field scores in
    `best_fields` / `cross_fields`."""

    analyzer: str
    """Optional. Analyzer used to tokenize the query text. Defaults to each
    field's search analyzer."""

    max_expansions: int
    """Optional. Maximum number of terms a fuzzy / prefix expansion will
    generate. Default `50`."""

    slop: int
    """Optional. Number of positions allowed between matching terms for the
    phrase types."""

    cutoff_frequency: float
    """Optional. Term-frequency threshold above which terms are treated as
    low-importance."""

    auto_generate_synonyms_phrase_query: bool
    """Optional. Whether to auto-generate phrase queries for multi-term
    synonyms. Default `true`."""

    lenient: bool
    """Optional. When `true`, format-based errors are ignored."""


class SimpleQueryStringQuery(
    ClauseScoringOptions, MinimumShouldMatchOption, total=False
):
    """A [`simple_query_string`](https://docs.opensearch.org/latest/query-dsl/full-text/simple-query-string/)
    full-text clause -- a compact mini-DSL (`+`, `|`, `-`, `"`, `*`, `()`)
    parsed leniently across the listed columns."""

    query: str
    """Required. The simple-query-string expression."""

    fields: List[str]
    """Optional. The columns to search. Each entry may carry a `^boost`
    suffix. Defaults to the index's default search fields."""

    default_operator: str
    """Optional. Boolean logic used between terms when no explicit operator is
    given: `or` (default) or `and`."""

    flags: str
    """Optional. Pipe-delimited list of enabled syntax features (e.g.
    `AND|OR|PREFIX`), or `ALL` / `NONE`."""

    analyzer: str
    """Optional. Analyzer used to tokenize the query text. Defaults to each
    field's search analyzer."""

    analyze_wildcard: bool
    """Optional. Whether to analyze wildcard terms. Default `false`. A leading
    wildcard with this enabled is rejected (it forces a full index scan)."""

    auto_generate_synonyms_phrase_query: bool
    """Optional. Whether to auto-generate phrase queries for multi-term
    synonyms. Default `true`."""

    fuzzy_max_expansions: int
    """Optional. Maximum number of terms a fuzzy expansion will generate.
    Default `50`."""

    fuzzy_prefix_length: int
    """Optional. Number of leading characters left unchanged when fuzzy
    matching."""

    fuzzy_transpositions: bool
    """Optional. Whether to count transpositions (ab -> ba) as a single edit.
    Default `true`."""

    lenient: bool
    """Optional. When `true`, format-based errors are ignored."""

    quote_field_suffix: str
    """Optional. Suffix appended to field names for quoted (exact-phrase)
    portions of the query."""


class MatchAllQuery(ClauseScoringOptions, total=False):
    """A [`match_all`](https://docs.opensearch.org/latest/query-dsl/match-all/)
    clause. Matches every document. Use `{"match_all": {}}` to match all
    documents."""


class BoolQuery(ClauseScoringOptions, total=False):
    """A [`bool`](https://docs.opensearch.org/latest/query-dsl/compound/bool/)
    compound clause -- combines sub-clauses with boolean logic."""

    must: List["Query"]
    """Sub-clauses that must all match (scored). Logical AND."""

    should: List["Query"]
    """Sub-clauses that should match (scored). See `minimum_should_match`."""

    must_not: List["Query"]
    """Sub-clauses that must not match (filter context, not scored)."""

    filter: List["Query"]
    """Sub-clauses that must all match in
    [filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)
    (not scored)."""

    minimum_should_match: Union[int, str]
    """Optional. How many `should` clauses must match. An integer or a
    [percentage / formula string](https://docs.opensearch.org/latest/query-dsl/minimum-should-match/)."""

    adjust_pure_negative: bool
    """Optional. Whether to automatically add a `match_all` when only negative
    clauses are present. Default `true`."""


class DisMaxQuery(ClauseScoringOptions, total=False):
    """A [`dis_max`](https://docs.opensearch.org/latest/query-dsl/compound/disjunction-max/)
    compound clause. A document matches if any sub-clause matches; its score is
    the best single sub-clause score plus `tie_breaker` times the rest."""

    queries: List["Query"]
    """Required. The candidate clauses."""

    tie_breaker: float
    """Optional. Weight (0-1) applied to the scores of the non-best matching
    clauses. Default `0.0`."""


class ConstantScoreQuery(ClauseScoringOptions, total=False):
    """A [`constant_score`](https://docs.opensearch.org/latest/query-dsl/compound/constant-score/)
    compound clause. Wraps a filter and assigns every matching document the
    same score (`boost`)."""

    filter: "Query"
    """Required. The clause evaluated in
    [filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)."""


class BoostingQuery(ClauseScoringOptions, total=False):
    """A [`boosting`](https://docs.opensearch.org/latest/query-dsl/compound/boosting/)
    compound clause. Returns documents matching `positive`, demoting those that
    also match `negative` by `negative_boost`."""

    positive: "Query"
    """Required. The clause documents must match."""

    negative: "Query"
    """Required. The clause whose matches are demoted."""

    negative_boost: float
    """Required. Multiplier (0-1) applied to the score of documents that also
    match `negative`."""


class Query(TypedDict, total=False):
    """A single [OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/)
    clause. Exactly one of the keys below may be set -- the set key names the
    clause kind. See [query vs. filter context](https://docs.opensearch.org/latest/query-dsl/query-filter-context/)
    and [term-level vs. full-text queries](https://docs.opensearch.org/latest/query-dsl/term-vs-full-text/).

    The field-keyed leaf clauses (`match`, `term`, `range`, ...) are maps whose
    key is the column name and whose value is the per-field options object --
    only the long form is accepted (e.g. `{"match": {"title": {"query": "x"}}}`,
    not the `{"match": {"title": "x"}}` shorthand). The compound clauses
    (`bool`, `dis_max`, ...) nest further Query DSL clauses recursively.
    """

    match: Dict[str, MatchFieldOptions]
    """A [`match`](https://docs.opensearch.org/latest/query-dsl/full-text/match/)
    full-text clause. Map of column name to its match options."""

    match_phrase: Dict[str, MatchPhraseFieldOptions]
    """A [`match_phrase`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase/)
    clause. Map of column name to its phrase options."""

    match_phrase_prefix: Dict[str, MatchPhrasePrefixFieldOptions]
    """A [`match_phrase_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase-prefix/)
    clause. Map of column name to its options."""

    match_bool_prefix: Dict[str, MatchBoolPrefixFieldOptions]
    """A [`match_bool_prefix`](https://docs.opensearch.org/latest/query-dsl/full-text/match-bool-prefix/)
    clause. Map of column name to its options."""

    term: Dict[str, TermFieldOptions]
    """A [`term`](https://docs.opensearch.org/latest/query-dsl/term/term/)
    term-level clause. Map of column name to its term options."""

    range: Dict[str, RangeFieldOptions]
    """A [`range`](https://docs.opensearch.org/latest/query-dsl/term/range/)
    term-level clause. Map of column name to its range bounds."""

    prefix: Dict[str, PrefixFieldOptions]
    """A [`prefix`](https://docs.opensearch.org/latest/query-dsl/term/prefix/)
    term-level clause. Map of column name to its prefix options."""

    wildcard: Dict[str, WildcardFieldOptions]
    """A [`wildcard`](https://docs.opensearch.org/latest/query-dsl/term/wildcard/)
    term-level clause. Map of column name to its wildcard options."""

    fuzzy: Dict[str, FuzzyFieldOptions]
    """A [`fuzzy`](https://docs.opensearch.org/latest/query-dsl/term/fuzzy/)
    term-level clause. Map of column name to its fuzzy options."""

    terms: Dict[str, Any]
    """A [`terms`](https://docs.opensearch.org/latest/query-dsl/term/terms/)
    term-level clause (matches any of several exact values). Field-keyed:
    `{"terms": {"<column>": [v1, v2], "boost": 1.0}}`. The cross-index
    `terms`-lookup form is rejected. Untyped because the column name is itself a
    key alongside the fixed option keys, which a `TypedDict` cannot express; the
    same allowlist is enforced server-side."""

    exists: ExistsQuery
    """An [`exists`](https://docs.opensearch.org/latest/query-dsl/term/exists/)
    term-level clause."""

    multi_match: MultiMatchQuery
    """A [`multi_match`](https://docs.opensearch.org/latest/query-dsl/full-text/multi-match/)
    full-text clause."""

    simple_query_string: SimpleQueryStringQuery
    """A [`simple_query_string`](https://docs.opensearch.org/latest/query-dsl/full-text/simple-query-string/)
    full-text clause."""

    match_all: MatchAllQuery
    """A [`match_all`](https://docs.opensearch.org/latest/query-dsl/match-all/)
    clause."""

    bool: BoolQuery
    """A [`bool`](https://docs.opensearch.org/latest/query-dsl/compound/bool/)
    compound clause -- combines sub-clauses with boolean logic."""

    dis_max: DisMaxQuery
    """A [`dis_max`](https://docs.opensearch.org/latest/query-dsl/compound/disjunction-max/)
    compound clause."""

    constant_score: ConstantScoreQuery
    """A [`constant_score`](https://docs.opensearch.org/latest/query-dsl/compound/constant-score/)
    compound clause."""

    boosting: BoostingQuery
    """A [`boosting`](https://docs.opensearch.org/latest/query-dsl/compound/boosting/)
    compound clause."""


class ExtendedBounds(TypedDict, total=False):
    """Min/max bounds that force a [`histogram`](https://docs.opensearch.org/latest/aggregations/bucket/histogram/)
    or [`date_histogram`](https://docs.opensearch.org/latest/aggregations/bucket/date-histogram/)
    to emit buckets across the full range (used as `extended_bounds` or
    `hard_bounds`). Bounding the range is what caps the bucket count."""

    min: ScalarValue
    """Lower bound. A number, or a date / date-math string on a
    `date_histogram`."""

    max: ScalarValue
    """Upper bound. A number, or a date / date-math string on a
    `date_histogram`."""


class HistogramBoundsOptions(TypedDict, total=False):
    """The extended/hard bounds options shared by the `histogram` and
    `date_histogram` aggregations."""

    extended_bounds: ExtendedBounds
    """Optional. Forces buckets to span at least this min/max range."""

    hard_bounds: ExtendedBounds
    """Optional. Restricts buckets to this min/max range (values outside are
    dropped)."""


class KeyedBucketOption(TypedDict, total=False):
    """The keyed-output option shared by the bucketing aggregations."""

    keyed: bool
    """Optional. Whether to return buckets as a keyed object rather than an
    array."""


class MetricAggregation(TypedDict, total=False):
    """Common options for the single-value numeric metric aggregations (`avg`,
    `max`, `min`, `sum`)."""

    field: str
    """Required. The numeric column to aggregate."""

    format: str
    """Optional. Format applied to the result value."""


class MissingValueOption(TypedDict, total=False):
    """The missing-value substitution option shared by the metric
    aggregations."""

    missing: ScalarValue
    """Optional. Value substituted for documents missing `field`."""


BucketOrder = Union[Dict[str, str], List[Dict[str, str]]]
"""Bucket sort order for a bucketing aggregation -- a `{metric: "asc|desc"}`
object or an array of them, applied in order."""


class TermsAggregation(TypedDict, total=False):
    """A [`terms`](https://docs.opensearch.org/latest/aggregations/bucket/terms/)
    bucket aggregation -- one bucket per distinct value of `field`."""

    field: str
    """Required. The column to bucket by."""

    size: int
    """Optional. Maximum number of buckets to return. Capped server-side."""

    shard_size: int
    """Optional. Number of candidate buckets collected per shard before the
    final reduce. Capped server-side."""

    min_doc_count: int
    """Optional. Minimum document count for a bucket to be returned. Default
    `1`."""

    shard_min_doc_count: int
    """Optional. Per-shard minimum document count before a bucket is
    considered."""

    show_term_doc_count_error: bool
    """Optional. Whether to return the per-bucket document-count error
    bound."""

    order: BucketOrder
    """Optional. Bucket sort order -- a `{metric: "asc|desc"}` object or an
    array of them."""

    include: Union[str, List[ScalarValue]]
    """Optional. Terms to include -- a regex string or an array of exact
    values."""

    exclude: Union[str, List[ScalarValue]]
    """Optional. Terms to exclude -- a regex string or an array of exact
    values."""

    missing: ScalarValue
    """Optional. Bucket value assigned to documents missing `field`."""

    collect_mode: str
    """Optional. `breadth_first` or `depth_first` sub-aggregation collection
    strategy."""

    execution_hint: str
    """Optional. Internal execution strategy hint (`map` /
    `global_ordinals`)."""

    format: str
    """Optional. Format applied to the bucket key in the response."""

    value_type: str
    """Optional. Explicit value type for the field when it cannot be
    inferred."""


class HistogramAggregation(KeyedBucketOption, HistogramBoundsOptions, total=False):
    """A [`histogram`](https://docs.opensearch.org/latest/aggregations/bucket/histogram/)
    bucket aggregation over a numeric column. Must specify `extended_bounds` or
    `hard_bounds` so the bucket count is bounded."""

    field: str
    """Required. The numeric column to bucket."""

    interval: float
    """Required. Bucket width. Must be positive."""

    min_doc_count: int
    """Optional. Minimum document count for a bucket to be returned."""

    offset: float
    """Optional. Shifts bucket boundaries by this amount."""

    order: BucketOrder
    """Optional. Bucket sort order -- a `{metric: "asc|desc"}` object or an
    array of them."""

    missing: ScalarValue
    """Optional. Bucket value assigned to documents missing `field`."""

    format: str
    """Optional. Format applied to the bucket key in the response."""


class DateHistogramAggregation(KeyedBucketOption, HistogramBoundsOptions, total=False):
    """A [`date_histogram`](https://docs.opensearch.org/latest/aggregations/bucket/date-histogram/)
    bucket aggregation over a date column. Must specify `extended_bounds` or
    `hard_bounds` so the bucket count is bounded."""

    field: str
    """Required. The date column to bucket."""

    calendar_interval: str
    """Optional. Calendar-aware interval (e.g. `month`, `year`). Mutually
    exclusive with `fixed_interval`."""

    fixed_interval: str
    """Optional. Fixed-duration interval (e.g. `30d`, `12h`). Mutually
    exclusive with `calendar_interval`."""

    interval: str
    """Optional. Legacy interval (use `calendar_interval` / `fixed_interval`
    instead)."""

    min_doc_count: int
    """Optional. Minimum document count for a bucket to be returned."""

    offset: str
    """Optional. Shifts bucket boundaries by this duration."""

    time_zone: str
    """Optional. UTC offset or IANA zone used to compute bucket boundaries."""

    order: BucketOrder
    """Optional. Bucket sort order -- a `{metric: "asc|desc"}` object or an
    array of them."""

    missing: ScalarValue
    """Optional. Bucket value assigned to documents missing `field`."""

    format: str
    """Optional. Date format applied to the bucket key in the response."""


class RangeAggregation(KeyedBucketOption, total=False):
    """A [`range`](https://docs.opensearch.org/latest/aggregations/bucket/range/)
    bucket aggregation -- one bucket per caller-defined numeric range."""

    field: str
    """Required. The numeric column to bucket."""

    ranges: List[Dict[str, Any]]
    """Required. The [bucket ranges](https://docs.opensearch.org/latest/aggregations/bucket/range/),
    each `{"from": <lower>, "to": <upper>, "key": "<label>"}` covering
    `[from, to)`. At least one of `from` / `to` is required per entry. Untyped
    because `from` is a Python keyword and cannot be a `TypedDict` field."""

    missing: ScalarValue
    """Optional. Value assigned to documents missing `field`."""

    format: str
    """Optional. Format applied to the bucket key in the response."""


class DateRangeAggregation(KeyedBucketOption, total=False):
    """A [`date_range`](https://docs.opensearch.org/latest/aggregations/bucket/date-range/)
    bucket aggregation -- one bucket per caller-defined date range."""

    field: str
    """Required. The date column to bucket."""

    ranges: List[Dict[str, Any]]
    """Required. The [bucket ranges](https://docs.opensearch.org/latest/aggregations/bucket/date-range/),
    each `{"from": <lower>, "to": <upper>, "key": "<label>"}`. Bound values may
    be dates or date-math expressions. Untyped because `from` is a Python
    keyword and cannot be a `TypedDict` field."""

    time_zone: str
    """Optional. UTC offset or IANA zone used to interpret the range bounds."""

    missing: ScalarValue
    """Optional. Value assigned to documents missing `field`."""

    format: str
    """Optional. Date format applied to the bucket key in the response."""


class MissingAggregation(TypedDict, total=False):
    """A [`missing`](https://docs.opensearch.org/latest/aggregations/bucket/missing/)
    bucket aggregation -- a single bucket of documents that have no value for
    `field`."""

    field: str
    """Required. The column whose missing values are bucketed."""

    missing: ScalarValue
    """Optional. Placeholder value treated as present (so those documents are
    excluded from the missing bucket)."""


class MinAggregation(MetricAggregation, MissingValueOption, total=False):
    """A [`min`](https://docs.opensearch.org/latest/aggregations/metric/minimum/)
    metric aggregation -- the minimum value of a numeric column."""

    value_type: str
    """Optional. Explicit value type for the field when it cannot be
    inferred."""


class MaxAggregation(MetricAggregation, MissingValueOption, total=False):
    """A [`max`](https://docs.opensearch.org/latest/aggregations/metric/maximum/)
    metric aggregation -- the maximum value of a numeric column."""

    value_type: str
    """Optional. Explicit value type for the field when it cannot be
    inferred."""


class AvgAggregation(MetricAggregation, MissingValueOption, total=False):
    """An [`avg`](https://docs.opensearch.org/latest/aggregations/metric/average/)
    metric aggregation -- the mean value of a numeric column."""

    value_type: str
    """Optional. Explicit value type for the field when it cannot be
    inferred."""


class SumAggregation(MetricAggregation, MissingValueOption, total=False):
    """A [`sum`](https://docs.opensearch.org/latest/aggregations/metric/sum/)
    metric aggregation -- the sum of a numeric column."""


class StatsAggregation(MissingValueOption, total=False):
    """A [`stats`](https://docs.opensearch.org/latest/aggregations/metric/stats/)
    metric aggregation -- count, min, max, avg, and sum of a numeric column in
    one pass."""

    field: str
    """Required. The numeric column to aggregate."""

    format: str
    """Optional. Format applied to the result values."""


class ExtendedStatsAggregation(MissingValueOption, total=False):
    """An [`extended_stats`](https://docs.opensearch.org/latest/aggregations/metric/extended-stats/)
    metric aggregation -- `stats` plus variance, standard deviation, and
    standard-deviation bounds."""

    field: str
    """Required. The numeric column to aggregate."""

    sigma: float
    """Optional. Number of standard deviations for the std-deviation bounds.
    Default `2.0`."""

    format: str
    """Optional. Format applied to the result values."""


class ValueCountAggregation(MissingValueOption, total=False):
    """A [`value_count`](https://docs.opensearch.org/latest/aggregations/metric/value-count/)
    metric aggregation -- the number of values extracted for a column."""

    field: str
    """Required. The column to count values of."""

    format: str
    """Optional. Format applied to the result value."""


class CardinalityAggregation(MissingValueOption, total=False):
    """A [`cardinality`](https://docs.opensearch.org/latest/aggregations/metric/cardinality/)
    metric aggregation -- an approximate distinct-value count of a column."""

    field: str
    """Required. The column whose distinct values are counted."""

    precision_threshold: int
    """Optional. Count below which the result is near-exact, trading memory for
    accuracy. Capped server-side."""

    execution_hint: str
    """Optional. Internal execution strategy hint."""


class FiltersAggregation(TypedDict, total=False):
    """A [`filters`](https://docs.opensearch.org/latest/aggregations/bucket/filters/)
    multi-bucket aggregation. Each entry of `filters` is a named bucket whose
    documents match its [Query][synapseclient.models.search_dsl.Query]. The
    named (keyed) form is the supported contract; each query is validated
    identically to the top-level `query` and is scoped by it."""

    filters: Dict[str, Query]
    """Required. Named buckets, keyed by caller-chosen name; each value is a
    [Query][synapseclient.models.search_dsl.Query] selecting that bucket's
    documents."""

    other_bucket: bool
    """Optional. When `true`, adds a bucket for documents that match none of
    the named filters."""

    other_bucket_key: str
    """Optional. The key under which the other bucket is returned."""

    keyed: bool
    """Optional. Whether buckets are returned as a keyed object (default)
    rather than an array."""


class Aggregation(TypedDict, total=False):
    """A single [OpenSearch aggregation](https://docs.opensearch.org/latest/aggregations/)
    definition. Exactly one of the aggregation-kind keys below may be set;
    `aggregations` may additionally carry nested sub-aggregations.

    The `filter` and `filters` kinds wrap a
    [Query][synapseclient.models.search_dsl.Query]; that query is validated
    identically to the top-level `query` and is scoped by it, so a `filter`
    aggregation never counts documents outside the top-level query.
    """

    terms: TermsAggregation
    """A [`terms`](https://docs.opensearch.org/latest/aggregations/bucket/terms/)
    bucket aggregation."""

    histogram: HistogramAggregation
    """A [`histogram`](https://docs.opensearch.org/latest/aggregations/bucket/histogram/)
    bucket aggregation."""

    date_histogram: DateHistogramAggregation
    """A [`date_histogram`](https://docs.opensearch.org/latest/aggregations/bucket/date-histogram/)
    bucket aggregation."""

    range: RangeAggregation
    """A [`range`](https://docs.opensearch.org/latest/aggregations/bucket/range/)
    bucket aggregation."""

    date_range: DateRangeAggregation
    """A [`date_range`](https://docs.opensearch.org/latest/aggregations/bucket/date-range/)
    bucket aggregation."""

    missing: MissingAggregation
    """A [`missing`](https://docs.opensearch.org/latest/aggregations/bucket/missing/)
    bucket aggregation."""

    min: MinAggregation
    """A [`min`](https://docs.opensearch.org/latest/aggregations/metric/minimum/)
    metric aggregation."""

    max: MaxAggregation
    """A [`max`](https://docs.opensearch.org/latest/aggregations/metric/maximum/)
    metric aggregation."""

    avg: AvgAggregation
    """An [`avg`](https://docs.opensearch.org/latest/aggregations/metric/average/)
    metric aggregation."""

    sum: SumAggregation
    """A [`sum`](https://docs.opensearch.org/latest/aggregations/metric/sum/)
    metric aggregation."""

    stats: StatsAggregation
    """A [`stats`](https://docs.opensearch.org/latest/aggregations/metric/stats/)
    metric aggregation."""

    extended_stats: ExtendedStatsAggregation
    """An [`extended_stats`](https://docs.opensearch.org/latest/aggregations/metric/extended-stats/)
    metric aggregation."""

    value_count: ValueCountAggregation
    """A [`value_count`](https://docs.opensearch.org/latest/aggregations/metric/value-count/)
    metric aggregation."""

    cardinality: CardinalityAggregation
    """A [`cardinality`](https://docs.opensearch.org/latest/aggregations/metric/cardinality/)
    metric aggregation."""

    filter: Query
    """A [`filter`](https://docs.opensearch.org/latest/aggregations/bucket/filter/)
    single-bucket aggregation. The body is a
    [Query][synapseclient.models.search_dsl.Query] -- validated like the
    top-level `query` -- that narrows the documents the nested `aggregations`
    see, within the top-level query scope."""

    filters: FiltersAggregation
    """A [`filters`](https://docs.opensearch.org/latest/aggregations/bucket/filters/)
    multi-bucket aggregation -- one named bucket per query."""

    aggregations: Dict[str, "Aggregation"]
    """Optional. Nested sub-aggregations, keyed by caller-chosen name. Bucket
    aggregations compute these once per bucket."""


class HighlightCommonOptions(TypedDict, total=False):
    """[Highlight](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/)
    options shared by the top-level highlight block and the per-field highlight
    overrides."""

    fragment_size: int
    """Optional. Maximum characters per highlighted fragment. Capped
    server-side."""

    fragment_offset: int
    """Optional. Character offset at which to start highlighting (`fvh`
    only)."""

    no_match_size: int
    """Optional. Number of leading characters to return when there is no
    match."""

    order: str
    """Optional. Fragment ordering: `none` (default) or `score`."""

    fragmenter: str
    """Optional. Fragmentation strategy: `simple` or `span` (plain
    highlighter)."""

    boundary_scanner: str
    """Optional. Boundary detection: `chars`, `sentence`, or `word`."""

    boundary_scanner_locale: str
    """Optional. Locale used by the boundary scanner."""

    boundary_chars: str
    """Optional. Characters treated as boundaries by the `chars` scanner."""

    boundary_max_scan: int
    """Optional. How far the boundary scanner looks for a boundary."""

    max_fragment_length: int
    """Optional. Maximum length of a fragment."""

    max_analyzer_offset: int
    """Optional. Maximum character offset analyzed for highlighting."""

    phrase_limit: int
    """Optional. Maximum number of matching phrases considered (`fvh`
    only)."""

    require_field_match: bool
    """Optional. Whether only fields that matched the query are highlighted.
    Default `true`."""

    highlight_filter: bool
    """Optional. Whether to highlight only fields that passed the query
    filter."""

    force_source: bool
    """Optional. Whether to highlight from the original `_source` rather than
    stored fields."""

    tags_schema: str
    """Optional. Built-in tag schema (`styled`) for the highlight markup."""


class HighlightField(HighlightCommonOptions, total=False):
    """Per-field [highlight](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/)
    options, carried as the value of a
    [Highlight][synapseclient.models.search_dsl.Highlight] `fields` entry (the
    map key is the column name). Any option set here overrides the top-level
    highlight option for this field."""

    type: str
    """Optional. Highlighter implementation: `unified` (default), `plain`, or
    `fvh`. The `semantic` highlighter is rejected."""

    number_of_fragments: int
    """Optional. Maximum number of fragments to return for this field. Capped
    server-side."""

    pre_tags: List[str]
    """Optional. Opening tags wrapped around highlighted terms."""

    post_tags: List[str]
    """Optional. Closing tags wrapped around highlighted terms."""

    matched_fields: List[str]
    """Optional. Other fields whose matches also highlight this field (`fvh`
    only)."""

    highlight_query: Query
    """Optional. A separate query used to select the terms to highlight for
    this field."""


class Highlight(HighlightCommonOptions, total=False):
    """A [highlight](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/)
    block. Adds matched-term snippet fragments to each hit. Top-level options
    apply to every highlighted field unless overridden in a per-field block
    under `fields`."""

    fields: Dict[str, HighlightField]
    """Required. The columns to highlight, keyed by column name; each value is
    its per-field option overrides."""

    type: str
    """Optional. Default highlighter implementation: `unified` (default),
    `plain`, or `fvh`. The `semantic` highlighter is rejected."""

    number_of_fragments: int
    """Optional. Maximum number of fragments per field. Capped server-side."""

    encoder: str
    """Optional. How highlighted text is encoded: `default` or `html`."""

    pre_tags: List[str]
    """Optional. Opening tags wrapped around highlighted terms. Default
    `<em>`."""

    post_tags: List[str]
    """Optional. Closing tags wrapped around highlighted terms. Default
    `</em>`."""

    highlight_query: Query
    """Optional. A separate query used to select the terms to highlight across
    all fields."""


class SourceFilter(TypedDict, total=False):
    """A [source filter](https://docs.opensearch.org/latest/search-plugins/searching-data/retrieve-specific-fields/)
    selecting which columns are returned on each hit. Carried as the
    [SearchQuery][synapseclient.models.SearchQuery] `source` field. Only this
    typed `{includes, excludes}` form is accepted -- the boolean (`true` /
    `false`) and bare-array shorthands are not."""

    includes: List[str]
    """Optional. Columns to include. When empty or absent, all columns are
    included (subject to `excludes`)."""

    excludes: List[str]
    """Optional. Columns to exclude. Applied after `includes`."""


class FieldCollapse(TypedDict, total=False):
    """A [collapse](https://docs.opensearch.org/latest/search-plugins/searching-data/collapse-search/)
    block. Returns only the top hit per distinct value of `field`,
    deduplicating the result list."""

    field: str
    """Required. The column to collapse on. Must be a keyword / doc-values
    column."""

    max_concurrent_group_searches: int
    """Optional. Maximum concurrent searches run to expand groups. Capped
    server-side."""


class RescoreQuery(TypedDict, total=False):
    """The secondary-scoring portion of a
    [rescore](https://docs.opensearch.org/latest/query-dsl/rescore/) stage: a
    query whose score is blended with the original score."""

    rescore_query: Query
    """Required. The query used to re-score the top window of hits."""

    query_weight: float
    """Optional. Weight applied to the original query score. Default `1.0`."""

    rescore_query_weight: float
    """Optional. Weight applied to the rescore-query score. Default `1.0`."""

    score_mode: str
    """Optional. How the two scores combine: `total` (default), `multiply`,
    `avg`, `max`, or `min`."""


class Rescore(TypedDict, total=False):
    """A [rescore](https://docs.opensearch.org/latest/query-dsl/rescore/) stage.
    Re-ranks the top `window_size` hits from the main query using a secondary
    scoring query. A single stage is supported."""

    window_size: int
    """Optional. Number of top hits per shard that are re-scored. Capped
    server-side."""

    query: RescoreQuery
    """Required. The secondary scoring query and how its score blends with the
    original."""
