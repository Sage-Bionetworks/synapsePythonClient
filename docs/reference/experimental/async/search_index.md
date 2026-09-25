# SearchIndex

## API reference

::: synapseclient.models.SearchIndex
    options:
        inherited_members: true
        members:
            - store_async
            - get_async
            - delete_async
            - query_async
            - autocomplete_async
            - get_permissions_async
            - get_acl_async
            - set_permissions_async
            - delete_permissions_async
            - list_acl_async

## Supporting types

::: synapseclient.models.SearchQuery
::: synapseclient.models.SearchQueryPart
::: synapseclient.models.SearchIndexQuery
::: synapseclient.models.SearchAutocompleteRequest
::: synapseclient.models.SearchHit
::: synapseclient.models.SearchFieldValue
::: synapseclient.models.SearchHighlight

## OpenSearch query DSL

::: synapseclient.models.search_dsl.Query

### Leaf clause field options

::: synapseclient.models.search_dsl.MatchFieldOptions
::: synapseclient.models.search_dsl.MatchPhraseFieldOptions
::: synapseclient.models.search_dsl.MatchPhrasePrefixFieldOptions
::: synapseclient.models.search_dsl.MatchBoolPrefixFieldOptions
::: synapseclient.models.search_dsl.TermFieldOptions
::: synapseclient.models.search_dsl.RangeFieldOptions
::: synapseclient.models.search_dsl.PrefixFieldOptions
::: synapseclient.models.search_dsl.WildcardFieldOptions
::: synapseclient.models.search_dsl.FuzzyFieldOptions
::: synapseclient.models.search_dsl.ExistsQuery
::: synapseclient.models.search_dsl.MultiMatchQuery
::: synapseclient.models.search_dsl.SimpleQueryStringQuery
::: synapseclient.models.search_dsl.MatchAllQuery

### Compound clauses

::: synapseclient.models.search_dsl.BoolQuery
::: synapseclient.models.search_dsl.DisMaxQuery
::: synapseclient.models.search_dsl.ConstantScoreQuery
::: synapseclient.models.search_dsl.BoostingQuery

### Shared per-field option mixins

::: synapseclient.models.search_dsl.ClauseScoringOptions
::: synapseclient.models.search_dsl.FuzzyMatchOptions
::: synapseclient.models.search_dsl.MinimumShouldMatchOption
::: synapseclient.models.search_dsl.ZeroTermsQueryOption

### Aggregations

::: synapseclient.models.search_dsl.Aggregation
::: synapseclient.models.search_dsl.TermsAggregation
::: synapseclient.models.search_dsl.HistogramAggregation
::: synapseclient.models.search_dsl.DateHistogramAggregation
::: synapseclient.models.search_dsl.RangeAggregation
::: synapseclient.models.search_dsl.DateRangeAggregation
::: synapseclient.models.search_dsl.MissingAggregation
::: synapseclient.models.search_dsl.MinAggregation
::: synapseclient.models.search_dsl.MaxAggregation
::: synapseclient.models.search_dsl.AvgAggregation
::: synapseclient.models.search_dsl.SumAggregation
::: synapseclient.models.search_dsl.StatsAggregation
::: synapseclient.models.search_dsl.ExtendedStatsAggregation
::: synapseclient.models.search_dsl.ValueCountAggregation
::: synapseclient.models.search_dsl.CardinalityAggregation
::: synapseclient.models.search_dsl.FiltersAggregation
::: synapseclient.models.search_dsl.ExtendedBounds
::: synapseclient.models.search_dsl.HistogramBoundsOptions
::: synapseclient.models.search_dsl.KeyedBucketOption
::: synapseclient.models.search_dsl.MetricAggregation
::: synapseclient.models.search_dsl.MissingValueOption

### Highlighting, source filtering, collapse, and rescore

::: synapseclient.models.search_dsl.Highlight
::: synapseclient.models.search_dsl.HighlightField
::: synapseclient.models.search_dsl.HighlightCommonOptions
::: synapseclient.models.search_dsl.SourceFilter
::: synapseclient.models.search_dsl.FieldCollapse
::: synapseclient.models.search_dsl.Rescore
::: synapseclient.models.search_dsl.RescoreQuery
