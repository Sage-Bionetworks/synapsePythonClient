# Search Indexes

A [SearchIndex][synapseclient.models.SearchIndex] is a Synapse entity whose content is
defined by a Synapse SQL query. Synapse builds an OpenSearch index from the rows that
query returns, which gives you full-text search, relevance ranking, faceting, and
autocomplete over a table or view.

This is a different way of asking questions than a
[Table](table.md) or a [Materialized View](materializedview.md). A table is queried with
Synapse SQL and answers "which rows match these exact conditions?". A search index is
queried with the
[OpenSearch Query DSL](https://docs.opensearch.org/latest/query-dsl/) and answers
"which rows are most relevant to this text?" — matching word stems, ignoring
punctuation and case, ranking the best matches first, and counting how many rows fall
into each category. It is what you would put behind a search box.

This tutorial will walk you through creating a search index and querying it with the
Synapse Python client.

## Tutorial Purpose
In this tutorial, you will:

1. Log in, get your project, and create a table to index
2. Create a SearchIndex
3. Run a full-text search
4. Highlight where the match happened
5. Combine scored clauses with unscored filters, and sort the results
6. Count facets with aggregations
7. Power a type-ahead box with autocomplete
8. Page through results
9. Tune matching with synonyms and analyzers

## Prerequisites
* This tutorial assumes that you have a Synapse project.
* Pandas must also be installed as shown in the [installation documentation](../installation.md).

## 1. Log in, get your project, and create a table to index

A search index is always defined over an existing table-like entity, so we first create
a small table of study summaries to search over.

You will want to replace `"My uniquely named project about Alzheimer's Disease"` with
the name of your project.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:setup"
```

The steps below use two small helpers — one to print the rows a query matched, and one
to wait out the index build.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:helpers"
```

## 2. Create a SearchIndex Entity

The `defining_sql` decides which rows and columns are indexed. It must reference exactly
one table-like entity — unlike a Materialized View, JOIN and UNION across several
entities are not supported. If you need to search across several tables, build a
[Materialized View](materializedview.md) first and index that.

Any of these can be the source, whichever one the SQL selects from:

* a [Table][synapseclient.models.Table]
* an [EntityView][synapseclient.models.EntityView]
* a [DatasetCollection][synapseclient.models.DatasetCollection]
* a [MaterializedView][synapseclient.models.MaterializedView]

The SQL can pin a specific version of the source (`SELECT * FROM syn12345.7`), select a
subset of columns, and carry `WHERE`, `ORDER BY`, and `LIMIT` clauses — only those rows
and columns end up in the index.

Storing the search index entity returns as soon as Synapse has accepted it, but the OpenSearch index
behind it is built in the background.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:create_index"
```

<details class="example">
  <summary>Creating the index should look like:</summary>

```
Created SearchIndex with ID: syn68123456
Waiting for the search index to build...
Waiting for the search index to build...
Index syn68123456 is queryable with 6 rows
```
</details>

**Note**: The index tracks its source. When rows in the underlying table change, the
index is updated in the background — you do not need to re-store the SearchIndex.

## 3. Run a full-text search

A [`match`](https://docs.opensearch.org/latest/query-dsl/full-text/match/) clause is the
workhorse of full-text search: the text you pass is analyzed the same way the column was
analyzed, so `"alzheimer"` matches `"Alzheimer's disease"`. Every clause kind Synapse
accepts is listed on [Query][synapseclient.models.search_dsl.Query].

By default a hit carries every indexed column. `source` narrows that down, and
`response_parts` asks for extras beyond the hits themselves — here the total hit count
and the columns each hit carries.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:full_text_search"
```

<details class="example">
  <summary>The results of your searches should look like:</summary>

```
Abstracts mentioning Alzheimer's:
columns: ['study_name', 'diagnosis']
total_hits=3, returned=3
  ROW_ID=1 {'study_name': 'ROSMAP Cortex Proteomics', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'diagnosis': "Alzheimer's Disease"}

Anything mentioning tau:
total_hits=1, returned=1
  ROW_ID=5 {'study_name': 'MCI Plasma Biomarkers'}
```
</details>

Hits come back ranked by relevance, and each one carries its score on
[`hit.score`][synapseclient.models.SearchHit] along with the `row_id` and `row_version`
of the source row.

## 4. Highlight where the match happened

A result list is much easier to read when it shows the matching text in context.
`highlight` returns short fragments of the matched columns with the matching terms
wrapped in `<em>` tags.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:columns_and_highlights"
```

<details class="example">
  <summary>The result of your highlighted search should look like:</summary>

```
Studies that sequenced something:
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'assay': 'rnaSeq'}
    abstract: ['Bulk RNA <em>sequencing</em> across four brain regions in a cohort']
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'assay': 'wholeGenomeSeq'}
    abstract: ['Whole genome <em>sequencing</em> of temporal cortex samples from']
  ROW_ID=4 {'study_name': 'Healthy Aging Single Cell Atlas', 'assay': 'snrnaSeq'}
    abstract: ['Single nucleus RNA <em>sequencing</em> of hippocampus from']
```
</details>

**Note**: Highlighting, like relevance scoring, depends on the column being indexed as
analyzed text. Step 9 covers how to control that with a
[SearchConfiguration][synapseclient.models.SearchConfiguration].

## 5. Combine scored clauses with unscored filters, and sort the results

A [`bool`](https://docs.opensearch.org/latest/query-dsl/compound/bool/) clause is how
you build a real search request out of several conditions:

* `must` clauses have to match and **do** contribute to the relevance score
* `filter` and `must_not` clauses have to match (or not match) but **do not** affect
  the score — use these for hard constraints like a numeric cutoff
* `should` clauses boost the rows that match them without excluding the rows that don't

Passing `sort` replaces relevance ranking with an ordering of your choosing. Only column
and `_score` sorts are accepted.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:filters_and_sorting"
```

<details class="example">
  <summary>The result of your filtered search should look like:</summary>

```
Sequencing studies with at least 200 participants, largest first:
total_hits=2, returned=2
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'participant_count': '350'}
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'participant_count': '300'}
```
</details>

## 6. Count facets with aggregations

Aggregations answer "how many rows are there of each kind?" — the counts you see next to
the checkboxes in a faceted search UI. A
[`terms`](https://docs.opensearch.org/latest/aggregations/bucket/terms/) aggregation
produces one bucket per distinct value of a column; metric aggregations like `avg` and
`stats` summarize a numeric column. Results come back on `aggregation_results` as the
raw OpenSearch response, with field references rewritten back to your column names.

`post_filter` is what keeps a facet list usable: it narrows the hits *after* the
aggregations have been computed, so selecting one diagnosis does not make the other
diagnosis counts disappear.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:aggregations"
```

<details class="example">
  <summary>The result of your faceted search should look like:</summary>

```
Hits after the post filter:
total_hits=3, returned=3
  ROW_ID=1 {'study_name': 'ROSMAP Cortex Proteomics', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'diagnosis': "Alzheimer's Disease"}

Facet counts across all studies:
{
  "by_diagnosis": {
    "doc_count_error_upper_bound": 0,
    "sum_other_doc_count": 0,
    "buckets": [
      {
        "key": "Alzheimer's Disease",
        "doc_count": 3
      },
      {
        "key": "Cognitively Normal",
        "doc_count": 1
      },
      {
        "key": "Mild Cognitive Impairment",
        "doc_count": 1
      },
      {
        "key": "Parkinson's Disease",
        "doc_count": 1
      }
    ]
  },
  "mean_cohort_size": {
    "value": 261.6666666666667
  }
}
```
</details>

## 7. Power a type-ahead box with autocomplete

[`autocomplete()`][synapseclient.models.SearchIndex.autocomplete] is a separate,
synchronous endpoint meant for search-as-you-type: it returns its hits directly instead
of going through the asynchronous job service, so it is fast enough to call on every
keystroke. In exchange, it only accepts prefix-style clauses — `prefix`,
`match_phrase_prefix`, or `match_bool_prefix` — and returns at most 8 hits.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:autocomplete"
```

<details class="example">
  <summary>The result of your autocomplete request should look like:</summary>
```
Suggestions for 'Mayo Cl':
  ['Mayo Clinic Whole Genome']
```
</details>

## 8. Pagination

A query returns at most 100 hits at a time (25 by default). `from_` and `size` walk
through the result set the way page numbers do.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:pagination"
```

<details class="example">
  <summary>The result of paging through your index should look like:</summary>
```
Page starting at offset 0:
total_hits=6, returned=2
  ROW_ID=1 {'study_name': 'ROSMAP Cortex Proteomics', 'participant_count': '400'}
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'participant_count': '350'}
Page starting at offset 2:
total_hits=6, returned=2
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'participant_count': '300'}
  ROW_ID=5 {'study_name': 'MCI Plasma Biomarkers', 'participant_count': '220'}
Page starting at offset 4:
total_hits=6, returned=2
  ROW_ID=6 {'study_name': 'Parkinson Comparative Cohort', 'participant_count': '180'}
  ROW_ID=4 {'study_name': 'Healthy Aging Single Cell Atlas', 'participant_count': '120'}
```
</details>

**Note**: Offset paging gets expensive deep into a large result set. For that case each
response carries a `next_search_after` cursor — pass it back unchanged as
`SearchQuery(search_after=...)` on the following request and leave `from_` unset.

## 9. Tune matching with synonyms and analyzers

Everything above relies on how each column was analyzed when the index was built: how
text is split into tokens, which tokens are dropped, and how they are normalized. Four
org-scoped resources let you control that:

* [SynonymSet][synapseclient.models.SynonymSet] — terms that should be treated as
  equivalent, so someone searching `AD` finds abstracts that say
  "Alzheimer's disease"
* [TextAnalyzer][synapseclient.models.TextAnalyzer] — a named OpenSearch analyzer: a
  tokenizer plus a chain of token filters, which may reference a SynonymSet
* [ColumnAnalyzerOverride][synapseclient.models.ColumnAnalyzerOverride] — a reusable
  bundle assigning specific analyzers to specific columns
* [SearchConfiguration][synapseclient.models.SearchConfiguration] — bundles a default
  analyzer with any column overrides; this is what a SearchIndex actually points at

Each resource belongs to an [Organization][synapseclient.models.Organization] and is
referenced from another resource by its qualified name,
`{organization_name}-{name}`, written as `{"$ref": "my.org-my_analyzer"}`.

!!! warning "Restricted and permanent"
    Creating and updating these resources is restricted to Sage Bionetworks employees,
    and the REST API has no delete endpoint for any of them. Once created, a
    SynonymSet, TextAnalyzer, ColumnAnalyzerOverride, or SearchConfiguration cannot be
    removed, and its owning Organization can no longer be deleted either. Choose names
    deliberately.

Note where the synonym filter goes below. The analyzer declares both a `default` chain,
used when rows are indexed, and a `default_search` chain, used when a query is analyzed.
Putting the synonyms only in `default_search` expands the incoming query instead of
storing every synonym for every row.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:search_configuration"
```

A SearchIndex resolves its configuration when the index is built, so set it up front.
Either point the index straight at a configuration with `search_configuration_id`, or
bind a configuration to the parent folder or project — an index with no
`search_configuration_id` of its own walks up the entity hierarchy and uses the first
[SearchConfigBinding][synapseclient.models.SearchConfigBinding] it finds, falling back
to the platform defaults.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py:apply_search_configuration"
```

<details class="example">
  <summary>Searching the abbreviation against the new index should look like:</summary>
```
Created SearchIndex syn68123457 using config 4321
Index syn68123457 is queryable with 6 rows
Bound configuration 4321 to syn12345678
Abstracts matching the abbreviation 'AD':
total_hits=3, returned=3
  ROW_ID=1 {'study_name': 'ROSMAP Cortex Proteomics', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=2 {'study_name': 'MSBB RNA Sequencing', 'diagnosis': "Alzheimer's Disease"}
  ROW_ID=3 {'study_name': 'Mayo Clinic Whole Genome', 'diagnosis': "Alzheimer's Disease"}
```
</details>

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
--8<-- "docs/tutorials/python/tutorial_scripts/search.py"
```
</details>

## References
- [SearchIndex][synapseclient.models.SearchIndex]
- [SearchQuery][synapseclient.models.SearchQuery]
- [SearchQueryPart][synapseclient.models.SearchQueryPart]
- [SearchIndexQuery][synapseclient.models.SearchIndexQuery]
- [SearchHit][synapseclient.models.SearchHit]
- [Query][synapseclient.models.search_dsl.Query]
- [SearchConfiguration][synapseclient.models.SearchConfiguration]
- [TextAnalyzer][synapseclient.models.TextAnalyzer]
- [SynonymSet][synapseclient.models.SynonymSet]
- [ColumnAnalyzerOverride][synapseclient.models.ColumnAnalyzerOverride]
- [SearchConfigBinding][synapseclient.models.SearchConfigBinding]
- [Organization][synapseclient.models.Organization]
- [Table][synapseclient.models.Table]
- [syn.login][synapseclient.Synapse.login]
- [OpenSearch query DSL](https://docs.opensearch.org/latest/query-dsl/)
- [OpenSearch aggregations](https://docs.opensearch.org/latest/aggregations/)
