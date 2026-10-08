# Semantic and Hybrid Search

The [Search Index tutorial](search.md) matches the *words* someone types. That works
until the words don't line up: someone searching for "memory problems in older adults"
wants the Alzheimer's studies, but none of those abstracts use the words "memory",
"older", or "adults", so a keyword search finds nothing.

**Semantic search** matches on *meaning* instead of exact words. When the index is
built, Synapse reads the text of each row and records what it is about. A search then
returns the rows whose meaning is closest to what was asked.

**Hybrid search** runs a keyword search and a semantic search together and blends them
into one ranked list. Keywords keep results precise; meaning catches what keywords
miss. A **search pipeline** controls the blend.

## Tutorial Purpose
In this tutorial, you will:

1. Create a table of study summaries to search over
2. Turn on semantic search for a column
3. Search by meaning
4. Blend keyword and semantic results with a hybrid search
5. Choose how much each side counts with weights
6. Save your weighting as a reusable search pipeline, and make it the default
7. Narrow a hybrid search with a filter

## Prerequisites
* This tutorial assumes that you have a Synapse project.
* Pandas must also be installed as shown in the [installation documentation](../installation.md).
* Familiarity with the [Search Index tutorial](search.md) is helpful, but not required.

## 1. Create a table to search over

We use the same six Alzheimer's disease study summaries as the
[Search Index tutorial](search.md). Replace
`"My uniquely named project about Alzheimer's Disease"` with the name of your project.

`print_hits` prints each hit's score next to its study name, so you can see how the
ranking changes through the tutorial.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:setup"
```

## 2. Turn on semantic search for a column

!!! warning "Restricted to Sage Bionetworks employees"
    Creating a SearchIndex and the Organization resources below is restricted to Sage
    Bionetworks employees. If that is not you, skip to step 3 and use an index that
    someone has already set up and shared with you.

!!! warning "Permanent"
    The ColumnAnalyzerOverride, SearchConfiguration and NamedSearchPipeline created in
    this tutorial cannot be deleted, and neither can the Organization that owns them.
    Choose names deliberately. Replace `"my.uniquely.named.organization"` with your own.

Semantic search is turned on one column at a time, by setting `semantic=True` for that
column in a [ColumnAnalyzerOverride][synapseclient.models.ColumnAnalyzerOverride]. A
[SearchConfiguration][synapseclient.models.SearchConfiguration] bundles the override,
and the SearchIndex points at the configuration.

These resources can't be deleted and their names must be unique, so `save()` updates
the existing resource with the same name instead of creating a second one. That makes
the script safe to run again.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:enable_semantic"
```

<details class="example">
  <summary>Creating the index should look like:</summary>

```
Created SearchIndex with ID: syn68123456
Index syn68123456 is ready with 6 rows
```
</details>

Before you pick columns:

* **Text columns only.** STRING, STRING_LIST, MEDIUMTEXT, LARGETEXT, LINK and JSON
  columns can be flagged. Flagging any other type makes the index build fail.
* **Up to 50,000 rows.** The build fails if the source has more rows than that.
* **Long text is cut off for meaning.** All of a row's flagged columns are combined,
  and only about the first 8,000 tokens count toward its meaning. The full text is
  still stored, returned, and searchable by keyword.

## 3. Search by meaning

A semantic search is a `neural` clause inside a
[HybridQuery][synapseclient.models.search_dsl.HybridQuery]. Give it plain text in
`query_text`; `k` is how many of the closest rows to return. `semantic_search` is the
only name a `neural` clause accepts.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:semantic_search"
```

<details class="example">
  <summary>The result of your semantic search should look like:</summary>

```
Closest in meaning to 'memory problems in older adults':
  1.00  ROSMAP Cortex Proteomics
  0.23  Mayo Clinic Whole Genome
  0.00  MCI Plasma Biomarkers
```
</details>

All three are about dementia or cognitive decline, and none of them contain a word
from the query. The Parkinson's study and the healthy aging atlas are not among them.

!!! note "Scores are relative"
    Scores are rescaled within each result set, so the best hit always scores 1.00 and
    the weakest returned hit scores 0.00. A score of 0.00 here does not mean
    "unrelated": MCI Plasma Biomarkers is still the third-closest of the six studies.
    Compare scores within one search, never between searches.

## 4. Blend keyword and semantic results

A hybrid search lists up to five clauses in `queries`, each scored on its own. A row
only has to match one clause to be returned.

Here a keyword search for "sequencing" and a semantic search for "forgetfulness and
dementia" pull in different directions:

* The keyword clause favors the healthy aging atlas, the best "sequencing" match, even
  though it has nothing to do with dementia.
* The meaning clause favors MCI Plasma Biomarkers, the closest match to "forgetfulness
  and dementia", even though it never mentions sequencing.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:hybrid_search"
```

<details class="example">
  <summary>The result of your hybrid search should look like:</summary>

```
'sequencing' + 'forgetfulness and dementia', weighted equally:
  0.50  Healthy Aging Single Cell Atlas
  0.50  MCI Plasma Biomarkers
  0.48  ROSMAP Cortex Proteomics
```
</details>

With no pipeline given, both clauses count equally. Each of the top two wins one clause
outright and gets nothing from the other, so they tie at 0.50.

## 5. Choose how much each side counts

The blend is controlled by a **search pipeline**. Its `weights` list gives each clause
a share, in the same order as `queries`, and the shares add up to 1. `[0.8, 0.2]` means
80% keyword and 20% meaning.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:weighting"
```

<details class="example">
  <summary>The result of your weighted searches should look like:</summary>

```
Keyword first (80/20):
  0.80  Healthy Aging Single Cell Atlas
  0.20  MCI Plasma Biomarkers
  0.19  ROSMAP Cortex Proteomics
Meaning first (20/80):
  0.80  MCI Plasma Biomarkers
  0.77  ROSMAP Cortex Proteomics
  0.58  Mayo Clinic Whole Genome
```
</details>

The weights break the tie. Keyword-first puts the sequencing atlas on top. Meaning-first
puts the dementia studies on top and drops the atlas out of the first three.

!!! tip "Which settings should I change?"
    `min_max` and `arithmetic_mean` are the defaults and a good starting point; most of
    the time only `weights` needs to change. The other options are described on
    [Normalization][synapseclient.models.search_dsl.Normalization] and
    [Combination][synapseclient.models.search_dsl.Combination].

## 6. Save your weighting as a reusable search pipeline

Rather than repeating a pipeline in every query, save it once as a
[NamedSearchPipeline][synapseclient.models.NamedSearchPipeline] and refer to it by its
qualified name, `{"$ref": "my.uniquely.named.organization-meaning_first"}`. You can
also make it the default for an index, so hybrid searches use it without asking.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:named_pipeline"
```

<details class="example">
  <summary>The result of using your saved pipeline should look like:</summary>

```
Saved search pipeline: my.uniquely.named.organization-meaning_first
Using the saved pipeline by name:
  0.80  MCI Plasma Biomarkers
  0.77  ROSMAP Cortex Proteomics
  0.58  Mayo Clinic Whole Genome
No pipeline on the query, so the default is used:
  0.80  MCI Plasma Biomarkers
  0.77  ROSMAP Cortex Proteomics
  0.58  Mayo Clinic Whole Genome
```
</details>

The last search passes no pipeline but still ranks meaning first, because the
configuration now defaults to `meaning_first`. The new default applies to the existing
index straight away; it does not need to be rebuilt.

A hybrid search uses the first pipeline it finds:

1. The `search_pipeline` on the query
2. The `default_search_pipeline` on the index's SearchConfiguration
3. The built-in default: every clause weighted equally

A saved pipeline is checked when it is stored:

* `weights` must hold 2 to 5 entries, each between 0 and 1, that add up to 1.
* A query with fewer clauses than weights uses the first weights, in order, and
  rescales them to add up to 1. A query with more clauses than weights is rejected.
* A `$ref` to a pipeline that doesn't exist is rejected.

A pipeline written inline on a query is not checked this strictly: Synapse rescales its
weights to add up to 1 whatever you pass.

## 7. Narrow a hybrid search with a filter

To limit a hybrid search to certain rows, put the condition on `HybridQuery.filter`. It
is applied before the closest rows are picked and does not change their scores.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py:filtering"
```

<details class="example">
  <summary>The result of your filtered search should look like:</summary>

```
'forgetfulness and dementia', Alzheimer's studies only:
  1.00  ROSMAP Cortex Proteomics
  0.38  Mayo Clinic Whole Genome
  0.00  MSBB RNA Sequencing
```
</details>

MCI Plasma Biomarkers was the closest study in step 4, but it is not an Alzheimer's
study, so the filter removed it before ranking.

!!! tip "Use `HybridQuery.filter`, not `post_filter`"
    `post_filter` runs *after* the semantic clause has picked its `k` closest rows, so
    it can leave you with few or no results. `HybridQuery.filter` narrows the rows
    first, then searches within them.

## Good to know

* **One of `query` or `hybrid`.** A search sets exactly one of them, and
  `search_pipeline` is only accepted alongside `hybrid`.
* **One of `k`, `min_score`, or `max_distance`.** A `neural` clause may set at most one
  of these to decide how many close rows it returns. With none set, `k` defaults to
  10.
* **Paging stops at 1,000.** Hybrid searches page with `from_` and `size`, and
  `from_ + size` may not exceed 1,000. `search_after` only works when `sort` names a
  column.
* **No semantic column, no `neural` clause.** On an index with no column flagged
  `semantic=True`, `neural` clauses are skipped and the keyword clauses still run. A
  search made only of `neural` clauses is rejected.

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
--8<-- "docs/tutorials/python/tutorial_scripts/semantic_search.py"
```
</details>

## References
- [SearchIndex][synapseclient.models.SearchIndex]
- [SearchQuery][synapseclient.models.SearchQuery]
- [HybridQuery][synapseclient.models.search_dsl.HybridQuery]
- [HybridClause][synapseclient.models.search_dsl.HybridClause]
- [NeuralFieldOptions][synapseclient.models.search_dsl.NeuralFieldOptions]
- [SearchPipeline][synapseclient.models.search_dsl.SearchPipeline]
- [CombinationParameters][synapseclient.models.search_dsl.CombinationParameters]
- [NamedSearchPipeline][synapseclient.models.NamedSearchPipeline]
- [SearchConfiguration][synapseclient.models.SearchConfiguration]
- [ColumnAnalyzerOverride][synapseclient.models.ColumnAnalyzerOverride]
- [ColumnAnalyzerOverrideEntry][synapseclient.models.ColumnAnalyzerOverrideEntry]
- [Organization][synapseclient.models.Organization]
- [Search Index tutorial](search.md)
- [OpenSearch hybrid search](https://docs.opensearch.org/latest/vector-search/ai-search/hybrid-search/index/)
