"""Here is where you'll find the code for the SearchIndex tutorial."""

# --8<-- [start:setup]
import json

import pandas as pd

from synapseclient import Synapse
from synapseclient.models import (
    Column,
    ColumnType,
    Project,
    SearchIndex,
    SearchQuery,
    SearchQueryPart,
    Table,
)
from synapseclient.models.search_dsl import (
    Aggregation,
    AvgAggregation,
    BoolQuery,
    Highlight,
    HighlightField,
    MatchBoolPrefixFieldOptions,
    MatchFieldOptions,
    MatchPhraseFieldOptions,
    MultiMatchQuery,
    Query,
    RangeFieldOptions,
    SourceFilter,
    TermsAggregation,
)

# Initialize Synapse client
syn = Synapse()
syn.login()

# Get the project where we want to create the search index
project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = project.id
print(f"Got project with ID: {project_id}")

# Create the table that will be indexed
table = Table(
    name="Study Summaries",
    parent_id=project_id,
    columns=[
        Column(name="study_name", column_type=ColumnType.STRING),
        Column(name="abstract", column_type=ColumnType.LARGETEXT),
        Column(name="diagnosis", column_type=ColumnType.STRING),
        Column(name="assay", column_type=ColumnType.STRING),
        Column(name="participant_count", column_type=ColumnType.INTEGER),
    ],
).store()
print(f"Created table with ID: {table.id}")

# Add the rows we are going to search over
studies = pd.DataFrame(
    [
        {
            "study_name": "ROSMAP Cortex Proteomics",
            "abstract": "Quantitative proteomics of dorsolateral prefrontal cortex "
            "from donors with Alzheimer's disease and cognitively normal controls.",
            "diagnosis": "Alzheimer's Disease",
            "assay": "TMT quantitation",
            "participant_count": 400,
        },
        {
            "study_name": "MSBB RNA Sequencing",
            "abstract": "Bulk RNA sequencing across four brain regions in a cohort "
            "spanning the full range of Alzheimer's disease neuropathology.",
            "diagnosis": "Alzheimer's Disease",
            "assay": "rnaSeq",
            "participant_count": 300,
        },
        {
            "study_name": "Mayo Clinic Whole Genome",
            "abstract": "Whole genome sequencing of temporal cortex samples from "
            "donors with Alzheimer's disease, progressive supranuclear palsy, "
            "and controls.",
            "diagnosis": "Alzheimer's Disease",
            "assay": "wholeGenomeSeq",
            "participant_count": 350,
        },
        {
            "study_name": "Healthy Aging Single Cell Atlas",
            "abstract": "Single nucleus RNA sequencing of hippocampus from "
            "cognitively normal aged donors, establishing a baseline atlas.",
            "diagnosis": "Cognitively Normal",
            "assay": "snrnaSeq",
            "participant_count": 120,
        },
        {
            "study_name": "MCI Plasma Biomarkers",
            "abstract": "Plasma biomarker panel measuring phosphorylated tau and "
            "neurofilament light chain in mild cognitive impairment.",
            "diagnosis": "Mild Cognitive Impairment",
            "assay": "immunoassay",
            "participant_count": 220,
        },
        {
            "study_name": "Parkinson Comparative Cohort",
            "abstract": "Comparative transcriptomic profiling of substantia nigra "
            "in Parkinson disease versus age-matched controls.",
            "diagnosis": "Parkinson's Disease",
            "assay": "rnaSeq",
            "participant_count": 180,
        },
    ]
)
table.upsert_rows(values=studies, primary_keys=["study_name"])
print(f"Stored {len(studies)} rows in {table.id}")
# --8<-- [end:setup]

# --8<-- [start:create_index]
# Create a SearchIndex over a single table and wait for it to build.
index = SearchIndex(
    name="Study Summaries Search Index",
    description="Full text search over the study summary table",
    parent_id=project_id,
    # The defining SQL must reference exactly one table-like entity
    defining_sql=f"SELECT * FROM {table.id}",
)
index = index.store()
print(f"Created SearchIndex with ID: {index.id}")

# --8<-- [end:create_index]


# --8<-- [start:search_configuration]
def create_search_configuration() -> str:
    """
    Example: Teach the index that "AD" means "Alzheimer's disease" by building a
    SynonymSet, wrapping it in a TextAnalyzer, and bundling that analyzer into a
    SearchConfiguration.

    These resources belong to an Organization, and creating them is restricted
    to Sage Bionetworks employees. None of them can be deleted once created.
    """
    from synapseclient.models import (
        ColumnAnalyzerOverride,
        ColumnAnalyzerOverrideEntry,
        Organization,
        SearchConfiguration,
        SynonymSet,
        TextAnalyzer,
    )

    organization_name = "my.uniquely.named.organization"
    organization = Organization(name=organization_name).store()
    print(f"Using organization: {organization.id} ({organization.name})")

    # Comma-separated entries are interchangeable in both directions; entries
    # written with "=>" expand the left side to the right side only.
    #
    # Keep every entry lowercase. The `lowercase` filter runs before the synonym
    # filter in the chain below, so a search term is already lowercased by the
    # time the synonyms are applied -- an entry written "AD => ..." would never
    # match and never expand.
    synonyms = SynonymSet(
        organization_name=organization_name,
        name="ad_synonyms",
        description="Abbreviations used across Alzheimer's disease studies",
        definition={
            "type": "synonym_graph",
            "synonyms": [
                "rna sequencing, rna-seq, rnaseq",
                "ad => alzheimer's disease, alzheimers disease",
                "mci => mild cognitive impairment",
            ],
        },
    ).store()
    print(f"Created SynonymSet: {synonyms.id} ({synonyms.qualified_name})")

    # The synonym filter is applied in `default_search` only, so synonyms expand
    # the incoming query rather than bloating the stored index.
    analyzer = TextAnalyzer(
        organization_name=organization_name,
        name="ad_synonym_analyzer",
        description="English analyzer that expands AD abbreviations at search time",
        settings={
            "filter": {
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
                # A $ref resolves to the SynonymSet by its qualified name
                "ad_synonyms": {"$ref": synonyms.qualified_name},
            },
            "analyzer": {
                "default": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "english_stop", "english_stemmer"],
                },
                "default_search": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "ad_synonyms",
                        "english_stop",
                        "english_stemmer",
                    ],
                },
            },
        },
    ).store()
    print(f"Created TextAnalyzer: {analyzer.id} ({analyzer.qualified_name})")

    # Columns not named here fall back to the configuration's default analyzer
    overrides = ColumnAnalyzerOverride(
        organization_name=organization_name,
        name="study_column_overrides",
        description="Treat the diagnosis column as a single exact value",
        overrides=[
            ColumnAnalyzerOverrideEntry(
                column_name="diagnosis",
                analyzer={"analyzer": {"default": {"type": "keyword"}}},
            ),
        ],
    ).store()
    print(f"Created ColumnAnalyzerOverride: {overrides.id}")

    configuration = SearchConfiguration(
        organization_name=organization_name,
        name="study_search_config",
        description="Analyzer settings for the study summary search index",
        default_analyzer={"$ref": analyzer.qualified_name},
        column_analyzer_overrides=[{"$ref": overrides.qualified_name}],
    ).store()
    print(f"Created SearchConfiguration: {configuration.id}")
    return configuration.id


# --8<-- [end:search_configuration]


# --8<-- [start:apply_search_configuration]
def create_index_with_configuration(search_configuration_id: str) -> SearchIndex:
    """
    Example: Build an index that uses a specific SearchConfiguration, and bind
    the same configuration to the project so later indexes inherit it.
    """
    from synapseclient.models import SearchConfigBinding

    index = SearchIndex(
        name="Study Summaries Search Index With Synonyms",
        parent_id=project_id,
        defining_sql=f"SELECT * FROM {table.id}",
        search_configuration_id=search_configuration_id,
    ).store()
    print(f"Created SearchIndex {index.id} using config {search_configuration_id}")

    # Any index created under this project without its own
    # search_configuration_id now inherits this configuration
    binding = SearchConfigBinding(
        object_id=project_id,
        search_configuration_id=search_configuration_id,
    ).store()
    print(f"Bound configuration {binding.search_configuration_id} to {project_id}")

    # "AD" now matches the abstracts that spell out "Alzheimer's disease"
    results = index.query(
        search_query=SearchQuery(
            query=Query(match={"abstract": MatchFieldOptions(query="AD")}),
            source=SourceFilter(includes=["study_name", "diagnosis"]),
            size=10,
        ),
        response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
    )
    print("Abstracts matching the abbreviation 'AD':")
    print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
    for hit in results.hits:
        fields = {field.name: field.value for field in hit.fields}
        print(f"  {fields}")
    return index


# --8<-- [end:apply_search_configuration]


# --8<-- [start:full_text_search]
# Find every study whose abstract mentions Alzheimer's disease, then
# search across several columns at once.
results = index.query(
    search_query=SearchQuery(
        query=Query(match={"abstract": MatchFieldOptions(query="alzheimer")}),
        # Every indexed column comes back on each hit unless a source filter
        # narrows them down, and the abstracts are long
        source=SourceFilter(includes=["study_name", "diagnosis"]),
        size=10,
    ),
    response_parts=[
        SearchQueryPart.HITS,
        SearchQueryPart.TOTAL_HITS,
        SearchQueryPart.SELECT_COLUMNS,
    ],
)
print("Abstracts mentioning Alzheimer's:")
print(f"columns: {[column.name for column in results.select_columns]}")
print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")

# A multi_match clause runs the same text across several columns, so the
# person searching does not need to know which column holds the term.
results = index.query(
    search_query=SearchQuery(
        query=Query(
            multi_match=MultiMatchQuery(
                query="tau",
                # ^2 boosts a match in the study name over one in the abstract
                fields=["study_name^2", "abstract"],
            )
        ),
        source=SourceFilter(includes=["study_name"]),
        size=10,
    ),
    response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
)
print("\nAnything mentioning tau:")
print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")

# People misspell things. `fuzziness` tolerates a number of single-character
# edits -- insert, delete, substitute, or transpose -- between what was typed
# and what is in the index.
results = index.query(
    search_query=SearchQuery(
        query=Query(
            match={
                "abstract": MatchFieldOptions(
                    # "sequencing" with a missing "i"
                    query="sequencng",
                    # "AUTO" scales the allowance with term length: 0 edits for
                    # very short terms, 1 for medium, 2 for long ones
                    fuzziness="AUTO",
                    # The first 3 characters still have to be exact. Without
                    # this, short unrelated words start matching each other
                    prefix_length=3,
                )
            }
        ),
        source=SourceFilter(includes=["study_name"]),
        size=10,
    ),
    response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
)
print("\nMisspelling 'sequencng' still finds:")
print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")

# --8<-- [end:full_text_search]


# --8<-- [start:columns_and_highlights]
# Ask for a snippet of the matching text alongside each hit, so a
# result list can show why the row matched.
results = index.query(
    search_query=SearchQuery(
        query=Query(match={"abstract": MatchFieldOptions(query="sequencing")}),
        source=SourceFilter(includes=["study_name", "assay"]),
        highlight=Highlight(fields={"abstract": HighlightField(number_of_fragments=1)}),
        size=10,
    ),
    response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
)
print("Studies that sequenced something:")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")
    for highlight in hit.highlights:
        print(f"    {highlight.name}: {highlight.snippets}")


# --8<-- [end:columns_and_highlights]


# --8<-- [start:filters_and_sorting]
# Combine a scored clause with unscored filters using a bool query,
# then order the results by a numeric column instead of by relevance.

results = index.query(
    search_query=SearchQuery(
        query=Query(
            bool=BoolQuery(
                # Scored: how well the abstract matches drives relevance
                must=[Query(match={"abstract": MatchFieldOptions(query="sequencing")})],
                # Unscored: a hard cutoff on cohort size
                filter=[Query(range={"participant_count": RangeFieldOptions(gte=200)})],
                # Unscored: drop a diagnosis we are not interested in
                must_not=[
                    Query(
                        match_phrase={
                            "diagnosis": MatchPhraseFieldOptions(
                                query="Parkinson's Disease"
                            )
                        }
                    )
                ],
            )
        ),
        source=SourceFilter(includes=["study_name", "participant_count"]),
        sort=[{"participant_count": "desc"}],
        size=10,
    ),
    response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
)
print("Sequencing studies with at least 200 participants, largest first:")
print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")

# --8<-- [end:filters_and_sorting]


# --8<-- [start:aggregations]
# Count how many studies fall under each diagnosis and average their
# cohort sizes, while the hit list itself shows only one diagnosis.

results = index.query(
    search_query=SearchQuery(
        query=Query(match_all={}),
        aggregations={
            "by_diagnosis": Aggregation(
                terms=TermsAggregation(field="diagnosis", size=10)
            ),
            "mean_cohort_size": Aggregation(
                avg=AvgAggregation(field="participant_count")
            ),
        },
        # post_filter narrows the hits but not the aggregations, so the facet
        # counts still show every option a person could pick next
        post_filter=Query(
            match_phrase={
                "diagnosis": MatchPhraseFieldOptions(query="Alzheimer's Disease")
            }
        ),
        source=SourceFilter(includes=["study_name", "diagnosis"]),
        size=10,
    ),
    response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
)
print("Hits after the post filter:")
print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
for hit in results.hits:
    fields = {field.name: field.value for field in hit.fields}
    print(f"  {fields}")
print("\nFacet counts across all studies:")
print(json.dumps(results.aggregation_results, indent=2))

# --8<-- [end:aggregations]


# --8<-- [start:autocomplete]
# Back a type-ahead box with the autocomplete endpoint, which returns
# its results directly instead of running as an asynchronous job.
hits = index.autocomplete(
    query=Query(
        match_bool_prefix={"study_name": MatchBoolPrefixFieldOptions(query="Mayo Cl")}
    ),
    source=SourceFilter(includes=["study_name"]),
)
print("Suggestions for 'Mayo Cl':")
for hit in hits:
    print(f"  {[field.value for field in hit.fields]}")


# --8<-- [end:autocomplete]


# --8<-- [start:pagination_offset]
# Walk every row in the index two hits at a time with a growing offset.
page_size = 2
offset = 0
while True:
    results = index.query(
        search_query=SearchQuery(
            query=Query(match_all={}),
            source=SourceFilter(includes=["study_name", "participant_count"]),
            sort=[{"participant_count": "desc"}],
            from_=offset,
            size=page_size,
        ),
        response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
    )
    print(f"Page starting at offset {offset}:")
    print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
    for hit in results.hits:
        fields = {field.name: field.value for field in hit.fields}
        print(f"  {fields}")

    offset += page_size
    if offset >= results.total_hits:
        break

# --8<-- [end:pagination_offset]


# --8<-- [start:pagination_cursor]
# The same walk, using the search_after cursor the server hands back instead of
# a growing offset.
page_size = 2
search_after = None
page = 0
while True:
    results = index.query(
        search_query=SearchQuery(
            query=Query(match_all={}),
            source=SourceFilter(includes=["study_name", "participant_count"]),
            # search_after walks a sort order, so the sort has to put every row
            # in a definite position. participant_count is unique in this table;
            # on real data append a unique column to break ties, or a page
            # boundary can skip or repeat rows.
            sort=[{"participant_count": "desc"}],
            # None on the first request, then the cursor from the previous one
            search_after=search_after,
            size=page_size,
        ),
        response_parts=[SearchQueryPart.HITS, SearchQueryPart.TOTAL_HITS],
    )
    print(f"Page {page}:")
    print(f"total_hits={results.total_hits}, returned={len(results.hits)}")
    for hit in results.hits:
        fields = {field.name: field.value for field in hit.fields}
        print(f"  {fields}")

    # The cursor is opaque -- pass it back unchanged. It goes None on the last
    # page, which is what ends the walk.
    search_after = results.next_search_after
    if not search_after or not results.hits:
        break
    page += 1

# --8<-- [end:pagination_cursor]
