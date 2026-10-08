"""Here is where you'll find the code for the semantic and hybrid search tutorial."""

# --8<-- [start:setup]
import time

import pandas as pd

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnAnalyzerOverride,
    ColumnAnalyzerOverrideEntry,
    ColumnType,
    NamedSearchPipeline,
    Organization,
    Project,
    SearchConfiguration,
    SearchIndex,
    SearchQuery,
    SearchQueryPart,
    Table,
)
from synapseclient.models.search_dsl import (
    HybridClause,
    HybridQuery,
    MatchFieldOptions,
    NeuralFieldOptions,
    Query,
    SearchPipeline,
    SourceFilter,
)

syn = Synapse()
syn.login()

project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = project.id

table = Table(
    name="Study Summaries For Semantic Search",
    parent_id=project_id,
    columns=[
        Column(name="study_name", column_type=ColumnType.STRING),
        Column(name="abstract", column_type=ColumnType.LARGETEXT),
        Column(name="diagnosis", column_type=ColumnType.STRING),
    ],
).store()

studies = pd.DataFrame(
    [
        {
            "study_name": "ROSMAP Cortex Proteomics",
            "abstract": "Quantitative proteomics of dorsolateral prefrontal cortex "
            "from donors with Alzheimer's disease and cognitively normal controls.",
            "diagnosis": "Alzheimer's Disease",
        },
        {
            "study_name": "MSBB RNA Sequencing",
            "abstract": "Bulk RNA sequencing across four brain regions in a cohort "
            "spanning the full range of Alzheimer's disease neuropathology.",
            "diagnosis": "Alzheimer's Disease",
        },
        {
            "study_name": "Mayo Clinic Whole Genome",
            "abstract": "Whole genome sequencing of temporal cortex samples from "
            "donors with Alzheimer's disease, progressive supranuclear palsy, "
            "and controls.",
            "diagnosis": "Alzheimer's Disease",
        },
        {
            "study_name": "Healthy Aging Single Cell Atlas",
            "abstract": "Single nucleus RNA sequencing of hippocampus from "
            "cognitively normal aged donors, establishing a baseline atlas.",
            "diagnosis": "Cognitively Normal",
        },
        {
            "study_name": "MCI Plasma Biomarkers",
            "abstract": "Plasma biomarker panel measuring phosphorylated tau and "
            "neurofilament light chain in mild cognitive impairment.",
            "diagnosis": "Mild Cognitive Impairment",
        },
        {
            "study_name": "Parkinson Comparative Cohort",
            "abstract": "Comparative transcriptomic profiling of substantia nigra "
            "in Parkinson disease versus age-matched controls.",
            "diagnosis": "Parkinson's Disease",
        },
    ]
)
table.upsert_rows(values=studies, primary_keys=["study_name"])


def print_hits(title: str, results) -> None:
    """Print each hit's score and study name."""
    print(title)
    for hit in results.hits:
        print(f"  {hit.score:.2f}  {hit.fields[0].value}")


# --8<-- [end:setup]


# --8<-- [start:enable_semantic]
# Organization-scoped resources are restricted to Sage Bionetworks employees,
# and none of them can be deleted once created.
organization_name = "my.uniquely.named.organization"
try:
    Organization(name=organization_name).get()
except SynapseHTTPError:
    Organization(name=organization_name).store()


def save(resource):
    """Create the resource, or update the one with the same name so that
    re-running this script leaves it exactly as written here."""
    for existing in type(resource).list(organization_name=organization_name):
        if existing.name == resource.name:
            resource.id, resource.etag = existing.id, existing.etag
    return resource.store()


# semantic=True is the switch: Synapse captures the meaning of each abstract
# when the index is built
overrides = save(
    ColumnAnalyzerOverride(
        organization_name=organization_name,
        name="semantic_abstracts",
        overrides=[ColumnAnalyzerOverrideEntry(column_name="abstract", semantic=True)],
    )
)

configuration = save(
    SearchConfiguration(
        organization_name=organization_name,
        name="semantic_study_config",
        column_analyzer_overrides=[{"$ref": overrides.qualified_name}],
    )
)

index = SearchIndex(
    name="Study Summaries Semantic Index",
    parent_id=project_id,
    defining_sql=f"SELECT * FROM {table.id}",
    search_configuration_id=configuration.id,
).store()
print(f"Created SearchIndex with ID: {index.id}")

# The index is built in the background. Wait until every row is searchable.
while True:
    results = index.query(
        search_query=SearchQuery(query=Query(match_all={})),
        response_parts=[SearchQueryPart.TOTAL_HITS],
    )
    if results.total_hits == len(studies):
        break
    time.sleep(5)
print(f"Index {index.id} is ready with {results.total_hits} rows")

# --8<-- [end:enable_semantic]


# --8<-- [start:semantic_search]
# None of the abstracts use the words "memory", "older", or "adults".
results = index.query(
    search_query=SearchQuery(
        hybrid=HybridQuery(
            queries=[
                HybridClause(
                    neural={
                        "semantic_search": NeuralFieldOptions(
                            query_text="memory problems in older adults",
                            # Return the 3 studies closest in meaning
                            k=3,
                        )
                    }
                )
            ]
        ),
        source=SourceFilter(includes=["study_name"]),
    ),
    response_parts=[SearchQueryPart.HITS],
)
print_hits("Closest in meaning to 'memory problems in older adults':", results)

# --8<-- [end:semantic_search]


# --8<-- [start:hybrid_search]
# A keyword search for "sequencing" and a semantic search for "forgetfulness
# and dementia", blended into one ranking. Any Query works as a clause; only
# the semantic clause needs HybridClause, for its `neural` key.
keyword_clause = Query(match={"abstract": MatchFieldOptions(query="sequencing")})
meaning_clause = HybridClause(
    neural={
        "semantic_search": NeuralFieldOptions(
            query_text="forgetfulness and dementia", k=6
        )
    }
)

results = index.query(
    search_query=SearchQuery(
        hybrid=HybridQuery(queries=[keyword_clause, meaning_clause]),
        source=SourceFilter(includes=["study_name"]),
        size=3,
    ),
    response_parts=[SearchQueryPart.HITS],
)
print_hits("'sequencing' + 'forgetfulness and dementia', weighted equally:", results)

# --8<-- [end:hybrid_search]


# --8<-- [start:weighting]
def blend(keyword_weight: float, meaning_weight: float) -> SearchPipeline:
    """A search pipeline that weights the keyword clause and the meaning
    clause, in the order they are listed in the query."""
    return {
        "phase_results_processors": [
            {
                "normalization-processor": {
                    "normalization": {"technique": "min_max"},
                    "combination": {
                        "technique": "arithmetic_mean",
                        "parameters": {"weights": [keyword_weight, meaning_weight]},
                    },
                }
            }
        ]
    }


for label, pipeline in [
    ("Keyword first (80/20):", blend(0.8, 0.2)),
    ("Meaning first (20/80):", blend(0.2, 0.8)),
]:
    results = index.query(
        search_query=SearchQuery(
            hybrid=HybridQuery(queries=[keyword_clause, meaning_clause]),
            search_pipeline=pipeline,
            source=SourceFilter(includes=["study_name"]),
            size=3,
        ),
        response_parts=[SearchQueryPart.HITS],
    )
    print_hits(label, results)

# --8<-- [end:weighting]


# --8<-- [start:named_pipeline]
# Save the meaning-first blend once, under the same Organization
meaning_first = save(
    NamedSearchPipeline(
        organization_name=organization_name,
        name="meaning_first",
        description="80% semantic similarity, 20% keyword match",
        settings=blend(0.2, 0.8),
    )
)
print(f"Saved search pipeline: {meaning_first.qualified_name}")

# Use it by name on a single query...
results = index.query(
    search_query=SearchQuery(
        hybrid=HybridQuery(queries=[keyword_clause, meaning_clause]),
        search_pipeline={"$ref": meaning_first.qualified_name},
        source=SourceFilter(includes=["study_name"]),
        size=3,
    ),
    response_parts=[SearchQueryPart.HITS],
)
print_hits("Using the saved pipeline by name:", results)

# ...or make it the default, so every hybrid query against indexes using this
# configuration blends meaning first unless it asks for something else
configuration.default_search_pipeline = {"$ref": meaning_first.qualified_name}
configuration = configuration.store()

results = index.query(
    search_query=SearchQuery(
        hybrid=HybridQuery(queries=[keyword_clause, meaning_clause]),
        source=SourceFilter(includes=["study_name"]),
        size=3,
    ),
    response_parts=[SearchQueryPart.HITS],
)
print_hits("No pipeline on the query, so the default is used:", results)

# --8<-- [end:named_pipeline]


# --8<-- [start:filtering]
# Only Alzheimer's studies, ranked by how close they are to the meaning
results = index.query(
    search_query=SearchQuery(
        hybrid=HybridQuery(
            queries=[meaning_clause],
            filter=Query(match={"diagnosis": MatchFieldOptions(query="Alzheimer")}),
        ),
        source=SourceFilter(includes=["study_name"]),
    ),
    response_parts=[SearchQueryPart.HITS],
)
print_hits("'forgetfulness and dementia', Alzheimer's studies only:", results)

# --8<-- [end:filtering]
