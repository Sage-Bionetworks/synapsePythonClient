[](){ #search-management-reference-sync }
# Search Configuration

Analyzer, synonym, and configuration resources that control how a
[SearchIndex][searchindex-reference-sync] builds its OpenSearch index.

## API reference

::: synapseclient.models.SearchConfiguration
    options:
        inherited_members: true
        members:
            - store
            - get
            - list

::: synapseclient.models.TextAnalyzer
    options:
        inherited_members: true
        members:
            - store
            - get
            - list

::: synapseclient.models.SynonymSet
    options:
        inherited_members: true
        members:
            - store
            - get
            - list

::: synapseclient.models.ColumnAnalyzerOverride
    options:
        inherited_members: true
        members:
            - store
            - get
            - list

::: synapseclient.models.SearchConfigBinding
    options:
        inherited_members: true
        members:
            - store
            - get
            - delete

## Supporting types

::: synapseclient.models.ColumnAnalyzerOverrideEntry
