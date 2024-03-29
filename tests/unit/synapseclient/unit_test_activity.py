from synapseclient.activity import Activity


# SYNPY-744
def test_private_getStringList():
    act = Activity()
    url_string = "https://github.com/Sage-Bionetworks/ampAdScripts/blob/master/Broad-Rush/migrateROSMAPGenotypesFeb2015.R"
    act.used(
        [
            {
                "wasExecuted": True,
                "concreteType": "org.sagebionetworks.repo.model.provenance.UsedURL",
                "url": url_string,
            }
        ]
    )
    assert [url_string] == act._getStringList()
