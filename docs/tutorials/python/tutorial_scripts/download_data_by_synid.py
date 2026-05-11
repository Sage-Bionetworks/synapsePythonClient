"""
Here is where you'll find the code for the downloading files by synapse ids tutorial.
"""

import asyncio

from synapseclient import Synapse
from synapseclient.models import File

syn = Synapse()
syn.login()

# A mapping of Synapse IDs to the local directory each file should be downloaded to.
# Files can be directed to different directories as needed.
SYN_IDS_AND_PATHS = {
    "syn60584250": "~/temp/subdir1",
    "syn60584256": "~/temp/subdir1",
    "syn60584248": "~/temp/subdir1",
    "syn60584252": "~/temp/subdir1",
    "syn60584258": "~/temp/subdir1",
    "syn60584260": "~/temp/subdir1",
    "syn60584257": "~/temp/subdir1",
    "syn60584251": "~/temp/subdir1",
    "syn60584253": "~/temp/subdir1",
    "syn60584390": "~/temp/subdir1",
    "syn60584405": "~/temp/subdir2",
    "syn60584400": "~/temp/subdir3",
}


async def main():
    # Build a list of concurrent download tasks — one per Synapse ID
    tasks = []
    for syn_id, path in SYN_IDS_AND_PATHS.items():
        tasks.append(File(id=syn_id, path=path).get_async())

    # Download all files concurrently and wait for every one to finish
    results = await asyncio.gather(*tasks)

    print(f"Retrieved {len(results)} files")


asyncio.run(main())
