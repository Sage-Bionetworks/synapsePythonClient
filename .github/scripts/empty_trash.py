import asyncio
from typing import Set

from synapseclient import Synapse

syn = Synapse()
syn.login()

# Maximum number of concurrent deletion operations
MAX_CONCURRENT_DELETIONS = 5


async def purge_entity(entity_id: str) -> str:
    """Purge an entity from trash asynchronously and return the status"""
    try:
        # Need to use run_in_executor since the restPUT function is synchronous
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: syn.restPUT(uri=f"/trashcan/purge/{entity_id}")
        )
        return f"Purged entity {entity_id} from trash"
    except Exception as e:
        return f"Failed to purge entity {entity_id}: {str(e)}"


async def main():
    # Create a semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELETIONS)

    # Set to track active tasks
    pending_tasks: Set[asyncio.Task] = set()

    # Track if we've processed any entities
    processed_any = False

    async def purge_with_semaphore(entity_id: str):
        """Helper function that uses the semaphore to limit concurrency"""
        async with semaphore:
            result = await purge_entity(entity_id)
            print(result)
            return result

    # Process entities as they come in from the paginated iterator
    for result in syn._GET_paginated("/trashcan/view", limit=200, offset=0):
        processed_any = True
        entity_id = result["entityId"]

        # Create a new task for this entity
        task = asyncio.create_task(purge_with_semaphore(entity_id))
        pending_tasks.add(task)
        task.add_done_callback(pending_tasks.discard)

        # Process any completed tasks when we reach MAX_CONCURRENT_DELETIONS
        if len(pending_tasks) >= MAX_CONCURRENT_DELETIONS:
            # Wait for at least one task to complete before continuing
            done, _ = await asyncio.wait(
                pending_tasks, return_when=asyncio.FIRST_COMPLETED
            )

    # Wait for all remaining tasks to complete
    if pending_tasks:
        await asyncio.gather(*pending_tasks)

    if not processed_any:
        print("No entities found in trash to purge")


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
