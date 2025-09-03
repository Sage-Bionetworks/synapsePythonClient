import asyncio
from typing import Dict, Set

from synapseclient import Synapse

syn = Synapse()
syn.login(profile=None)

# Maximum number of concurrent deletion operations
MAX_CONCURRENT_DELETIONS = 5


async def delete_project(project_id: str) -> str:
    """Delete a project asynchronously and return the result status"""
    try:
        # Need to use run_in_executor since the delete function is synchronous
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: syn.delete(project_id))
        return f"Deleted {project_id}"
    except Exception as e:
        return f"Failed to delete {project_id}: {str(e)}"


async def main():
    # Create a semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELETIONS)

    # Set to track active tasks
    pending_tasks: Set[asyncio.Task] = set()

    # Track if we've processed any projects
    processed_any = False

    async def delete_with_semaphore(project: Dict):
        """Helper function that uses the semaphore to limit concurrency"""
        async with semaphore:
            result = await delete_project(project["id"])
            print(result)
            return result

    # Process projects as they come in from the iterator
    for project in syn.getChildren(parent=None, includeTypes=["project"]):
        processed_any = True

        # Create a new task for this project
        task = asyncio.create_task(delete_with_semaphore(project))
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
        print("No projects found to delete")


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
