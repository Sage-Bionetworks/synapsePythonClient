import asyncio
from typing import Set

from synapseclient import Evaluation, Synapse

syn = Synapse()
syn.login()

# Maximum number of concurrent deletion operations
MAX_CONCURRENT_DELETIONS = 5


async def delete_evaluation(eval_obj: Evaluation) -> str:
    """Delete an evaluation asynchronously and return the status"""
    try:
        # Need to use run_in_executor since the delete function is synchronous
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: syn.delete(eval_obj))
        return f"Deleted evaluation {eval_obj.id}"
    except Exception as e:
        return f"Failed to delete evaluation {eval_obj.id}: {str(e)}"


async def main():
    # Create a semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELETIONS)

    # Set to track active tasks
    pending_tasks: Set[asyncio.Task] = set()

    # Track if we've processed any evaluations
    processed_any = False

    async def delete_with_semaphore(eval_obj: Evaluation):
        """Helper function that uses the semaphore to limit concurrency"""
        async with semaphore:
            result = await delete_evaluation(eval_obj)
            print(result)
            return result

    # Process evaluations as they come in from the paginated iterator
    for result in syn._GET_paginated(
        "/evaluation?accessType=DELETE", limit=200, offset=0
    ):
        processed_any = True
        eval_obj = Evaluation(**result)

        # Create a new task for this evaluation
        task = asyncio.create_task(delete_with_semaphore(eval_obj))
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
        print("No evaluations found to delete")


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
