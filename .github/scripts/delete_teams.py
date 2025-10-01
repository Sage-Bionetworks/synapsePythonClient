import asyncio
from typing import Dict, Set

from synapseclient import Synapse

syn = Synapse()
syn.login(profile=None)

# Maximum number of concurrent team deletions
MAX_CONCURRENT_DELETIONS = 5


async def delete_team(team_id: str) -> str:
    """Delete a team asynchronously and return the team_id"""
    try:
        # Need to use run_in_executor since the delete_team function is synchronous
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: syn.delete_team(team_id))
        return f"Deleted team {team_id}"
    except Exception as e:
        return f"Failed to delete team {team_id}: {str(e)}"


async def main():
    # Get all teams for the current user
    teams = syn._find_teams_for_principal(principal_id=syn.credentials.owner_id)

    # Create a semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELETIONS)

    # Set to track active tasks
    pending_tasks: Set[asyncio.Task] = set()

    # Track if we've processed any teams
    processed_any = False

    async def delete_with_semaphore(team: Dict):
        """Helper function that uses the semaphore to limit concurrency"""
        async with semaphore:
            result = await delete_team(team["id"])
            print(result)
            return result

    # Process teams as they come in from the iterator
    for team in teams:
        processed_any = True

        # Create a new task for this team
        task = asyncio.create_task(delete_with_semaphore(team))
        pending_tasks.add(task)
        task.add_done_callback(pending_tasks.discard)

        # Process any completed tasks
        if len(pending_tasks) >= MAX_CONCURRENT_DELETIONS:
            # Wait for at least one task to complete before continuing
            done, _ = await asyncio.wait(
                pending_tasks, return_when=asyncio.FIRST_COMPLETED
            )

    # Wait for all remaining tasks to complete
    if pending_tasks:
        await asyncio.gather(*pending_tasks)

    if not processed_any:
        print("No teams found to delete")


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
