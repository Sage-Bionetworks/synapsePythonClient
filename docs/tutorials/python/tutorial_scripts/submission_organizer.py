"""
Submission Organizer Tutorial - Code for working with Submissions as a challenge organizer.

This tutorial demonstrates how to:
1. Annotate a submission to score it
2. Batch-update submission statuses
3. Fetch the submission bundle for a given submission
4. Allow cancellation of submissions
5. Delete submissions
"""

from synapseclient import Synapse
from synapseclient.models import Submission, SubmissionBundle, SubmissionStatus

syn = Synapse()
syn.login()

# REQUIRED: Set these to your actual Synapse IDs
# Do NOT leave these as None - the script will not work properly
EVALUATION_ID = None  # Replace with the evaluation queue ID you manage
SUBMISSION_ID = None  # Replace with a submission ID from your evaluation

assert (
    EVALUATION_ID is not None
), "EVALUATION_ID must be set to the evaluation queue ID you manage"
assert (
    SUBMISSION_ID is not None
), "SUBMISSION_ID must be set to a submission ID from your evaluation"

print(f"Working with Evaluation: {EVALUATION_ID}")
print(f"Managing Submission: {SUBMISSION_ID}")

# ==============================================================================
# 1. Annotate a submission to score it
# ==============================================================================

print("\n=== 1. Annotating a submission with scores ===")

# First, get the submission status
status = SubmissionStatus(id=SUBMISSION_ID).get()
print(f"Retrieved submission status for submission {SUBMISSION_ID}")
print(f"Current status: {status.status}")

# Update the submission status with scoring information
status.status = "SCORED"
status.submission_annotations = {
    "accuracy": [0.85],
    "precision": [0.82],
    "feedback": ["Good performance!"],
    "validation_errors": "None detected",
    "score_errors": "None detected",
}

# Store the updated status
updated_status = status.store()
print(f"Successfully scored submission!")
print(f"Status: {updated_status.status}")
print(f"Annotations added:")
for key, value in updated_status.submission_annotations.items():
    print(f"  {key}: {value}")

# ==============================================================================
# 2. Batch-update submission statuses
# ==============================================================================

print("\n=== 2. Batch updating submission statuses ===")

# First, get all submission statuses that need updating
statuses_to_update = SubmissionStatus.get_all_submission_statuses(
    evaluation_id=EVALUATION_ID,
    status="RECEIVED",  # Get submissions that haven't been scored yet
    limit=50,  # Limit to 50 for this example (max is 500 for batch operations)
)

print(f"Found {len(statuses_to_update)} submissions to batch update")

if statuses_to_update:
    # Update each status with validation information
    for i, status in enumerate(statuses_to_update):
        status.status = "VALIDATED"
        status.submission_annotations = {
            "validation_status": ["PASSED"],
            "validation_timestamp": ["2024-11-24T10:30:00Z"],
            "batch_number": [i + 1],
            "validator": ["automated_system"],
        }

    # Perform batch update
    batch_response = SubmissionStatus.batch_update_submission_statuses(
        evaluation_id=EVALUATION_ID,
        statuses=statuses_to_update,
        is_first_batch=True,
        is_last_batch=True,
    )

    print(f"Batch update completed successfully!")
    print(f"Batch response: {batch_response}")
else:
    print("No submissions found with 'RECEIVED' status to update")

# ==============================================================================
# 3. Fetch the submission bundle for a given submission
# ==============================================================================

print("\n=== 3. Fetching submission bundle ===")

# Get all submission bundles for the evaluation
print("Fetching all submission bundles for the evaluation...")

bundles = list(
    SubmissionBundle.get_evaluation_submission_bundles(
        evaluation_id=EVALUATION_ID, status="SCORED"  # Only get scored submissions
    )
)

print(f"Found {len(bundles)} scored submission bundles")

for i, bundle in enumerate(bundles[:5]):  # Show first 5
    submission = bundle.submission
    status = bundle.submission_status

    print(f"\nBundle {i + 1}:")
    if submission:
        print(f"  Submission ID: {submission.id}")
        print(f"  Submitter: {submission.submitter_alias}")
        print(f"  Entity ID: {submission.entity_id}")
        print(f"  Created: {submission.created_on}")

    if status:
        print(f"  Status: {status.status}")
        print(f"  Modified: {status.modified_on}")
        if status.submission_annotations:
            print(f"  Scores:")
            for key, value in status.submission_annotations.items():
                if key in ["accuracy", "f1_score", "precision", "recall"]:
                    print(f"    {key}: {value}")

# ==============================================================================
# 4. Allow cancellation of submissions
# ==============================================================================

print("\n=== 4. Managing submission cancellation ===")

# First, check if any submissions have requested cancellation
all_statuses = SubmissionStatus.get_all_submission_statuses(
    evaluation_id=EVALUATION_ID, limit=100
)

cancellation_requests = [status for status in all_statuses if status.cancel_requested]

print(f"Found {len(cancellation_requests)} submissions with cancellation requests")

# Process cancellation requests
for status in cancellation_requests:
    print(f"Processing cancellation request for submission {status.id}")

    # Update to allow cancellation (organizer decision)
    status.can_cancel = True
    status.status = "CLOSED"
    status.submission_annotations.update(
        {
            "cancellation_reason": ["User requested cancellation"],
            "cancelled_by": ["organizer"],
            "cancellation_date": ["2024-11-24"],
        }
    )

    # Store the update
    updated_status = status.store()
    print(f"  Approved cancellation for submission {updated_status.id}")

# Example: Proactively allow cancellation for a specific submission
print("\nEnabling cancellation for a specific submission...")
target_status = SubmissionStatus(id=SUBMISSION_ID).get()
target_status.can_cancel = True
target_status = target_status.store()
print(f"Cancellation enabled for submission {SUBMISSION_ID}")

# ==============================================================================
# 5. Delete submissions
# ==============================================================================

# print("\n=== 5. Deleting submissions ===")
# print("Finding and deleting submissions that have been requested for cancellation...")

# # Get all submission statuses to check for cancellation requests
# all_statuses = SubmissionStatus.get_all_submission_statuses(
#     evaluation_id=EVALUATION_ID,
# )

# # Find submissions that have been requested for cancellation
# submissions_to_delete = []
# for status in all_statuses:
#     if status.cancel_requested:
#         submissions_to_delete.append(status.id)

# print(f"Found {len(submissions_to_delete)} submissions with cancellation requests")

# # Delete each submission that was requested for cancellation
# for submission_id in submissions_to_delete:
#     submission = Submission(id=submission_id).get()
#     submission.delete()
#     print(f"Successfully deleted submission {submission_id}")

# if submissions_to_delete:
#     print(f"Completed deletion of {len(submissions_to_delete)} requested submissions")

print(f"\nDeletion step is commented out by default.")
print(f"Uncomment the deletion code if you want to test this functionality.")

print(f"\n=== Organizer tutorial completed! ===")
