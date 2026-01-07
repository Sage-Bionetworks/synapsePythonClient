"""
Submission Participant Tutorial - Code for working with Submissions as a challenge participant.

This tutorial demonstrates how to:
1. Make a submission to an existing evaluation queue
2. Fetch your existing submission
3. Count your submissions
4. Fetch all of your submissions from an evaluation queue
5. Check the status of your submission
6. Cancel your submission
"""

from synapseclient import Synapse
from synapseclient.models import Submission, SubmissionStatus

syn = Synapse()
syn.login()

# REQUIRED: Set these to your actual Synapse IDs
# Do NOT leave these as None - the script will not work properly
EVALUATION_ID = None  # Replace with the evaluation queue ID you want to submit to
ENTITY_ID = None  # Replace with the entity ID you want to submit

assert EVALUATION_ID is not None, "EVALUATION_ID must be set to the evaluation queue ID"
assert (
    ENTITY_ID is not None
), "ENTITY_ID must be set to the entity ID you want to submit"

print(f"Working with Evaluation: {EVALUATION_ID}")
print(f"Submitting Entity: {ENTITY_ID}")

# ==============================================================================
# 1. Make a submission to an existing evaluation queue on Synapse
# ==============================================================================

print("\n=== 1. Making a submission ===")

# Create a new Submission object
submission = Submission(
    entity_id=ENTITY_ID, evaluation_id=EVALUATION_ID, name="My Tutorial Submission"
)

# Submit the entity to the evaluation queue
submission = submission.store()

print(f"Submission created successfully!")
print(f"Submission ID: {submission.id}")
print(f"Submitted Entity: {submission.entity_id}")
print(f"Evaluation: {submission.evaluation_id}")
print(f"Submission Name: {submission.name}")
print(f"Created On: {submission.created_on}")

# Store the submission ID for later use
submission_id = submission.id

# ==============================================================================
# 2. Fetch your existing submission
# ==============================================================================

print("\n=== 2. Fetching existing submission ===")

# Retrieve the submission we just created
retrieved_submission = Submission(id=submission_id).get()

print(f"Retrieved submission:")
print(f"  ID: {retrieved_submission.id}")
print(f"  Name: {retrieved_submission.name}")
print(f"  Entity ID: {retrieved_submission.entity_id}")
print(f"  Submitter: {retrieved_submission.submitter_alias}")
print(f"  Created On: {retrieved_submission.created_on}")

# ==============================================================================
# 3. Count your submissions
# ==============================================================================

print("\n=== 3. Counting submissions ===")

# Get the total count of submissions for this evaluation
submission_count = Submission.get_submission_count(evaluation_id=EVALUATION_ID)

print(f"Total submissions in evaluation: {submission_count}")

# Get count of submissions with specific status (optional)
scored_count = Submission.get_submission_count(
    evaluation_id=EVALUATION_ID, status="SCORED"
)

print(f"SCORED submissions in evaluation: {scored_count}")

# ==============================================================================
# 4. Fetch all of your submissions from an existing evaluation queue
# ==============================================================================

print("\n=== 4. Fetching all your submissions ===")

# Get all of your submissions for this evaluation
user_submissions = list(Submission.get_user_submissions(evaluation_id=EVALUATION_ID))

print(f"Found {len(user_submissions)} submissions from the current user:")
for i, sub in enumerate(user_submissions, 1):
    print(f"  {i}. ID: {sub.id}, Name: {sub.name}, Created: {sub.created_on}")

# ==============================================================================
# 5. Check the status of your submission
# ==============================================================================

print("\n=== 5. Checking submission status ===")

# Fetch the status of our submission
status = SubmissionStatus(id=submission_id).get()

print(f"Submission status details:")
print(f"  Status: {status.status}")
print(f"  Modified On: {status.modified_on}")
print(f"  Can Cancel: {status.can_cancel}")
print(f"  Cancel Requested: {status.cancel_requested}")

# Check if there are any submission annotations (scores, feedback, etc.)
if status.submission_annotations:
    print(f"  Submission Annotations:")
    for key, value in status.submission_annotations.items():
        print(f"    {key}: {value}")
else:
    print(f"  No submission annotations available")

# ==============================================================================
# 6. Cancel your submission (optional)
# ==============================================================================

print("\n=== 6. Cancelling submission ===")

# Note: Only cancel if the submission allows it
# Uncomment the following lines if you want to test cancellation:

# cancelled_submission = submission.cancel()
# print(f"Submission {cancelled_submission.id} has been requested for cancellation")
#
# # Check the updated status
# updated_status = SubmissionStatus(id=submission_id).get()
# print(f"Cancel requested: {updated_status.cancel_requested}")

print(f"\nCancellation is commented out by default.")
print(f"Uncomment the cancellation code if you want to test this functionality.")

print(f"\n=== Tutorial completed! ===")
print(f"Your submission ID {submission_id} is ready for evaluation.")
print(f"Check back later to see if the organizers have scored your submission.")
