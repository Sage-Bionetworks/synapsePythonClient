def test_create_submission_async():
    # WHEN an evaluation is retrieved
    evaluation = Evaluation(id=evaluation_id).get()

    # AND an entity is retrieved
    file = File(name="test.txt", parentId=project.id).get()

    # THEN the entity can be submitted to the evaluation
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()


def test_get_submission_async():
    # GIVEN a submission has been created
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the submission is retrieved by ID
    retrieved_submission = Submission(id=submission.id).get()

    # THEN the retrieved submission matches the created one
    assert retrieved_submission.id == submission.id
    assert retrieved_submission.name == submission.name

    # AND the user_id matches the current user
    current_user = syn.getUserProfile()().id
    assert retrieved_submission.user_id == current_user


def test_get_evaluation_submissions_async():
    # GIVEN an evaluation has submissions
    evaluation = Evaluation(id=evaluation_id).get()

    # WHEN submissions are retrieved for the evaluation
    submissions = Submission.get_evaluation_submissions(evaluation.id)

    # THEN the submissions list is not empty
    assert len(submissions) > 0

    # AND each submission belongs to the evaluation
    for submission in submissions:
        assert submission.evaluation_id == evaluation.id


def test_get_user_submissions_async():
    # GIVEN a user has made submissions
    current_user = syn.getUserProfile()().id

    # WHEN submissions are retrieved for the user
    submissions = Submission.get_user_submissions(current_user)

    # THEN the submissions list is not empty
    assert len(submissions) > 0

    # AND each submission belongs to the user
    for submission in submissions:
        assert submission.user_id == current_user


def test_get_submission_count_async():
    # GIVEN an evaluation has submissions
    evaluation = Evaluation(id=evaluation_id).get()

    # WHEN the submission count is retrieved for the evaluation
    count = Submission.get_submission_count(evaluation.id)

    # THEN the count is greater than zero
    assert count > 0


def test_delete_submission_async():
    # GIVEN a submission has been created
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the submission is deleted
    submission.delete()

    # THEN retrieving the submission should raise an error
    try:
        Submission(id=submission.id).get()
        assert False, "Expected an error when retrieving a deleted submission"
    except SynapseError as e:
        assert e.response.status_code == 404


def test_cancel_submission_async():
    # GIVEN a submission has been created
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the submission is canceled
    submission.cancel()

    # THEN the submission status should be 'CANCELED'
    updated_submission = Submission(id=submission.id).get()
    assert updated_submission.status == "CANCELED"


def test_get_submission_status_async():
    # GIVEN a submission has been created
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the submission status is retrieved
    status = submission.get_status()

    # THEN the status should be 'RECEIVED'
    assert status == "RECEIVED"


def test_update_submission_status_async():
    # GIVEN a submission has been created
    submission = Submission(
        name="Test Submission",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the submission status is retrieved
    status = submission.get_status()
    assert status != "SCORED"

    # AND the submission status is updated to 'SCORED'
    submission.update_status("SCORED")

    # THEN the submission status should be 'SCORED'
    updated_submission = Submission(id=submission.id).get()
    assert updated_submission.status == "SCORED"


def test_get_evaluation_submission_statuses_async():
    # GIVEN an evaluation has submissions
    evaluation = Evaluation(id=evaluation_id).get()

    # WHEN the submission statuses are retrieved for the evaluation
    statuses = Submission.get_evaluation_submission_statuses(evaluation.id)

    # THEN the statuses list is not empty
    assert len(statuses) > 0


def test_batch_update_statuses_async():
    # GIVEN multiple submissions have been created
    submission1 = Submission(
        name="Test Submission 1",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()
    submission2 = Submission(
        name="Test Submission 2",
        entity_id=file.id,
        evaluation_id=evaluation.id,
        version_number=1,
    ).store()

    # WHEN the statuses of the submissions are batch updated to 'SCORED'
    Submission.batch_update_statuses([submission1.id, submission2.id], "SCORED")

    # THEN each submission status should be 'SCORED'
    updated_submission1 = Submission(id=submission1.id).get()
    updated_submission2 = Submission(id=submission2.id).get()
    assert updated_submission1.status == "SCORED"
    assert updated_submission2.status == "SCORED"


def test_get_evaluation_submission_bundles_async():
    # GIVEN an evaluation has submissions
    evaluation = Evaluation(id=evaluation_id).get()

    # WHEN the submission bundles are retrieved for the evaluation
    bundles = Submission.get_evaluation_submission_bundles(evaluation.id)

    # THEN the bundles list is not empty
    assert len(bundles) > 0


def test_get_user_submission_bundles_async():
    # GIVEN a user has made submissions
    current_user = syn.getUserProfile()().id

    # WHEN the submission bundles are retrieved for the user
    bundles = Submission.get_user_submission_bundles(current_user)

    # THEN the bundles list is not empty
    assert len(bundles) > 0
