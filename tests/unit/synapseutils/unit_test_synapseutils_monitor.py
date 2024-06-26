from unittest.mock import MagicMock, call, patch

import synapseutils
from synapseutils import notifyMe, with_progress_bar


def test_notifyMe__successful_call(syn):
    subject = "some message subject"
    owner_id = "12434"
    user_profile = {"ownerId": owner_id}
    with patch.object(syn, "sendMessage") as mocked_send_message, patch.object(
        syn, "getUserProfile", return_value=user_profile
    ) as mocked_get_user_profile:
        mocked_func = MagicMock()

        @notifyMe(syn, messageSubject=subject)
        def test_function():
            mocked_func()

        test_function()
        mocked_get_user_profile.assert_called_once()
        mocked_send_message.assert_called_once_with(
            [owner_id],
            subject,
            messageBody="Call to test_function completed successfully!",
        )


def test_notifyMe__exception_thrown_and_retry_fail(syn):
    subject = "some message subject"
    owner_id = "12434"
    user_profile = {"ownerId": owner_id}
    with patch.object(syn, "sendMessage") as mocked_send_message, patch.object(
        syn, "getUserProfile", return_value=user_profile
    ):
        mocked_func = MagicMock(
            side_effect=[Exception("first time fails"), "second time is Fine"]
        )

        @notifyMe(syn, messageSubject=subject, retries=1)
        def test_function():
            mocked_func()

        test_function()
        assert 2 == mocked_send_message.call_count

        # call_args_list is a list of tuples, each tuple in the form (args,kwargs)
        first_call_args = mocked_send_message.call_args_list[0][0]
        first_call_kwargs = mocked_send_message.call_args_list[0][1]

        second_call_args = mocked_send_message.call_args_list[1][0]
        second_call_kwargs = mocked_send_message.call_args_list[1][1]

        assert ([owner_id], subject) == first_call_args
        assert (
            "Encountered a temporary Failure during upload"
            in first_call_kwargs["messageBody"]
        )

        assert ([owner_id], subject) == first_call_args
        assert 1 == len(first_call_kwargs)
        assert (
            "Encountered a temporary Failure during upload"
            in first_call_kwargs["messageBody"]
        )

        assert ([owner_id], subject) == second_call_args
        assert 1 == len(second_call_kwargs)
        assert (
            "Call to test_function completed successfully!"
            == second_call_kwargs["messageBody"]
        )


def test_with_progress_bar():
    num_calls = 5
    mocked_function = MagicMock()

    progress_func = with_progress_bar(mocked_function, num_calls)

    with patch.object(
        synapseutils.monitor, "printTransferProgress"
    ) as mocked_progress_print:
        for i in range(num_calls):
            progress_func(i)
        mocked_progress_print.assert_has_calls(
            [call(float(i + 1), num_calls, "", "", False) for i in range(num_calls)]
        )
        mocked_function.assert_has_calls([call(i) for i in range(num_calls)])
