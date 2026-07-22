import datetime
from datetime import datetime as Datetime
from math import pi

from synapseclient.core.utils import from_unix_epoch_time
from synapseclient.models.submission_status import (
    _is_submission_status_annotations,
    _to_submission_annotations,
    _to_submission_status_annotations,
)


class TestIsSubmissionStatusAnnotations:
    def test_valid(self) -> None:
        assert _is_submission_status_annotations(
            {"objectId": "1", "scopeId": "2", "stringAnnos": []}
        )

    def test_invalid_has_foreign_key(self) -> None:
        assert not _is_submission_status_annotations({"foo": "bar"})

    def test_non_mapping(self) -> None:
        assert not _is_submission_status_annotations("not a mapping")


class TestToSubmissionStatusAnnotations:
    def test_categorizes_by_type(self) -> None:
        april_28_1969 = Datetime(1969, 4, 28, tzinfo=datetime.timezone.utc)
        annotations = {
            "screen_name": "Bullwinkle",
            "species": "Moose",
            "lucky": 13,
            "pi": pi,
            "birthday": april_28_1969,
        }
        sa = _to_submission_status_annotations(annotations)

        assert {"screen_name", "species"} == {kvp["key"] for kvp in sa["stringAnnos"]}
        assert {"lucky", "birthday"} == {kvp["key"] for kvp in sa["longAnnos"]}
        assert {"pi"} == {kvp["key"] for kvp in sa["doubleAnnos"]}

        for kvp in sa["longAnnos"]:
            if kvp["key"] == "lucky":
                assert kvp["value"] == 13
            if kvp["key"] == "birthday":
                assert from_unix_epoch_time(kvp["value"]) == april_28_1969

    def test_boolean_becomes_string(self) -> None:
        sa = _to_submission_status_annotations({"flag": True})
        assert sa["stringAnnos"][0]["key"] == "flag"
        assert sa["stringAnnos"][0]["value"] == "true"

    def test_is_private_flag(self) -> None:
        sa = _to_submission_status_annotations({"species": "Moose"}, is_private=False)
        assert sa["stringAnnos"][0]["isPrivate"] is False

    def test_idempotent(self) -> None:
        sa = _to_submission_status_annotations({"species": "Moose", "lucky": 13})
        assert sa == _to_submission_status_annotations(sa)


class TestToSubmissionAnnotations:
    def test_nested_type_format(self) -> None:
        result = _to_submission_annotations(
            id="9999999",
            etag="abc123",
            annotations={
                "score": [85],
                "feedback": ["Good work!"],
                "ratio": [1.5],
            },
        )
        assert result["id"] == "9999999"
        assert result["etag"] == "abc123"
        assert result["annotations"]["score"] == {"type": "LONG", "value": [85]}
        assert result["annotations"]["feedback"] == {
            "type": "STRING",
            "value": ["Good work!"],
        }
        assert result["annotations"]["ratio"] == {"type": "DOUBLE", "value": [1.5]}

    def test_scalar_value_is_wrapped_in_list(self) -> None:
        result = _to_submission_annotations(id="1", etag="e", annotations={"score": 85})
        assert result["annotations"]["score"] == {"type": "LONG", "value": [85]}

    def test_boolean_becomes_lowercase_string(self) -> None:
        result = _to_submission_annotations(
            id="1", etag="e", annotations={"passed": [True, False]}
        )
        assert result["annotations"]["passed"] == {
            "type": "STRING",
            "value": ["true", "false"],
        }

    def test_date_becomes_long_timestamp(self) -> None:
        birthday = Datetime(1969, 4, 28, tzinfo=datetime.timezone.utc)
        result = _to_submission_annotations(
            id="1", etag="e", annotations={"birthday": [birthday]}
        )
        assert result["annotations"]["birthday"]["type"] == "LONG"
        assert (
            from_unix_epoch_time(result["annotations"]["birthday"]["value"][0])
            == birthday
        )
