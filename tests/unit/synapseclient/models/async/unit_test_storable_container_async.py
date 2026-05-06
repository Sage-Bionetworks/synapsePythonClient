"""Unit tests for StorableContainer"""

import csv
import os
import platform
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models import File, Folder, Project
from synapseclient.models.services import manifest as manifest_module


def _write_manifest(rows: list[dict], tmp_path: Path) -> Path:
    """Write a minimal CSV manifest to a unique path under *tmp_path*."""
    path = tmp_path / f"{uuid.uuid4()}_manifest.csv"
    if not rows:
        return path
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames, restval="", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


class TestSyncToSynapse:
    """Tests for StorableContainer.sync_to_synapse_async that do not require a Synapse connection."""

    @pytest.fixture(autouse=True)
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_missing_path_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'path' column raises ValueError immediately."""
        manifest_path = _write_manifest([{"parentId": "syn123", "name": "x"}], tmp_path)
        project = Project(id="test", name="test")

        with pytest.raises(ValueError, match="'path'"):
            await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )

    async def test_missing_parent_id_column_raises(self, tmp_path: Path) -> None:
        """A manifest without a 'parentId' column raises ValueError immediately."""
        local_file = tmp_path / "f.txt"
        local_file.write_text("x")

        manifest_path = _write_manifest(
            [{"path": str(local_file), "name": "f.txt"}], tmp_path
        )
        project = Project(id="test", name="test")

        with pytest.raises(ValueError, match="'parentId'"):
            await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )

    async def test_all_rows_have_errors_no_crash(self, tmp_path: Path) -> None:
        """When every row has an error the call completes without uploading anything."""
        manifest_path = _write_manifest(
            [
                {
                    "path": "/nonexistent/file.txt",
                    "parentId": "syn123",
                    "name": "ignored.txt",
                    "error": "Some failure",
                }
            ],
            tmp_path,
        )
        project = Project(id="test", name="test")

        with patch(
            "synapseclient.models.mixins.storable_container.upload_sync_files"
        ) as mock_upload:
            result = await project.sync_to_synapse_async(
                manifest_path=str(manifest_path), synapse_client=self.syn
            )
            mock_upload.assert_not_called()
            assert result == []


class TestGenerateSyncManifest:
    """Tests for StorableContainer.generate_sync_manifest_async."""

    async def test_unstored_container_raises(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """Calling the method on a container whose id is None is a caller bug
        and must surface as a ValueError rather than a confusing downstream
        error. The integration suite cannot exercise this case because every
        container fixture has already been stored."""
        # GIVEN a local directory with one file
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        # WHEN generate_sync_manifest_async is called on a Project that has
        # never been stored (id is None)
        # THEN a ValueError is raised explaining the container is not stored
        with pytest.raises(ValueError, match="has not been stored in Synapse"):
            await Project(name="unstored").generate_sync_manifest_async(
                directory_path=str(src),
                manifest_path=str(manifest),
                synapse_client=syn,
            )

    async def test_empty_directory_logs_warning(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """An empty directory logs a 'No uploadable files found' warning so
        the caller knows why their manifest is empty. The integration suite
        covers the resulting header-only file content."""
        # GIVEN an empty local directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        manifest = tmp_path / "manifest.csv"

        # WHEN we generate a manifest for it (parent-id validation is stubbed
        # so this stays offline)
        with (
            patch(
                "synapseclient.models.services.manifest._validate_target_container_async",
                new=AsyncMock(return_value=None),
            ),
            patch.object(syn.logger, "warning") as mock_warning,
        ):
            await Folder(id="syn1").generate_sync_manifest_async(
                directory_path=str(empty_dir),
                manifest_path=str(manifest),
                synapse_client=syn,
            )

        # THEN a "no uploadable files" warning is logged so the caller knows
        # why their manifest is empty
        assert any(
            "No uploadable files found" in call.args[0]
            for call in mock_warning.call_args_list
        )

    @pytest.mark.parametrize(
        "kind",
        ["missing-path", "file-path"],
    )
    async def test_invalid_directory_path_raises(
        self, tmp_path: Path, syn: Synapse, kind: str
    ) -> None:
        """The public surface raises ValueError for both a non-existent path
        and a regular file path, and writes no manifest. The integration suite
        only exercises the missing-path case, so the file-path case is covered
        here."""
        # GIVEN either a non-existent path or a regular file path
        if kind == "missing-path":
            path = tmp_path / "does_not_exist"
        else:
            path = tmp_path / "a.txt"
            path.write_text("hello")
        manifest = tmp_path / "manifest.csv"

        # WHEN we try to generate a manifest for it
        # THEN a ValueError is raised
        with pytest.raises(ValueError, match="is not a directory or does not exist"):
            await Folder(id="syn1").generate_sync_manifest_async(
                directory_path=str(path),
                manifest_path=str(manifest),
                synapse_client=syn,
            )

        # AND no manifest file is written (failure happens before any output)
        assert not manifest.exists()

    async def test_parent_id_validated_upfront(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """The container's id is validated via _validate_target_container_async
        before the directory is walked. This ordering invariant cannot be
        verified through the live API; integration tests can only observe the
        end state."""
        # GIVEN a directory with one file
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        manifest = tmp_path / "manifest.csv"

        # WHEN we generate a manifest under a Folder with id "synROOT" with
        # the parent-id validation stubbed so it stays offline
        with patch(
            "synapseclient.models.services.manifest._validate_target_container_async",
            new_callable=AsyncMock,
        ) as mock_validate:
            await Folder(id="synROOT").generate_sync_manifest_async(
                directory_path=str(src),
                manifest_path=str(manifest),
                synapse_client=syn,
            )

        # THEN _validate_target_container_async was awaited exactly once
        # with that id, ensuring the parent check happens before traversal
        mock_validate.assert_awaited_once_with("synROOT", client=syn)


class TestResolveAndValidateDirectoryPath:
    """Tests for manifest._resolve_and_validate_directory_path_async."""

    async def test_valid_directory_returns_realpath(self, tmp_path: Path) -> None:
        """A valid directory returns the realpath-resolved absolute path."""
        # GIVEN a real directory on disk
        src = tmp_path / "src"
        src.mkdir()

        # WHEN we validate the directory path
        result = await manifest_module._resolve_and_validate_directory_path_async(
            directory_path=str(src)
        )

        # THEN the realpath-resolved absolute path is returned
        assert result == os.path.realpath(str(src))

    @pytest.mark.parametrize(
        "kind",
        ["missing-path", "file-path"],
    )
    async def test_non_directory_path_raises(self, tmp_path: Path, kind: str) -> None:
        """Both a non-existent path and a regular file path raise ValueError."""
        # GIVEN either a non-existent path or a regular file path
        if kind == "missing-path":
            path = tmp_path / "missing"
        else:
            path = tmp_path / "f.txt"
            path.write_text("hello")

        # WHEN we validate the directory path
        # THEN a ValueError is raised explaining the path is not a directory
        with pytest.raises(ValueError, match="is not a directory or does not exist"):
            await manifest_module._resolve_and_validate_directory_path_async(
                directory_path=str(path)
            )

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Symlink creation requires elevated privileges on Windows.",
    )
    async def test_symlink_resolved_to_target(self, tmp_path: Path) -> None:
        """If directory_path is a symlink, the resolved target path is returned
        so the manifest survives the original symlink being removed/redirected."""
        # GIVEN a target directory and a symlink pointing to it
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        os.symlink(str(target), str(link))

        # WHEN we validate the symlink path
        result = await manifest_module._resolve_and_validate_directory_path_async(
            directory_path=str(link)
        )

        # THEN the realpath of the symlink target is returned (not the link)
        assert result == os.path.realpath(str(target))


class TestValidateTargetContainer:
    """Tests for manifest._validate_target_container_async."""

    @pytest.mark.parametrize(
        "container",
        [
            pytest.param(Folder(id="syn1", name="folder"), id="folder"),
            pytest.param(Project(id="syn1", name="project"), id="project"),
        ],
    )
    async def test_container_passes(
        self, syn: Synapse, container: Folder | Project
    ) -> None:
        """Both Folder and Project pass validation"""
        # GIVEN get_async is stubbed to return either a Folder or Project
        with patch.object(
            manifest_module,
            "get_async",
            new_callable=AsyncMock,
            return_value=container,
        ):
            # WHEN we validate the target container by id
            result = await manifest_module._validate_target_container_async(
                "syn1", client=syn
            )
        # THEN validation returns None (no error raised)
        assert result is None

    async def test_non_container_raises_value_error(self, syn: Synapse) -> None:
        """A File (or any non-container) raises ValueError naming the id."""
        # GIVEN get_async is stubbed to return a File (non-container) entity
        not_container = File(id="syn1", name="file")
        with patch.object(
            manifest_module,
            "get_async",
            new_callable=AsyncMock,
            return_value=not_container,
        ):
            # WHEN we validate the target container by id
            # THEN a ValueError is raised naming the id and rejecting the type
            with pytest.raises(
                ValueError, match=r"Container syn1 is not a Folder or Project"
            ):
                await manifest_module._validate_target_container_async(
                    "syn1", client=syn
                )


class TestCollectManifestRows:
    """Tests for manifest._collect_manifest_rows_async."""

    async def test_empty_directory_returns_empty_list(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """An empty directory produces no rows."""
        # GIVEN an empty local directory
        empty = tmp_path / "empty"
        empty.mkdir()

        # WHEN we collect manifest rows for it
        rows = await manifest_module._collect_manifest_rows_async(
            directory_path=str(empty), parent_id="syn1", client=syn
        )

        # THEN the result is an empty list
        assert rows == []

    async def test_flat_directory_uses_root_parent_id(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """Files at the top level all share the supplied parent_id and are
        emitted in sorted order. No Synapse folders are created when the
        local tree has no subdirectories."""
        # GIVEN a flat directory containing two files in non-sorted order
        src = tmp_path / "src"
        src.mkdir()
        (src / "b.txt").write_text("two")
        (src / "a.txt").write_text("one")

        # WHEN we collect manifest rows under a root parent_id
        with patch(
            "synapseclient.models.Folder.store_async",
            new_callable=AsyncMock,
        ) as mock_store_async:
            rows = await manifest_module._collect_manifest_rows_async(
                directory_path=str(src), parent_id="synROOT", client=syn
            )

        # THEN the rows are emitted in sorted filename order
        assert [os.path.basename(row["path"]) for row in rows] == ["a.txt", "b.txt"]
        # AND every row uses the root parent_id
        assert all(row["parentId"] == "synROOT" for row in rows)
        # AND no Synapse folders are created (no subdirectories to mirror)
        mock_store_async.assert_not_called()

    async def test_nested_directories_use_created_folder_ids(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """Files in nested subdirectories receive the id of the Synapse folder
        created for their innermost local directory."""
        # GIVEN a nested directory tree with one file at the deepest level
        src = tmp_path / "src"
        (src / "level1" / "level2").mkdir(parents=True)
        (src / "level1" / "level2" / "deep.txt").write_text("content")

        # AND a fake _create_child_folders_async that returns Folders with
        # deterministic ids (syn_<dirname>) so the test can verify how the
        # production code threads parent ids from one walk iteration to
        # the next. Records (parent_id, dirnames) per call to assert call
        # order AND that each call's parent_id is the fake id assigned to
        # the previously-created folder.
        observed: list[tuple[str, list[str]]] = []

        async def fake_create_child_folders_async(
            parent_id: str, dirnames: list[str], client: Synapse
        ) -> dict[str, Folder]:
            observed.append((parent_id, list(dirnames)))
            return {d: Folder(name=d, id=f"syn_{d}") for d in dirnames}

        # WHEN we collect manifest rows under the root parent_id
        with patch.object(
            manifest_module,
            "_create_child_folders_async",
            new=fake_create_child_folders_async,
        ):
            rows = await manifest_module._collect_manifest_rows_async(
                directory_path=str(src), parent_id="synROOT", client=syn
            )

        # AND folders are created top-down: level1 under root, then level2
        # under the freshly-created level1 (id syn_level1), and a final
        # no-op call for the leaf directory.
        assert observed == [
            ("synROOT", ["level1"]),
            ("syn_level1", ["level2"]),
            ("syn_level2", []),
        ]
        # AND the deep file's parentId is the innermost folder's id
        assert rows == [
            {
                "path": os.path.join(str(src), "level1", "level2", "deep.txt"),
                "parentId": "syn_level2",
            }
        ]

    async def test_mixed_flat_and_nested_uses_correct_parent_ids(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """Files at the root use the supplied parent_id while files inside
        subdirectories use the id of the folder created for their containing
        directory. The flat-only and nested-only tests each cover one branch
        in isolation; this combined case asserts both work together in a
        single walk."""
        # GIVEN a tree with one file at the root AND a subdir with a file
        src = tmp_path / "src"
        src.mkdir()
        (src / "root.txt").write_text("root-data")
        (src / "child").mkdir()
        (src / "child" / "nested.txt").write_text("nested-data")

        # AND a fake _create_child_folders_async that assigns deterministic
        # ids so the test can distinguish root vs. nested rows by parentId
        async def fake_create_child_folders_async(
            parent_id: str, dirnames: list[str], client: Synapse
        ) -> dict[str, Folder]:
            return {d: Folder(name=d, id=f"syn_{d}") for d in dirnames}

        # WHEN we collect manifest rows under the root parent_id
        with patch.object(
            manifest_module,
            "_create_child_folders_async",
            new=fake_create_child_folders_async,
        ):
            rows = await manifest_module._collect_manifest_rows_async(
                directory_path=str(src), parent_id="synROOT", client=syn
            )

        # THEN the root-level file uses the root parent_id and the nested
        # file uses the id assigned to its containing folder
        by_basename = {os.path.basename(r["path"]): r["parentId"] for r in rows}
        assert by_basename == {
            "root.txt": "synROOT",
            "nested.txt": "syn_child",
        }

    async def test_multiple_siblings_share_parent_id_in_one_batch(
        self, tmp_path: Path, syn: Synapse
    ) -> None:
        """Sibling directories at the same depth are passed to
        _create_child_folders_async in a single batch (one call), all sharing
        the same parent_id."""
        # GIVEN three sibling directories at the same depth, each with a
        # uniquely-named file so rows can be matched back to their folder
        src = tmp_path / "src"
        src.mkdir()
        for name in ("alpha", "beta", "gamma"):
            (src / name).mkdir()
            (src / name / f"{name}.txt").write_text("data")

        # AND a fake _create_child_folders_async that records each call's
        # (parent_id, dirnames) so we can assert there's exactly one batch
        # for the three siblings
        observed: list[tuple[str, list[str]]] = []

        async def fake_create_child_folders_async(
            parent_id: str, dirnames: list[str], client: Synapse
        ) -> dict[str, Folder]:
            observed.append((parent_id, list(dirnames)))
            return {d: Folder(name=d, id=f"syn_{d}") for d in dirnames}

        # WHEN we collect manifest rows under the root parent_id
        with patch.object(
            manifest_module,
            "_create_child_folders_async",
            new=fake_create_child_folders_async,
        ):
            rows = await manifest_module._collect_manifest_rows_async(
                directory_path=str(src), parent_id="synROOT", client=syn
            )

        # THEN the first call passes all three siblings in sorted order
        # under the root parent_id (one batch, not three)
        assert observed[0] == ("synROOT", ["alpha", "beta", "gamma"])
        # AND each subsequent call (one per sibling, for its own children)
        # passes an empty dirnames list because each sibling is a leaf dir
        assert all(call[1] == [] for call in observed[1:])
        # AND every file's parentId matches the id of its containing folder
        assert {os.path.basename(r["path"]): r["parentId"] for r in rows} == {
            "alpha.txt": "syn_alpha",
            "beta.txt": "syn_beta",
            "gamma.txt": "syn_gamma",
        }


class TestLogWalkError:
    """Tests for manifest._log_walk_error."""

    def test_logs_warning_with_filename_and_message(self, syn: Synapse) -> None:
        """The warning message references the offending filename and the
        underlying OSError representation."""
        # GIVEN an OSError carrying an errno, message, and filename
        err = OSError(13, "permission denied", "/tmp/unreadable")

        # WHEN _log_walk_error is invoked with that error
        with patch.object(syn.logger, "warning") as mock_warning:
            manifest_module._log_walk_error(syn, err)

        # THEN logger.warning is called exactly once
        mock_warning.assert_called_once()
        # AND the message references the offending filename and OSError text
        message = mock_warning.call_args.args[0]
        assert "/tmp/unreadable" in message
        assert "permission denied" in message

    def test_logs_warning_with_no_filename(self, syn: Synapse) -> None:
        """An OSError without a filename still produces a warning rather than
        raising (best-effort during traversal)."""
        # GIVEN an OSError without a filename attribute
        err = OSError("io failure")

        # WHEN _log_walk_error is invoked with that error
        with patch.object(syn.logger, "warning") as mock_warning:
            manifest_module._log_walk_error(syn, err)

        # THEN logger.warning is called once (no exception is raised)
        mock_warning.assert_called_once()


class TestCreateChildFolders:
    """Tests for manifest._create_child_folders_async."""

    async def test_empty_dirnames_returns_empty_dict(self, syn: Synapse) -> None:
        """No dirnames produces no folders and never invokes Folder.store_async."""
        # GIVEN an empty list of dirnames
        dirnames = []
        # WHEN we ask for child folders
        with patch(
            "synapseclient.models.Folder.store_async",
            new_callable=AsyncMock,
        ) as mock_store_async:
            result = await manifest_module._create_child_folders_async(
                parent_id="synROOT", dirnames=dirnames, client=syn
            )

        # THEN the result is an empty dict and no Synapse calls were made
        assert result == {}
        mock_store_async.assert_not_called()

    async def test_returns_dirname_to_folder_mapping(self, syn: Synapse) -> None:
        """Each input dirname maps to the Folder returned by store_async, all
        sharing the supplied parent_id, regardless of completion order."""
        # GIVEN three sibling dirnames to create under a single parent
        dirnames = ["alpha", "beta", "gamma"]

        # AND a fake Folder.store_async that assigns a deterministic id per
        # call so the returned Folders can be asserted against by id.
        # Patched onto Folder.store_async as an unbound method, so the bound
        # Folder instance arrives as `self`. Mutating self.id and returning
        # self matches the real signature without hitting the network.
        observed_parent_ids: list[str] = []

        async def fake_store_async(self: Any, *args: Any, **kwargs: Any) -> Any:
            observed_parent_ids.append(self.parent_id)
            self.id = f"syn_{self.name}"
            return self

        # WHEN we create the child folders concurrently
        with patch(
            "synapseclient.models.Folder.store_async",
            new=fake_store_async,
        ):
            result = await manifest_module._create_child_folders_async(
                parent_id="synROOT", dirnames=dirnames, client=syn
            )

        # THEN every dirname maps to its own Folder with the expected id
        assert set(result.keys()) == set(dirnames)
        for dirname in dirnames:
            assert result[dirname].name == dirname
            assert result[dirname].id == f"syn_{dirname}"

        # AND every store call used the same parent_id
        assert observed_parent_ids == ["synROOT"] * len(dirnames)


class TestPruneSymlinksAndSortDirnames:
    """Tests for manifest._prune_symlinks_and_sort_dirnames."""

    def test_sorts_dirnames_in_place(self, tmp_path: Path) -> None:
        """Plain directories are sorted in place; the original list object is
        mutated (os.walk relies on identity, not return value)."""
        # GIVEN three real directories and an unsorted dirnames list referring to them
        for name in ("c", "a", "b"):
            (tmp_path / name).mkdir()
        dirnames = ["c", "a", "b"]

        # WHEN we prune and sort
        manifest_module._prune_symlinks_and_sort_dirnames(dirnames, str(tmp_path))

        # AND the caller's list is sorted alphabetically
        assert dirnames == ["a", "b", "c"]

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Symlink creation requires elevated privileges on Windows.",
    )
    def test_drops_symlinked_subdirectories(self, tmp_path: Path) -> None:
        """Symlinked subdirectories are pruned so we don't mirror folders whose
        contents os.walk(followlinks=False) won't visit."""
        # GIVEN one real subdirectory and one symlink pointing at another
        # directory, both listed in dirnames
        real = tmp_path / "real"
        real.mkdir()
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        os.symlink(str(target), str(link))

        dirnames = ["link", "real"]

        # WHEN we prune and sort
        manifest_module._prune_symlinks_and_sort_dirnames(dirnames, str(tmp_path))

        # THEN only the real directory remains; the symlink was dropped
        assert dirnames == ["real"]

    def test_empty_list_unchanged(self, tmp_path: Path) -> None:
        """An empty list remains empty."""
        # GIVEN an empty dirnames list
        dirnames: list[str] = []
        # WHEN we prune and sort
        manifest_module._prune_symlinks_and_sort_dirnames(dirnames, str(tmp_path))
        # THEN it is still empty
        assert dirnames == []

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Symlink creation requires elevated privileges on Windows.",
    )
    def test_prunes_symlinks_and_sorts_in_one_call(self, tmp_path: Path) -> None:
        """Symlinks are pruned AND the surviving entries are sorted in a
        single call. The prune-only and sort-only tests cover each behavior
        in isolation, so this guards against a future change where one step
        accidentally undoes the other (e.g., sort happening before prune
        and reintroducing the link, or prune leaving the list in walk
        order)."""
        # GIVEN two real subdirs and one symlinked subdir, in non-sorted
        # order with the symlink between them
        for name in ("c", "a"):
            (tmp_path / name).mkdir()
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        os.symlink(str(target), str(link))

        dirnames = ["c", "link", "a"]

        # WHEN we prune and sort
        manifest_module._prune_symlinks_and_sort_dirnames(dirnames, str(tmp_path))

        # THEN the symlink is gone and the remaining real dirs are sorted
        assert dirnames == ["a", "c"]


class TestBuildManifestRows:
    """Tests for manifest._build_manifest_rows."""

    def test_returns_rows_in_sorted_order(self, tmp_path: Path, syn: Synapse) -> None:
        """Filenames are visited in sorted order so manifest output is
        deterministic regardless of filesystem yield order."""
        # GIVEN three real files and an unsorted filenames list
        for name in ("c.txt", "a.txt", "b.txt"):
            (tmp_path / name).write_text("data")

        # WHEN we build manifest rows under "syn1"
        rows = manifest_module._build_manifest_rows(
            dirpath=str(tmp_path),
            filenames=["c.txt", "a.txt", "b.txt"],
            parent_id="syn1",
            client=syn,
        )

        # THEN the rows are emitted in sorted filename order
        assert [os.path.basename(row["path"]) for row in rows] == [
            "a.txt",
            "b.txt",
            "c.txt",
        ]
        # AND every row uses the supplied parent_id
        assert all(row["parentId"] == "syn1" for row in rows)

    def test_skips_zero_byte_and_missing(self, tmp_path: Path, syn: Synapse) -> None:
        """Zero-byte files and unreadable/missing files are dropped via
        _is_uploadable_file."""
        # GIVEN one real non-empty file and one zero-byte file (and a
        # filename for a file that doesn't exist on disk)
        (tmp_path / "ok.txt").write_text("hello")
        (tmp_path / "empty.txt").write_text("")

        # WHEN we build manifest rows for all three filenames
        with patch.object(syn.logger, "warning"):
            rows = manifest_module._build_manifest_rows(
                dirpath=str(tmp_path),
                filenames=["ok.txt", "empty.txt", "missing.txt"],
                parent_id="syn1",
                client=syn,
            )

        # THEN only the non-empty real file produces a row; the zero-byte
        # and missing files are filtered out
        assert [os.path.basename(row["path"]) for row in rows] == ["ok.txt"]

    def test_empty_filenames_returns_empty(self, tmp_path: Path, syn: Synapse) -> None:
        """No filenames produce no rows."""
        # GIVEN an empty filenames list
        # WHEN we build manifest rows
        rows = manifest_module._build_manifest_rows(
            dirpath=str(tmp_path),
            filenames=[],
            parent_id="syn1",
            client=syn,
        )
        # THEN the result is empty
        assert rows == []


class TestIsUploadableFile:
    """Tests for manifest._is_uploadable_file."""

    def test_regular_file_is_uploadable(self, tmp_path: Path, syn: Synapse) -> None:
        """A non-empty readable file returns True."""
        # GIVEN a regular non-empty readable file
        f = tmp_path / "ok.txt"
        f.write_text("hello")
        # THEN the result is True
        assert manifest_module._is_uploadable_file(str(f), syn) is True

    def test_zero_byte_file_skipped(self, tmp_path: Path, syn: Synapse) -> None:
        """A zero-byte file is skipped because Synapse rejects empty uploads."""
        # GIVEN a zero-byte file
        f = tmp_path / "empty.txt"
        f.write_text("")
        # THEN the result is False (Synapse rejects empty uploads)
        assert manifest_module._is_uploadable_file(str(f), syn) is False

    def test_missing_file_skipped(self, tmp_path: Path, syn: Synapse) -> None:
        """A path that doesn't exist (e.g. broken symlink, race) is skipped
        rather than raising OSError up to the caller."""
        # GIVEN a path that doesn't exist on disk (e.g. broken symlink,
        # race against rmdir)
        missing = tmp_path / "nope.txt"
        # THEN the result is False rather than raising OSError
        assert manifest_module._is_uploadable_file(str(missing), syn) is False
