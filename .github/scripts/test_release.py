import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

import release


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="kql-release-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "Cargo.toml").write_text(
            f"""[package]
name = "{release.PACKAGE}"
version = "0.8.0"
repository = "{release.REPOSITORY}"
license = "Apache-2.0"
publish = ["crates-io"]
[lib]
name = "datafusion_kql"
""",
            encoding="utf-8",
        )
        self.git("init", "--quiet", "--initial-branch=main")
        self.git("config", "user.name", "Release tests")
        self.git("config", "user.email", "release-tests@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", str(self.root / "disabled-hooks"))
        self.git("add", "Cargo.toml")
        self.git("commit", "--quiet", "-m", "fixture")
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.git("tag", "v0.8.0")

    def git(self, *args):
        return release.git(self.root, *args)

    def info(self):
        return release.metadata(self.root)

    def test_matching_clean_tag_on_main(self):
        info = release.validate_tag(self.root, self.info(), "v0.8.0")
        self.assertEqual(info["package"], release.PACKAGE)
        self.assertEqual(info["tag"], "v0.8.0")
        self.assertEqual(info["archive"], f"target/package/{release.PACKAGE}-0.8.0.crate")

    def test_release_tag_must_match_manifest(self):
        for tag in ["0.8.0", "v0.9.0", "v0.8.0\ninjected=output", "--help"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.validate_tag(self.root, self.info(), tag)

    def test_checkout_must_match_tag(self):
        (self.root / "change").write_text("later", encoding="utf-8")
        self.git("add", "change")
        self.git("commit", "--quiet", "-m", "later")
        with self.assertRaisesRegex(ValueError, "checkout does not match"):
            release.validate_tag(self.root, self.info(), "v0.8.0")

    def test_dirty_and_untracked_files_block_releases(self):
        (self.root / "untracked").write_text("not committed", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "clean"):
            release.validate_tag(self.root, self.info(), "v0.8.0")

    def test_unmerged_tags_block_releases(self):
        self.git("switch", "--quiet", "-c", "feature")
        (self.root / "change").write_text("feature", encoding="utf-8")
        self.git("add", "change")
        self.git("commit", "--quiet", "-m", "feature")
        self.git("tag", "--force", "v0.8.0")
        with self.assertRaisesRegex(ValueError, "merged into origin/main"):
            release.validate_tag(self.root, self.info(), "v0.8.0")

    def test_old_released_commit_can_be_ancestor_of_main(self):
        commit = self.git("rev-parse", "HEAD")
        (self.root / "later").write_text("main advanced", encoding="utf-8")
        self.git("add", "later")
        self.git("commit", "--quiet", "-m", "later")
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.git("checkout", "--quiet", "--detach", commit)
        release.validate_tag(self.root, self.info(), "v0.8.0")

    def test_package_name_import_name_and_registry_are_guarded(self):
        path = self.root / "Cargo.toml"
        original = path.read_text(encoding="utf-8")
        for old, new in [
            (release.PACKAGE, "datafusion-kql"),
            ('name = "datafusion_kql"', 'name = "different_import"'),
            ('publish = ["crates-io"]', "publish = false"),
            ('version = "0.8.0"', 'version = "0.8.0\\ninjected=value"'),
            ('license = "Apache-2.0"', 'license = "UNLICENSED"'),
        ]:
            with self.subTest(replacement=new):
                path.write_text(original.replace(old, new), encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.info()
        path.write_text(original, encoding="utf-8")

    def published(self, **changes):
        info = self.info()
        archive = self.root / info["archive"]
        archive.parent.mkdir(parents=True, exist_ok=True)
        archive.write_bytes(b"fixture archive")
        version = {
            "crate": info["package"],
            "num": info["version"],
            "repository": release.REPOSITORY,
            "checksum": hashlib.sha256(archive.read_bytes()).hexdigest(),
            **changes,
        }
        return info, json.dumps({"version": version}).encode()

    def test_registry_checksum_is_verified(self):
        info, response = self.published()
        with patch("release.urllib.request.urlopen", return_value=io.BytesIO(response)) as fetch:
            release.verify_published(self.root, info)
        self.assertIn(release.PACKAGE, fetch.call_args.args[0].full_url)
        info, response = self.published(checksum="wrong")
        with patch("release.urllib.request.urlopen", return_value=io.BytesIO(response)):
            with self.assertRaisesRegex(ValueError, "checksum"):
                release.verify_published(self.root, info)

    def test_registry_failures_are_not_silently_accepted(self):
        info, _ = self.published()
        failure = urllib.error.HTTPError("https://crates.io", 403, "forbidden", {}, None)
        with patch("release.urllib.request.urlopen", side_effect=failure):
            with self.assertRaises(urllib.error.HTTPError):
                release.verify_published(self.root, info)

    def test_registry_visibility_retry_is_bounded(self):
        info, response = self.published()
        missing = urllib.error.HTTPError("https://crates.io", 404, "not found", {}, None)
        with (
            patch("release.urllib.request.urlopen", side_effect=[missing, io.BytesIO(response)]),
            patch("release.time.sleep"),
        ):
            release.verify_published(self.root, info)
        with (
            patch("release.urllib.request.urlopen", side_effect=missing),
            patch("release.time.sleep") as sleep,
            self.assertRaises(urllib.error.HTTPError),
        ):
            release.verify_published(self.root, info, attempts=2)
        self.assertEqual(sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
