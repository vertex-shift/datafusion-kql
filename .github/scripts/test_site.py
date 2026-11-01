"""Regression tests for the GitHub Pages artifact contract."""

from pathlib import Path
import shutil
import tempfile
import tomllib
import unittest

from check_site import ROOT, validate


class SiteTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        shutil.copytree(ROOT / "site", self.root / "site")
        shutil.copytree(ROOT / "examples", self.root / "examples")
        shutil.copyfile(ROOT / "Cargo.toml", self.root / "Cargo.toml")

    def tearDown(self):
        self.directory.cleanup()

    def change(self, before, after):
        path = self.root / "site/index.html"
        text = path.read_text()
        self.assertIn(before, text)
        path.write_text(text.replace(before, after, 1))

    def test_shipped_site_is_valid(self):
        self.assertIn("Site contract passed", validate(self.root))

    def test_root_relative_assets_are_rejected(self):
        self.change("./assets/styles.css", "/assets/styles.css")
        with self.assertRaisesRegex(ValueError, "root-relative"):
            validate(self.root)

    def test_broken_anchors_are_rejected(self):
        self.change('href="#examples"', 'href="#missing"')
        with self.assertRaisesRegex(ValueError, "missing fragment"):
            validate(self.root)

    def test_stale_version_is_rejected(self):
        version = tomllib.loads((self.root / "Cargo.toml").read_text())["package"]["version"]
        series = ".".join(version.split(".")[:2])
        self.change(f"data-project-version>{series}</span>", "data-project-version>0.01</span>")
        with self.assertRaisesRegex(ValueError, "version"):
            validate(self.root)

    def test_external_runtime_assets_are_rejected(self):
        self.change("./assets/styles.css", "https://example.com/styles.css")
        with self.assertRaisesRegex(ValueError, "self-hosted"):
            validate(self.root)

    def test_missing_font_license_is_rejected(self):
        (self.root / "site/assets/fonts/Barlow-OFL.txt").unlink()
        with self.assertRaisesRegex(ValueError, "font license"):
            validate(self.root)

    def test_theme_defaults_to_system(self):
        self.change('value="system" selected', 'value="system"')
        self.change('value="light"', 'value="light" selected')
        with self.assertRaisesRegex(ValueError, "default to System"):
            validate(self.root)

    def test_both_system_color_schemes_are_required(self):
        self.change('name="color-scheme" content="light dark"', 'name="color-scheme" content="light"')
        with self.assertRaisesRegex(ValueError, "both system color schemes"):
            validate(self.root)

    def test_theme_bootstrap_cannot_be_deferred(self):
        self.change('src="./assets/theme.js"', 'src="./assets/theme.js" defer')
        with self.assertRaisesRegex(ValueError, "first styled paint"):
            validate(self.root)

    def test_theme_bootstrap_must_precede_styles(self):
        self.change(
            '<script src="./assets/theme.js"></script>\n    <link rel="stylesheet" href="./assets/styles.css">',
            '<link rel="stylesheet" href="./assets/styles.css">\n    <script src="./assets/theme.js"></script>',
        )
        with self.assertRaisesRegex(ValueError, "precede the stylesheet"):
            validate(self.root)


if __name__ == "__main__":
    unittest.main()
