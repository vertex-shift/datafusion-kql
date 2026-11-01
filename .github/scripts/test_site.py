"""Regression tests for the GitHub Pages artifact contract."""

import json
from pathlib import Path
import re
import shutil
import struct
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

    def change_schema(self, update):
        path = self.root / "site/index.html"
        text = path.read_text()
        match = re.search(r'<script type="application/ld\+json">(.*?)</script>', text, re.S)
        self.assertIsNotNone(match)
        data = json.loads(match.group(1))
        update(data)
        path.write_text(text[:match.start(1)] + json.dumps(data) + text[match.end(1):])

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

    def test_search_and_social_titles_must_match(self):
        self.change(
            'property="og:title" content="Native KQL for Apache DataFusion | datafusion-kql"',
            'property="og:title" content="Different project"',
        )
        with self.assertRaisesRegex(ValueError, "SEO metadata: og:title"):
            validate(self.root)

    def test_noindex_is_rejected(self):
        self.change("index, follow, max-image-preview:large", "noindex, follow, max-image-preview:large")
        with self.assertRaisesRegex(ValueError, "permit indexing"):
            validate(self.root)

    def test_empty_robots_metadata_is_rejected(self):
        self.change('name="robots" content="index, follow, max-image-preview:large"', 'name="robots" content')
        with self.assertRaisesRegex(ValueError, "permit indexing"):
            validate(self.root)

    def test_social_images_must_use_the_public_project_prefix(self):
        self.change(
            'property="og:image" content="https://vertex-shift.github.io/datafusion-kql/assets/social-preview.png"',
            'property="og:image" content="https://vertex-shift.github.io/assets/social-preview.png"',
        )
        with self.assertRaisesRegex(ValueError, "SEO metadata: og:image"):
            validate(self.root)

    def test_social_image_alt_is_required(self):
        self.change(
            'property="og:image:alt" content="datafusion-kql: native KQL text compiled to DataFusion plans and Arrow execution."',
            'property="og:image:alt" content=""',
        )
        with self.assertRaisesRegex(ValueError, "alt descriptions"):
            validate(self.root)

    def test_social_image_dimensions_are_checked_from_the_png(self):
        path = self.root / "site/assets/social-preview.png"
        image = path.read_bytes()
        path.write_bytes(image[:16] + struct.pack(">II", 640, 360) + image[24:])
        with self.assertRaisesRegex(ValueError, "exactly 1200x630"):
            validate(self.root)

    def test_social_image_budget_is_enforced(self):
        path = self.root / "site/assets/social-preview.png"
        path.write_bytes(path.read_bytes() + bytes(300 * 1024))
        with self.assertRaisesRegex(ValueError, "300 KiB"):
            validate(self.root)

    def test_missing_social_image_is_rejected(self):
        (self.root / "site/assets/social-preview.png").unlink()
        with self.assertRaisesRegex(ValueError, "self-hosted PNG"):
            validate(self.root)

    def test_changed_social_source_requires_regeneration(self):
        path = self.root / "site/assets/social-preview.svg"
        path.write_text(path.read_text().replace("Native KQL.", "Changed KQL.", 1))
        with self.assertRaisesRegex(ValueError, "regenerated"):
            validate(self.root)

    def test_malformed_jsonld_is_rejected(self):
        self.change('"@context": "https://schema.org"', '"@context":')
        with self.assertRaisesRegex(ValueError, "invalid JSON-LD"):
            validate(self.root)

    def test_inline_executable_scripts_remain_prohibited(self):
        self.change("</head>", '<script>alert("fixture");</script></head>')
        with self.assertRaisesRegex(ValueError, "inline executable"):
            validate(self.root)

    def test_jsonld_cannot_use_an_external_script(self):
        self.change('type="application/ld+json"', 'type="application/ld+json" src="https://example.com/data.json"')
        with self.assertRaisesRegex(ValueError, "embedded non-executable"):
            validate(self.root)

    def test_library_schema_does_not_claim_app_rich_results(self):
        self.change_schema(lambda data: data["@graph"][2].update({"@type": "SoftwareApplication"}))
        with self.assertRaisesRegex(ValueError, "software-app rich result"):
            validate(self.root)

    def test_schema_version_must_match_cargo(self):
        self.change_schema(lambda data: data["@graph"][2].update({"version": "0.0.0"}))
        with self.assertRaisesRegex(ValueError, "source package: version"):
            validate(self.root)

    def test_schema_identities_must_match_the_canonical_site(self):
        self.change_schema(lambda data: data["@graph"][1].update({"url": "https://example.com/"}))
        with self.assertRaisesRegex(ValueError, "identities and URLs"):
            validate(self.root)

    def test_schema_cannot_invent_ratings(self):
        self.change_schema(lambda data: data["@graph"][2].update({
            "aggregateRating": {"@type": "AggregateRating", "ratingValue": 5, "ratingCount": 100}
        }))
        with self.assertRaisesRegex(ValueError, "cannot invent ratings"):
            validate(self.root)

    def test_schema_cannot_claim_upstream_publisher_affiliation(self):
        self.change_schema(lambda data: data["@graph"][0]["publisher"].update({"name": "Apache Software Foundation"}))
        with self.assertRaisesRegex(ValueError, "publisher"):
            validate(self.root)

    def test_package_homepage_must_point_to_the_project_site(self):
        path = self.root / "Cargo.toml"
        path.write_text(path.read_text().replace(
            'homepage = "https://vertex-shift.github.io/datafusion-kql/"',
            'homepage = "https://example.com/"',
        ))
        with self.assertRaisesRegex(ValueError, "package homepage"):
            validate(self.root)


if __name__ == "__main__":
    unittest.main()
