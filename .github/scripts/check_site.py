"""Validate and preview the self-contained GitHub Pages project site."""

import argparse
from functools import partial
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import tomllib
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site"
BASE = "/datafusion-kql/"
CANONICAL = "https://vertex-shift.github.io/datafusion-kql/"


class Document(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()
        self.references = []
        self.local_resources = []
        self.headings = []
        self.canonical = None
        self.description = False
        self.language = None
        self.color_scheme = None
        self.theme_bootstrap = None
        self.theme_options = []
        self.in_theme_select = False
        self.errors = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "html":
            self.language = attrs.get("lang")
        if attrs.get("id"):
            if attrs["id"] in self.ids:
                self.errors.append(f"duplicate id: {attrs['id']}")
            self.ids.add(attrs["id"])
        if re.fullmatch(r"h[1-6]", tag):
            self.headings.append(int(tag[1]))
        if tag == "meta" and attrs.get("name") == "description":
            self.description = bool(attrs.get("content"))
        if tag == "meta" and attrs.get("name") == "color-scheme":
            self.color_scheme = attrs.get("content")
        if tag == "script" and attrs.get("src") == "./assets/theme.js":
            self.theme_bootstrap = attrs
        if tag == "select" and attrs.get("id") == "theme-select":
            self.in_theme_select = True
        if tag == "option" and self.in_theme_select:
            self.theme_options.append((attrs.get("value"), "selected" in attrs))
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonical = attrs.get("href")
        for attribute in ("href", "src"):
            if attrs.get(attribute):
                self.references.append(attrs[attribute])
        if tag in ("script", "link") and (
            tag == "script" or attrs.get("rel") in ("stylesheet", "preload", "icon")
        ):
            self.local_resources.append(attrs.get("src") or attrs.get("href"))
        if tag == "script" and not attrs.get("src"):
            self.errors.append("inline scripts are not part of the static-site contract")
        if tag == "a" and attrs.get("target") == "_blank":
            if "noopener" not in attrs.get("rel", "").split():
                self.errors.append("new-tab links must declare noopener")
        if tag == "img" and "alt" not in attrs:
            self.errors.append("images must have alt text")
        if tag == "th" and attrs.get("scope") not in ("row", "col"):
            self.errors.append("table headers must declare row or col scope")
        if tag == "button" and attrs.get("type") != "button":
            self.errors.append("interactive buttons must have an explicit type")
        if attrs.get("data-copy"):
            self.references.append("#" + attrs["data-copy"])

    def handle_endtag(self, tag):
        if tag == "select":
            self.in_theme_select = False


def validate(root=ROOT):
    root = Path(root)
    site = root / "site"
    errors = []
    html = (site / "index.html").read_text(encoding="utf-8")
    document = Document()
    document.feed(html)
    errors.extend(document.errors)
    if document.language != "en":
        errors.append("the site must declare its language")
    if document.headings.count(1) != 1:
        errors.append("the homepage must have exactly one h1")
    if any(right > left + 1 for left, right in zip(document.headings, document.headings[1:])):
        errors.append("heading levels must not skip")
    if not document.description:
        errors.append("the homepage must include a description")
    if document.canonical != CANONICAL:
        errors.append("canonical URL must match the GitHub Pages project URL")
    if document.color_scheme != "light dark":
        errors.append("the page must support both system color schemes")
    if document.theme_options != [("system", True), ("light", False), ("dark", False)]:
        errors.append("the theme selector must default to System with Light and Dark overrides")
    if document.local_resources.count("./assets/theme.js") != 1 or document.theme_bootstrap is None:
        errors.append("the page must load one theme bootstrap")
    elif any(attribute in document.theme_bootstrap for attribute in ("async", "defer")):
        errors.append("the theme bootstrap must run before the first styled paint")
    elif "./assets/styles.css" in document.local_resources:
        if document.local_resources.index("./assets/theme.js") > document.local_resources.index("./assets/styles.css"):
            errors.append("the theme bootstrap must precede the stylesheet")
    for reference in document.references:
        parsed = urlsplit(reference)
        if parsed.scheme:
            if parsed.scheme != "https":
                errors.append(f"unsupported external URL scheme: {reference}")
            continue
        if reference.startswith("//"):
            errors.append(f"protocol-relative URL: {reference}")
            continue
        if parsed.path:
            if parsed.path.startswith("/"):
                errors.append(f"root-relative URL breaks the project prefix: {reference}")
                continue
            target = (site / unquote(parsed.path)).resolve()
            if not target.is_relative_to(site.resolve()) or not target.exists():
                errors.append(f"missing or out-of-scope local target: {reference}")
        elif parsed.fragment and parsed.fragment not in document.ids:
            errors.append(f"missing fragment target: {reference}")
    for resource in document.local_resources:
        if not resource or urlsplit(resource).scheme or resource.startswith("//"):
            errors.append(f"runtime assets must be self-hosted: {resource}")
    css = (site / "assets/styles.css").read_text(encoding="utf-8")
    for reference in re.findall(r'url\(["\']?([^"\')]+)', css):
        target = (site / "assets" / reference).resolve()
        if not target.is_relative_to(site.resolve()) or not target.is_file():
            errors.append(f"missing local CSS asset: {reference}")
    if "prefers-reduced-motion" not in css:
        errors.append("the stylesheet must honor reduced motion")
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    series = ".".join(version.split(".")[:2])
    if f"data-project-version>{series}</span>" not in html:
        errors.append("displayed frontend version must match Cargo.toml")
    count = len(list((root / "examples").glob("*.rs")))
    if f"Explore {count} runnable examples" not in html:
        errors.append("example count must match the runnable source examples")
    for license_name in ("Barlow-OFL.txt", "BarlowCondensed-OFL.txt"):
        license_path = site / "assets/fonts" / license_name
        if not license_path.is_file() or "SIL OPEN FONT LICENSE" not in license_path.read_text():
            errors.append(f"missing font license: {license_name}")
    sitemap = ET.parse(site / "sitemap.xml")
    locations = [element.text for element in sitemap.iter() if element.tag.endswith("}loc")]
    if locations != [CANONICAL]:
        errors.append("sitemap must contain the project homepage")
    if f"Sitemap: {CANONICAL}sitemap.xml" not in (site / "robots.txt").read_text():
        errors.append("robots.txt must point to the correct project sitemap")
    if not (site / ".nojekyll").is_file():
        errors.append("the static artifact must include .nojekyll")
    for path in site.rglob("*"):
        if path.is_symlink():
            errors.append(f"Pages artifacts cannot contain symlinks: {path}")
    if errors:
        raise ValueError("\n".join(errors))
    return f"Site contract passed: local assets, metadata, anchors, version {series}, and {count} examples."


class PreviewHandler(SimpleHTTPRequestHandler):
    def _serve(self, head=False):
        path = urlsplit(self.path).path
        if path in ("/", BASE.rstrip("/")):
            self.send_response(302)
            self.send_header("Location", BASE)
            self.end_headers()
            return
        if not path.startswith(BASE):
            self.send_error(404)
            return
        self.path = self.path[len(BASE) - 1:]
        if head:
            super().do_HEAD()
        else:
            super().do_GET()

    def do_GET(self):
        self._serve()

    def do_HEAD(self):
        self._serve(head=True)

    def list_directory(self, path):
        self.send_error(404)
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--port", type=int, default=4173)
    args = parser.parse_args()
    print(validate(), flush=True)
    if args.serve:
        handler = partial(PreviewHandler, directory=str(SITE))
        with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
            print(f"Preview: http://127.0.0.1:{args.port}{BASE}", flush=True)
            server.serve_forever()


if __name__ == "__main__":
    main()
