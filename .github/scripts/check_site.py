"""Validate and preview the self-contained GitHub Pages project site."""

import argparse
from functools import partial
import hashlib
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import struct
import tomllib
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site"
BASE = "/datafusion-kql/"
CANONICAL = "https://vertex-shift.github.io/datafusion-kql/"
SOCIAL_IMAGE = CANONICAL + "assets/social-preview.png"
SOCIAL_FONTS = (
    "barlow-regular.woff2",
    "barlow-semibold.woff2",
    "barlow-condensed-semibold.woff2",
)


class Document(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()
        self.references = []
        self.local_resources = []
        self.headings = []
        self.canonical = None
        self.description = False
        self.metadata = {}
        self.title = []
        self.title_count = 0
        self.in_title = False
        self.jsonld = []
        self.jsonld_parts = None
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
        if tag == "meta" and (key := attrs.get("property") or attrs.get("name")):
            if key != "theme-color":
                if key in self.metadata:
                    self.errors.append(f"duplicate metadata: {key}")
                self.metadata[key] = attrs.get("content") or ""
        if tag == "title":
            self.title_count += 1
            self.in_title = True
        is_jsonld = tag == "script" and attrs.get("type") == "application/ld+json"
        if is_jsonld:
            self.jsonld_parts = []
            if attrs.get("src"):
                self.errors.append("JSON-LD must be embedded non-executable data")
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
        ) and not is_jsonld:
            self.local_resources.append(attrs.get("src") or attrs.get("href"))
        if tag == "script" and not attrs.get("src") and not is_jsonld:
            self.errors.append("inline executable scripts are not part of the static-site contract")
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
        if tag == "title":
            self.in_title = False
        if tag == "script" and self.jsonld_parts is not None:
            try:
                self.jsonld.append(json.loads("".join(self.jsonld_parts)))
            except json.JSONDecodeError as error:
                self.errors.append(f"invalid JSON-LD: {error.msg}")
            self.jsonld_parts = None

    def handle_data(self, data):
        if self.in_title:
            self.title.append(data)
        if self.jsonld_parts is not None:
            self.jsonld_parts.append(data)


def social_fingerprint(site):
    digest = hashlib.sha256()
    inputs = [site / "assets/social-preview.svg"]
    inputs.extend(site / "assets/fonts" / name for name in SOCIAL_FONTS)
    for path in inputs:
        data = path.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def validate_seo(document, site, package, errors):
    title = "".join(document.title).strip()
    description = document.metadata.get("description", "")
    if document.title_count != 1 or not all(word in title for word in ("KQL", "Apache DataFusion", "datafusion-kql")):
        errors.append("the single search title must identify native KQL, Apache DataFusion and the project")
    for key, expected in {
        "og:type": "website",
        "og:site_name": "datafusion-kql",
        "og:locale": "en_US",
        "og:title": title,
        "twitter:title": title,
        "og:description": description,
        "twitter:description": description,
        "og:url": CANONICAL,
        "og:image": SOCIAL_IMAGE,
        "twitter:image": SOCIAL_IMAGE,
        "og:image:type": "image/png",
        "og:image:width": "1200",
        "og:image:height": "630",
        "twitter:card": "summary_large_image",
    }.items():
        if not expected or document.metadata.get(key) != expected:
            errors.append(f"missing or inconsistent SEO metadata: {key}")
    if not document.metadata.get("og:image:alt") or (
        document.metadata.get("og:image:alt") != document.metadata.get("twitter:image:alt")
    ):
        errors.append("social preview alt descriptions must be present and consistent")
    robots = {value.strip().lower() for value in document.metadata.get("robots", "").split(",")}
    if not {"index", "follow", "max-image-preview:large"}.issubset(robots) or (
        robots & {"noindex", "nofollow", "none", "noimageindex"}
    ):
        errors.append("the page must permit indexing, links and large image previews")
    if "./sitemap.xml" not in document.references:
        errors.append("the sitemap must be discoverable from the homepage")
    if package.get("homepage") != CANONICAL:
        errors.append("the package homepage must point to the canonical project site")

    image_path = site / "assets/social-preview.png"
    if not image_path.is_file():
        errors.append("the social preview must be a self-hosted PNG")
    else:
        image = image_path.read_bytes()
        if len(image) > 300 * 1024:
            errors.append("the social preview exceeds the 300 KiB image budget")
        if len(image) < 24 or image[:8] != b"\x89PNG\r\n\x1a\n" or image[12:16] != b"IHDR":
            errors.append("the social preview must be a valid PNG")
        elif struct.unpack(">II", image[16:24]) != (1200, 630):
            errors.append("the social preview must be exactly 1200x630")
        try:
            fingerprint = social_fingerprint(site)
        except FileNotFoundError:
            errors.append("the social preview source or local fonts are missing")
        else:
            if b"SourceFingerprint\0" + fingerprint.encode("ascii") not in image:
                errors.append("the social preview must be regenerated from its current SVG and fonts")

    if len(document.jsonld) != 1 or not isinstance(document.jsonld[0], dict):
        errors.append("the page must have one JSON-LD graph")
        return
    data = document.jsonld[0]
    graph = data.get("@graph")
    if data.get("@context") != "https://schema.org" or not isinstance(graph, list):
        errors.append("JSON-LD must use the Schema.org graph context")
        return
    if len(graph) != 3 or any(not isinstance(node, dict) for node in graph):
        errors.append("JSON-LD must describe WebSite, WebPage and SoftwareSourceCode")
        return
    nodes = {node.get("@type"): node for node in graph if isinstance(node.get("@type"), str)}
    if nodes.keys() != {"WebSite", "WebPage", "SoftwareSourceCode"}:
        errors.append("JSON-LD must describe the library, not an unsupported software-app rich result")
        return
    website, page, software = (nodes[name] for name in ("WebSite", "WebPage", "SoftwareSourceCode"))
    for node, fragment in ((website, "website"), (page, "webpage"), (software, "software")):
        if node.get("@id") != CANONICAL + "#" + fragment or node.get("url") != CANONICAL:
            errors.append("JSON-LD identities and URLs must use the canonical project URL")
    if website.get("name") != "datafusion-kql" or website.get("inLanguage") != document.language:
        errors.append("WebSite metadata must match the project and page language")
    if website.get("publisher") != {
        "@type": "Organization", "name": "vertex-shift", "url": "https://github.com/vertex-shift"
    }:
        errors.append("the structured-data publisher must match the visible independent project")
    if page.get("name") != title or page.get("description") != description or page.get("inLanguage") != document.language:
        errors.append("WebPage metadata must match the visible search metadata")
    if page.get("isPartOf") != {"@id": website.get("@id")} or (
        page.get("mainEntity") != {"@id": software.get("@id")}
    ) or software.get("mainEntityOfPage") != {"@id": page.get("@id")}:
        errors.append("JSON-LD graph relationships must refer to the declared project nodes")
    if page.get("primaryImageOfPage") != {
        "@type": "ImageObject", "url": SOCIAL_IMAGE, "width": 1200, "height": 630
    } or software.get("image") != SOCIAL_IMAGE:
        errors.append("JSON-LD image metadata must match the social preview")
    for key, expected in {
        "name": package["name"],
        "description": description,
        "codeRepository": package["repository"],
        "programmingLanguage": "Rust",
        "runtimePlatform": "Apache DataFusion",
        "version": package["version"],
        "license": package["repository"] + "/blob/main/LICENSE",
    }.items():
        if software.get(key) != expected:
            errors.append(f"SoftwareSourceCode metadata must match the source package: {key}")

    pending = [data]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if value.keys() & {"aggregateRating", "review", "offers", "potentialAction", "downloadUrl", "installUrl"}:
                errors.append("structured data cannot invent ratings, offers, search or publication features")
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)


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
    package = tomllib.loads((root / "Cargo.toml").read_text())["package"]
    validate_seo(document, site, package, errors)
    version = package["version"]
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
