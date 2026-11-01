# Project website

The project site is published at
https://vertex-shift.github.io/datafusion-kql/ using GitHub Pages.

## Source and deployment

`site/` is a self-contained static artifact: HTML, CSS, small JavaScript,
licensed local fonts, a favicon, robots.txt and a sitemap. It has no application
framework, analytics, external runtime assets or browser-hosted query engine.
The example explorer presents synthetic results and links to the authoritative
compatibility documentation.

`.github/workflows/pages.yml` validates every push and pull request. Only a
successful run on this repository's `main` branch can deploy. Deployment uses
pinned GitHub Pages actions, the main-only `github-pages` environment, and
job-scoped `pages: write`/`id-token: write` permissions. Only `site/` is uploaded;
repository source, development design files and review screenshots are not
part of the Pages artifact. Changes go through protected-main pull requests.

## Local preview and validation

```bash
python3 .github/scripts/check_site.py
python3 -m unittest discover -s .github/scripts -p 'test_site.py'
node --check site/assets/site.js
node --check site/assets/theme.js
node --test .github/scripts/test_site_theme.cjs
python3 .github/scripts/check_site.py --serve --port 4173
```

The preview is at http://127.0.0.1:4173/datafusion-kql/. It deliberately uses the
project prefix rather than testing only a domain root. Stop the server with
Ctrl-C. Validation checks local assets and licenses, anchors, canonical URL,
heading structure, the Cargo version, example count, sitemap, and prohibited
out-of-scope/symlink artifacts. Update the displayed version and example count
when the corresponding source changes.

The Barlow and Barlow Condensed font files originate from Google Fonts.
Their licenses are retained in `site/assets/fonts/Barlow-OFL.txt` and
`BarlowCondensed-OFL.txt`. The favicon is an original minimal pipe/arrow mark,
not an Apache or Microsoft logo.

## Color themes

The header's native **Color theme** selector offers System, Light and Dark.
System is the default: CSS `color-scheme` and paired `light-dark()` tokens follow
the operating system, including changes while the page is open. Selecting Light
or Dark overrides the system and is remembered in localStorage under
`datafusion-kql-theme`. Selecting System removes that override. Other open tabs
follow changes to the same preference.

The small, self-hosted `theme.js` bootstrap runs before the stylesheet so a saved
override is applied before the first styled paint. The example/copy script
remains deferred. Browser chrome and native controls follow the selected scheme;
petrol code regions and dark text on lime actions retain their contrast in both
appearances. Printing uses the light palette.

Without JavaScript, the page still follows the system scheme and the manual
selector stays hidden. If browser storage is denied or full, overrides still
apply to the current page and a visible status explains that they cannot be
saved. Unexpected storage errors are not silently swallowed.

## Review record

The first implementation was reviewed directly in Chromium with desktop
(1440px), mobile (390px), tablet (768px), the app's reported viewport (1280px),
and narrow (320px) emulated viewport widths. This was a direct review, not a
separate-agent assessment. Actual full-page captures were inspected; mechanical
checks used axe-core with WCAG 2 A/AA and 2.1 AA rules.

The review tightened the first viewport so the native query specimen is visible
at the reported 1280x720 app size, moved the copy action next to the query,
improved mobile action targets, and preserved spaces when mobile headings
reflow. The final checks found no page-level horizontal overflow, asset failures,
JavaScript errors, or axe violations in the tested layouts. Example tabs,
keyboard arrows/Home, clipboard success and denied-clipboard recovery were
exercised. With JavaScript disabled, the first query and setup remain readable;
copy controls stay hidden, selectors are disabled and source links remain usable.

The design detector's contrast warnings for petrol-muted on white were false
positives: computed foreground was rgb(190,213,209) on the petrol query region
rgb(23,73,77). Its padding warnings also missed CSS logical padding; the final
specification and key-run rails have 16px vertical padding. These findings were
checked against actual browser styles, not silently ignored.

**Review disposition: ship** for this project-site scope. Automated checks are
not a complete accessibility certification, and emulated viewports are not
physical-device testing. Development captures are gitignored under
`.impeccable/review/`; PRODUCT.md, DESIGN.md and the surface brief preserve the
site's context without being delivered to visitors.

### System-theme review

Both light and dark appearances were reviewed at all five widths above.
The paired captures preserved the existing typography and hierarchy, including
the compact 320px header. The final Chromium checks found no axe violations,
horizontal overflow, failed assets or JavaScript errors in either appearance.
The selector has a 44px target and a visible 3px focus ring reached through Tab.

Browser checks covered the default system scheme, changes to that scheme while
the page is open, explicit overrides across reloads, first styled paint with a
saved override, theme-color metadata, cross-tab storage changes, print appearance,
and both no-JavaScript schemes. Simulated denied/full storage left the current
theme usable with a visible warning. Existing example tabs and clipboard success
were also exercised in both appearances.

Selection changes used Playwright's native select action. macOS headless Chromium
did not commit synthesized native-picker arrow/Enter sequences, including on an
unchanged native select with all site handlers removed. Physical-device native
menu keyboard commits are therefore not claimed as verified; no custom keyboard
handlers were added to work around the browser harness.

The design hook's 46 off-ramp font-size findings refer to unchanged incumbent
component sizes outside the abbreviated typography frontmatter. They were
classified as contextually intentional existing sizes, not new theme defects.
This change retains that typography rather than normalizing the site or
silencing the detector. Actual rendered contrast was checked in both schemes.
