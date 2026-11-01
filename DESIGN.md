---
name: datafusion-kql
description: A clear engineering reference for native KQL in DataFusion.
colors:
  paper: "#f3f7f6"
  white: "#ffffff"
  ink: "#183b40"
  muted: "#50696c"
  petrol: "#17494d"
  petrol-deep: "#11383c"
  on-petrol: "#edf6f3"
  petrol-muted: "#bed5d1"
  lime: "#d4ef9e"
  lime-hover: "#e0f6b8"
  line: "#ccd9d6"
  dark-line: "#497175"
  soft-field: "#e5eeeb"
  focus: "#246f76"
typography:
  display:
    fontFamily: "Barlow Condensed, Barlow, sans-serif"
    fontSize: "clamp(3.5rem, 8.5vw, 5rem)"
    fontWeight: 600
    lineHeight: 0.98
    letterSpacing: "-0.02em"
  headline:
    fontFamily: "Barlow Condensed, Barlow, sans-serif"
    fontSize: "clamp(2.5rem, 4.4vw, 3.8rem)"
    fontWeight: 600
    lineHeight: 1.05
    letterSpacing: "-0.015em"
  body:
    fontFamily: "Barlow, Segoe UI, sans-serif"
    fontSize: "1.125rem"
    fontWeight: 400
    lineHeight: 1.55
  code:
    fontFamily: "SFMono-Regular, Consolas, Liberation Mono, monospace"
    fontSize: "0.9rem"
    lineHeight: 1.8
rounded:
  control: "0.375rem"
spacing:
  compact: "0.5rem"
  normal: "1rem"
  group: "1.5rem"
  section: "3rem"
  major: "5rem"
components:
  button-primary:
    backgroundColor: "{colors.lime}"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
    padding: "0.8rem 1.4rem"
  button-secondary:
    backgroundColor: "{colors.petrol-deep}"
    textColor: "{colors.on-petrol}"
    rounded: "{rounded.control}"
    padding: "0.8rem 1.4rem"
  example-tab-selected:
    backgroundColor: "{colors.petrol}"
    textColor: "{colors.on-petrol}"
    rounded: "{rounded.control}"
    padding: "0.6rem 1rem"
---

# Design system: datafusion-kql

## Overview

**Creative north star: "The engineering datasheet."**

The website uses readable specifications, code specimens and evidence tables
to explain a native language extension. Condensed display lettering creates
hierarchy; calm body text and precise source links do the explanatory work.
The visual world is a development decision, not an additional product claim.

## Colors

Cool mineral paper and deep petrol dominate. Pale lime identifies primary
actions, not numerical success or compatibility certification. Muted text is
paired with its actual background: petrol-muted is for dark petrol regions,
while muted is for light regions.

## Typography

Barlow Condensed carries headlines; Barlow carries navigation and prose.
Monospace is reserved for source code and API names. Fonts are self-hosted with
their SIL Open Font License files. Body text becomes 1rem at the mobile
breakpoint; code may scroll locally instead of forcing page overflow.

## Layout

The content width is at most 76rem. Major sections use paired headings and
explanation, while the first specimen spans the page. At 1000px the layout
reduces margins and simplifies ownership rows. At 700px paired regions stack,
specifications and the execution path use two columns, and actionable links
have 44px touch targets. The page remains readable without JavaScript.

## Elevation & Depth

Use flat tonal regions and thin rules, not decorative shadows or glass.
Selection, focus and contrast communicate state.

## Shapes

Controls and the query specimen use a 0.375rem radius. Tables retain square,
open rows. SVG icons use a consistent simple stroke; no icon fonts are required.

## Components

The example selector is a keyboard-operable tablist only after JavaScript
enhancement. It updates the query, correlated synthetic result and contract
link together. Copy controls provide explicit success/failure feedback and
select code for manual recovery when clipboard access fails. The query
specimen uses one restrained brightness transition; reduced motion disables
that transition and smooth scrolling without hiding content.

## Do's and Don'ts

- Do keep headings, source examples and compatibility boundaries visible.
- Do preserve the native-plan explanation and distinguish fixture provenance.
- Do use local assets, semantic HTML, focus rings and project-prefix-safe URLs.
- Don't present the example explorer as an in-browser KQL runtime.
- Don't turn benchmarks or coverage into unsupported hero statistics.
- Don't deliver development briefs, screenshots or design metadata as site assets.
