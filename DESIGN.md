---
name: StreamPulse Engine
description: Operator dashboard for a streaming IoT anomaly-detection pipeline, drawn as an engineering drawing set.
colors:
  film: "#eef1ef"
  sheet: "#f8faf8"
  sheet-2: "#e4e9e6"
  graphite: "#1e2326"
  graphite-2: "#485158"
  graphite-3: "#6b747a"
  rule: "#c3cbc7"
  rule-strong: "#8f9a95"
  rail-ink: "#dfe5e2"
  diazo: "#3a3d9c"
  diazo-deep: "#2e3185"
  diazo-wash: "#dfe0f1"
  diazo-on-graphite: "#a9adf0"
  redline: "#c0291f"
  redline-wash: "#f6dfdc"
  amber: "#a85f00"
  amber-wash: "#f4e6cf"
  ok: "#2b7149"
  ok-wash: "#d9ebdf"
  sev-low: "#5d676e"
  sev-medium: "#a85f00"
  sev-high: "#b5400c"
  sev-critical: "#c0291f"
  series-1: "#3a3d9c"
  series-2: "#8a5a2b"
  series-3: "#1d7470"
  series-4: "#7a3b69"
  series-5: "#5f6a1f"
typography:
  display:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "26px"
    fontWeight: 600
    lineHeight: 1.15
    letterSpacing: "-0.01em"
  figure:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "20px"
    fontWeight: 600
    lineHeight: 1.2
    fontFeature: "tnum"
  title:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "15px"
    fontWeight: 600
    lineHeight: 1.3
  body:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "15px"
    fontWeight: 400
    lineHeight: 1.45
    fontFeature: "tnum"
  body-sm:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.4
  label:
    fontFamily: "Barlow Semi Condensed, Barlow, ui-sans-serif, sans-serif"
    fontSize: "11px"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "0.08em"
  mark:
    fontFamily: "Barlow Semi Condensed, Barlow, ui-sans-serif, sans-serif"
    fontSize: "12px"
    fontWeight: 600
    lineHeight: 1
    letterSpacing: "0.06em"
  balloon:
    fontFamily: "Barlow, ui-sans-serif, system-ui, sans-serif"
    fontSize: "11px"
    fontWeight: 700
    lineHeight: 1
rounded:
  hairline: "2px"
  circle: "50%"
spacing:
  row: "8px"
  cell: "10px"
  pad: "16px"
  gap: "20px"
  rail: "216px"
components:
  button:
    backgroundColor: "#ffffff"
    textColor: "{colors.graphite}"
    typography: "{typography.title}"
    rounded: "{rounded.hairline}"
    padding: "10px 16px"
    height: "38px"
  button-hover:
    backgroundColor: "{colors.sheet-2}"
  button-primary:
    backgroundColor: "{colors.diazo}"
    textColor: "#ffffff"
    rounded: "{rounded.hairline}"
    padding: "10px 16px"
    height: "38px"
  button-primary-hover:
    backgroundColor: "{colors.diazo-deep}"
  button-danger:
    backgroundColor: "#ffffff"
    textColor: "{colors.redline}"
    rounded: "{rounded.hairline}"
    padding: "10px 16px"
  button-danger-hover:
    backgroundColor: "{colors.redline-wash}"
  button-quiet:
    backgroundColor: "#ffffff"
    textColor: "{colors.graphite}"
    rounded: "{rounded.hairline}"
    padding: "7px 12px"
    height: "32px"
  chip:
    backgroundColor: "#ffffff"
    textColor: "{colors.graphite-2}"
    typography: "{typography.mark}"
    rounded: "{rounded.hairline}"
    padding: "8px 10px"
  chip-selected:
    backgroundColor: "{colors.graphite}"
    textColor: "#ffffff"
  input:
    backgroundColor: "#ffffff"
    textColor: "{colors.graphite}"
    rounded: "{rounded.hairline}"
    padding: "7px 9px"
    height: "36px"
  segmented-pressed:
    backgroundColor: "{colors.graphite}"
    textColor: "#ffffff"
    padding: "7px 12px"
  nav-item:
    backgroundColor: "{colors.graphite}"
    textColor: "{colors.rail-ink}"
    rounded: "{rounded.hairline}"
    padding: "8px 10px"
  nav-item-active:
    backgroundColor: "#2d3338"
    textColor: "#ffffff"
  sheet:
    backgroundColor: "{colors.sheet}"
    rounded: "{rounded.hairline}"
    padding: "{spacing.pad}"
  table-header:
    backgroundColor: "{colors.sheet-2}"
    textColor: "{colors.graphite-2}"
    typography: "{typography.label}"
    padding: "8px 10px"
  status-tag:
    textColor: "{colors.graphite}"
    typography: "{typography.label}"
    rounded: "{rounded.hairline}"
    padding: "3px 6px"
  balloon:
    backgroundColor: "{colors.sheet}"
    typography: "{typography.balloon}"
    rounded: "{rounded.circle}"
    size: "22px"
---

# Design System: StreamPulse Engine

## Overview

**Creative North Star: "The Drawing Set"**

The dashboard is a well-kept engineering drawing set laid on drafting film. Every view is a sheet; every number is dimensioned and referenced; every alert is an item callout with a numbered balloon. Structure comes from 1px hairline rules and nothing else: no shadows, no tinted cards, no glow. Ink is graphite, data and selection are diazo violet-blue, and redline red is reserved for what is critical or broken.

Density is operational. Tables run at 8px row padding, charts are stacked plates that share one scale and one header band, and the first thing on the Overview is a ruled title block of pipeline facts rather than a row of hero metrics. One grotesk family carries everything: Barlow for text and figures, its semi-condensed cut in tracked capitals for every label, and tabular figures for every number so columns of values align like a parts list.

The world explicitly rejects the default grey card grid with glowing neon charts and a hero-metric row. Severity is never colour alone: it is a fill-density mark (outline, hatched, cross-hatched, solid) plus a written label.

**Key Characteristics:**
- Cool drafting-film ground with whiter sheets and a graphite rail.
- 1px hairline rules as the only structure; flat at every level.
- Diazo violet-blue for data, selection, focus and the one primary action.
- Redline only for critical severity, errors and rejected data; amber for degraded, green for healthy.
- Barlow throughout; semi-condensed tracked caps for labels; tabular figures everywhere.
- Numbered drafting balloons with leader lines cross-reference alerts between charts and lists.

## Colors

A near-neutral, slightly green-cool paper palette inked in graphite, with one cool data colour and three status inks that each come with a pale wash.

### Primary
- **Diazo Violet-Blue** (diazo): the colour of data and intent. Primary series lines, bar fills, the primary button, focus outlines, selected-row wash (diazo-wash), the acknowledged status tag, text selection. Darkens to Diazo Deep on primary-button hover. On the graphite rail it lifts to **Diazo on Graphite** (diazo-on-graphite) for the brand mark and the active-nav indicator.

### Secondary
- **Redline Red** (redline): critical severity, errors, invalid inputs, rejected-record series and the error banner (on redline-wash). Never decorative.
- **Drafting Amber** (amber): degraded state and medium severity.
- **Survey Green** (ok): healthy and running state, confirmed outcomes.

### Tertiary
- **Severity inks** (sev-low through sev-critical): a graphite-to-redline ramp (slate, amber, burnt orange, redline) used for severity marks, balloon strokes, the per-alert list colour and severity distribution bars.
- **Series inks** (series-1 to series-5): diazo, umber, teal, plum, olive. Muted drafting-pen colours for metric plates and per-query lines; one hue per metric, kept constant across plates. Secondary lines in the same plate reuse the hue with a dash pattern (3 3 or 4 3) instead of a second colour.

### Neutral
- **Drafting Film** (film): the page ground.
- **Sheet** (sheet): every bordered panel, title block and balloon fill.
- **Sheet Well** (sheet-2): table header rows, bar tracks, evidence notes, inline code, quiet-button hover.
- **Graphite** (graphite): body ink, the rail, selected chips and pressed segments, tooltips.
- **Graphite 2** (graphite-2): secondary text, labels, ledes (7.2:1 on film).
- **Graphite 3** (graphite-3): axis ticks and resolved tags (4.6:1 on sheet); the lightest text allowed.
- **Rule** (rule) and **Rule Strong** (rule-strong): hairlines between rows and cells; the strong rule under table headers, around the title block and around inputs.
- **Rail Ink** (rail-ink): text on the graphite rail.

### Named Rules
**The Redline Rule.** Redline marks only what is critical, failed or rejected. If nothing is wrong, no red is on screen.

**The Wash Pairing Rule.** Every status ink has exactly one wash (diazo-wash, redline-wash, amber-wash, ok-wash). A tinted background always pairs with its own ink, never with another hue.

**The Density, Not Hue, Rule.** Severity is read from the mark's fill (empty, hatched, cross-hatched, solid) and its label first; colour only reinforces it.

## Typography

**Display Font:** Barlow (self-hosted, with ui-sans-serif, system-ui fallback)
**Body Font:** Barlow
**Label Font:** Barlow Semi Condensed (600/700), tracked uppercase
**Mono:** system monospace stack (ui-monospace, Cascadia Mono, SF Mono, Consolas) for IDs and raw payloads only

**Character:** One industrial grotesk at a few sizes, ranked by weight and case rather than by scale. The condensed caps read like lettering in a drawing's title block.

### Hierarchy
- **Display** (600, 26px, 1.15, -0.01em): the single view heading per view.
- **Figure** (600, 20px, 1.2, tabular): values in the title block; units follow in 500 13px graphite-2.
- **Title** (600, 15px, 1.3): sheet and plate headings, fleet heading. Alert detail heading steps up to 17px.
- **Body** (400, 15px, 1.45, tabular): running text and ledes (ledes capped at 75ch, graphite-2). Tables run at 14px (13px under 640px).
- **Body Small** (400, 13px): foot notes, muted meta, form messages, tooltips at 12px.
- **Label** (Semi Condensed 600, 11px, 0.08em, uppercase): table headers, title-block captions (0.1em), filter legends, field labels, chart plate titles, status tags.
- **Mark** (Semi Condensed 600, 12px, 0.06em, uppercase): severity marks and filter chips.
- **Balloon numeral** (700, 10.5-11px, centred): numbers inside balloons.

### Named Rules
**The Tabular Everywhere Rule.** `font-variant-numeric: tabular-nums` is set on body, inputs and chart text. Every number aligns in its column.

**The Few Sizes Rule.** Hierarchy comes from weight and case. Add a new size only when a role genuinely has none; the set above is the set.

## Layout

A fixed graphite rail (216px) holds the product name, five views and a live status line at its foot; the main column carries the views with 26px/32px padding and a 1480px maximum width. Sheets stack with a 20px gap and use 16px internal padding. The Overview pairs a two-thirds chart sheet with a one-third table sheet (2fr / 1fr); the Alerts and Devices views use the same split with a sticky detail sheet on the right.

The title block is an auto-fit grid of cells (minimum 150px), each separated by a vertical hairline. Telemetry plates stack inside one sheet, divided by a hairline and 12-14px of space, and reserve a shared 30px balloon lane above each plot so all plates keep identical scale and alignment.

Responsive: at 1180px the 2:1 splits collapse to one column and the detail sheet stops being sticky. At 900px the rail becomes a sticky top bar with horizontal nav (active item underlined in diazo-on-graphite), and low-priority fleet columns drop. At 640px the title block becomes two columns with hairlines on both axes, forms go single-column, and secondary alert columns drop.

## Elevation & Depth

Flat. There are no box-shadows anywhere in the system. Depth is tonal and linear: film ground, whiter sheets on top, sheet-2 wells inside, all bounded by 1px rules. The only floating element, the chart readout tooltip, is a solid graphite slab with a 1px black edge, not a shadowed card. State is shown by fill (washes, graphite inversion), never by lift.

### Named Rules
**The Hairline-Only Rule.** Structure is a 1px rule. If an element needs separating, draw a rule; do not add a shadow, a heavier border or a tinted card.

## Shapes

Corners are effectively square: a 2px radius on sheets, buttons, inputs, chips, tags and tooltips, just enough to soften pixel edges. Circles are reserved for meaning: drafting balloons (22px, 1.5px stroke), onset anchor dots, and state dots (8-9px). Severity marks are 12px squares with a 1.5px outline whose fill density encodes level. Chart lines are 1.75px with round joins; legends and dash patterns, not colour shifts, distinguish secondary series.

## Components

### Buttons
- **Shape:** near-square (2px), 38px tall, 10px 16px padding, 600 14px Barlow.
- **Default:** white with a 1px graphite border and graphite text; hover fills with sheet-2.
- **Primary:** diazo fill, white text; hover darkens to diazo-deep. One per action group.
- **Danger:** white with redline border and text; hover fills with redline-wash.
- **Quiet:** rule-strong border, 500 weight, 13px, 32px tall, for pagers and secondary actions.
- **States:** 160ms colour transitions on the system ease; active nudges down 1px; disabled at 45% opacity; busy at 70% with a progress cursor; focus is a 2px diazo outline offset 2px.

### Chips
- **Style:** filter toggles in the Mark type style, white with a rule-strong hairline and graphite-2 text.
- **State:** checked inverts to solid graphite with white text. Focus draws the diazo outline around the label.

### Segmented control
Time-range switch: one rule-strong outline, segments divided by hairlines, 500 13px text; the pressed segment inverts to graphite.

### Cards / Containers (sheets)
- **Corner Style:** 2px.
- **Background:** sheet on film.
- **Shadow Strategy:** none (see Elevation & Depth).
- **Border:** 1px rule; the title block uses rule-strong.
- **Internal Padding:** 16px; sheet heading sits 12px above content, baseline-aligned with its trailing legend or link.

### Inputs / Fields
- **Style:** white field, 1px rule-strong border, 2px corners, 36px minimum height, 400 14px tabular. Selects carry a drawn chevron in graphite-2.
- **Hover:** border darkens to graphite-2.
- **Focus:** 2px diazo outline at 0 offset with a diazo border.
- **Error:** invalid fields take a redline border; form errors print in redline.
- **Labels:** stacked above in the Label style.

### Navigation
Graphite rail, items at 500 weight in rail-ink with 8px 10px padding. Hover lightens the fill one step and whitens the text; the current view gets a slightly lighter slab, white text and a 1px diazo-on-graphite left rule. On mobile the rule moves to a 2px underline.

### Title block
The Overview's header row of pipeline facts, set as a drawing title block: a sheet with a rule-strong outline, cells divided by vertical hairlines, a Label-style caption over a Figure-style value with a small graphite-2 unit or qualifier. Status values take their status ink (ok, amber, redline).

### Tables
Collapsed, full-width, 14px. Header row on sheet-2 in the Label style over a rule-strong line; rows separated by rule hairlines; numbers right-aligned. Selectable rows hover to a faint diazo tint, selected rows take diazo-wash, and the row linked to a hovered balloon takes the same faint diazo tint. Long findings truncate with an ellipsis at 46ch.

### Severity mark
A 12px square, 1.5px outline in the severity ink, filled by density: low is empty, medium is single 45-degree hatching, high is cross-hatching, critical is solid. Always followed by the severity word in the Mark style, in the same ink. Patterns are shared SVG defs drawn in `currentColor`.

### Status tag and state dot
Status tags (open, acknowledged, resolved) are outlined Label-style tags whose border is `currentColor`: graphite, diazo, graphite-3 respectively. Query and pipeline state uses an 8px dot before the word: ok, amber or redline.

### Drafting balloon callout (signature)
Every alert on a telemetry plate is an item callout: a small solid onset dot (2.6px radius) at the alert's point on the series, a 1px leader line up to a numbered circle (9px radius, 1.5px stroke, sheet fill) in the shared lane above the plot. Balloons are coloured by severity, numbered per device in time order, and nudged apart so they never overlap. The same number appears as a 22px outlined balloon in the alert list beside the chart and in the Overview alert table. Hovering or focusing either end links them: the active balloon fills with its ink and inverts its numeral to white, other balloons fade to 30%, and the matching list or table row takes the linked tint. Balloons are keyboard-focusable buttons with an accessible label.

### Charts
Plates on the sheet: Label-style plate title top-left, series key right-aligned in the same row, hairline gridlines (crispEdges), a rule-strong baseline, 11px graphite-3 ticks, 1.75px series lines, min-max ranges as a 16%-opacity band of the series hue. The hover readout is a dashed graphite-3 vertical line plus the graphite tooltip.

## Do's and Don'ts

### Do:
- **Do** separate everything with 1px rule hairlines on sheet over film; keep surfaces flat.
- **Do** set every number in tabular figures and right-align numeric columns.
- **Do** label every caption, column header and legend in Barlow Semi Condensed 600 uppercase at 11px with 0.08em tracking.
- **Do** encode severity as fill density (empty, hatched, cross-hatched, solid) plus the written level, in its severity ink.
- **Do** reference each alert with the same numbered balloon on the chart and in every list that shows it, and link them on hover and focus.
- **Do** keep diazo as the single data and selection colour; use the 2px diazo outline for every focus state.
- **Do** keep stacked plates on identical width, margins and balloon lane so they read as one set.

### Don't:
- **Don't** add box-shadows, glows or lifted cards; depth is tonal and ruled.
- **Don't** round containers beyond 2px; circles are for balloons, onset anchors and state dots only.
- **Don't** use redline for decoration, emphasis or branding; it means critical, error or rejected.
- **Don't** show severity by colour alone.
- **Don't** build a row of oversized hero metrics; pipeline facts belong in the title block at Figure size.
- **Don't** introduce a second type family for display; hierarchy is weight and case within Barlow.
- **Don't** present simulated telemetry as real plant data; keep the synthetic label in the rail.
