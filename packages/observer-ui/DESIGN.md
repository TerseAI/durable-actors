---
name: Actor observer
description: Neutral light and dark developer console for actor residency and connection inspection.
colors:
  background: "#fafafa"
  foreground: "#171717"
  card: "#ffffff"
  primary: "#171717"
  primary-foreground: "#ffffff"
  secondary: "#f5f5f5"
  muted: "#f5f5f5"
  muted-foreground: "#666666"
  accent: "#f0f0f0"
  destructive: "#c42525"
  border: "#e5e5e5"
  input: "#dedede"
  ring: "#737373"
  success: "#187442"
  danger: "#c42525"
  warning: "#946000"
  background-dark: "#0a0a0a"
  foreground-dark: "#ededed"
  card-dark: "#111111"
  primary-dark: "#ededed"
  primary-foreground-dark: "#0a0a0a"
  secondary-dark: "#1a1a1a"
  muted-dark: "#191919"
  muted-foreground-dark: "#a1a1a1"
  accent-dark: "#222222"
  destructive-dark: "#ff7777"
  border-dark: "#2a2a2a"
  input-dark: "#333333"
  ring-dark: "#a1a1a1"
  success-dark: "#62c991"
  danger-dark: "#ff7777"
  warning-dark: "#e8bc66"
typography:
  title:
    fontFamily: "inherit"
    fontSize: "1.75rem"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-0.035em"
  section:
    fontFamily: "inherit"
    fontSize: "0.875rem"
    fontWeight: 600
    lineHeight: 1.5
  body:
    fontFamily: "inherit"
    fontSize: "0.8125rem"
    lineHeight: 1.6
  label:
    fontFamily: "inherit"
    fontSize: "0.75rem"
  metric:
    fontFamily: "inherit"
    fontSize: "1.625rem"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "-0.03em"
  code:
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
    fontSize: "0.75rem"
rounded:
  sm: "4px"
  md: "6px"
  lg: "8px"
spacing:
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "20px"
  xxl: "24px"
  section: "28px"
  region: "32px"
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.primary-foreground}"
    rounded: "{rounded.md}"
    height: "36px"
    padding: "8px 16px"
  button-outline:
    backgroundColor: "{colors.card}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.md}"
    height: "36px"
  input:
    backgroundColor: "{colors.card}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.md}"
    height: "36px"
    padding: "4px 12px"
  badge:
    backgroundColor: "{colors.muted}"
    textColor: "{colors.muted-foreground}"
    rounded: "{rounded.md}"
    padding: "2px 8px"
  summary:
    backgroundColor: "{colors.card}"
    rounded: "{rounded.lg}"
---

# Design System: Actor observer

## Overview

**Creative North Star: "The precise runtime console"**

A clean, Vercel-like developer console: neutral light and dark surfaces, hairline boundaries, compact shadcn controls, and restrained semantic color. Information density comes from aligned data and progressive disclosure.

The embedded observer inherits its host’s font and semantic tokens. Optional theme defaults and standalone navigation remain separate from the embeddable surface. Source authority is src/theme.css, src/styles.css, src/standalone.css, and src/components/ui/; PRODUCT.md defines the read-only product boundary.

**Key Characteristics:**
- Neutral surfaces with green, amber, and red reserved for state.
- System sans typography with monospace for identifiers and JSON.
- Bordered data regions, inline inspection, and short state transitions.

## Colors

The palette is neutral, with semantic status accents. Frontmatter records the optional standalone defaults; `-dark` entries describe the `.dark` overrides, not additional accents. Host tokens remain authoritative when embedded.

Primary is near-black in light mode and near-white in dark mode. Background, card, muted, and accent create quiet surface layers; foreground and muted foreground distinguish data from context. Border and input define hairlines; ring makes keyboard focus visible. Success green identifies live residency, warning amber identifies unknown residency and unavailable refresh, and danger/destructive red identifies failure. Zero live counts retain normal text color.

## Typography

Embedded typography inherits the host. Standalone uses `-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif`; the unused Satoshi utility mapping does not load a font. The title, section, body, label, and metric roles are recorded above. Tables and controls use 14px text, column headings use 12px, and table numbers use tabular numerals. Supporting paragraphs are capped at 70ch. IDs and JSON use the code stack; JSON line-height is 1.7. The title reduces to 1.5rem on narrow screens.

## Layout

The observer fills its host container. A heading and refresh action lead into a single divided summary strip, searchable inventory, and inline instance and connection details. Summary cells use 20px × 24px padding; tables use 20px horizontal cell padding, 40px headers, 60px actor rows, and 56px instance rows. Numeric actor columns are right aligned and 100px wide. Detail regions start after a 28px gap and separator. Search fields are 260px wide at desktop sizes.

At 640px and below, the actor search stacks beneath its section heading and expands to full width. Summary cells use 16px padding; a fourth Unknown cell causes a two-column arrangement. Tables retain horizontal scrolling, with 12px horizontal cell padding and wrapping identifiers. Buttons and inputs have 44px minimum touch height below Tailwind’s medium breakpoint; the state select and disclosure overrides use 44px at the 640px breakpoint.

The standalone shell alone adds a 64px brand header, 48px active Actors strip, and a centered main area capped at 1152px with 40px × 32px top/side padding. Mobile main padding becomes 28px × 20px, with 48px below. Header content has its own 1440px maximum. The footer and read-only label are shell context, not embedded navigation.

## Elevation & Depth

Borders and tonal fills provide depth. Summary and table frames are flat card surfaces; hovered rows receive a muted fill and selected rows a stronger muted fill. The default button alone uses the small control shadow defined in the theme; outline refresh and disclosure controls do not add card shadows. Failure banners mix a small amount of danger into the card and border colors.

## Shapes

The default radius is 8px. Controls and badges derive their 6px radius from the host radius minus 2px; summary/table frames use an explicit 8px radius and JSON blocks 4px. State dots are circular. Empty instance and connection regions use dashed borders. Preserve host-derived control geometry when embedding.

## Components

- **Buttons:** the shadcn primitive supports default, destructive, outline, secondary, ghost, and link variants. Refresh and recovery use outline; inline disclosure uses ghost. Keep the 2px focus ring and offset, disabled opacity, 150ms ease-out transitions, and 1px active press. Disclosure rotates its chevron when expanded and exposes its state to assistive technology.
- **Inputs and select:** actor and instance search use the shared Input with an inset search icon. The native state select offers All states, Live, Dormant, and Unknown. Preserve explicit accessible names, focus feedback, and clear-filter recovery for no matches.
- **Badges:** muted count labels and outlined namespace/state labels are compact, medium-weight chips. State badges pair a dot with a written label. Long namespace values wrap.
- **Summary and tables:** one divided summary region presents existing inventory totals. Semantic tables retain named regions, column scopes, tabular values, hover/selected states, and horizontal overflow. Unknown summary/column content appears only when the inventory contains unknown counts. Actor and instance selection progressively reveal details inline.
- **Connection details:** show active socket IDs and formatted, wrapping JSON metadata in a scrollable code area capped at 240px height. Connections are WebSockets, not unique people.
- **Loading and freshness:** initial loading uses an accessible skeleton. Refresh disables the action and animates its icon; a real request schedules the next poll five seconds after completion. Prior data remains visible on refresh failure with an explicit stale-count warning and retry action. Counts reflect the latest host heartbeat.
- **Empty and error states:** distinguish no deployed actors, actor types without instances, no matching filters, and no active connections. Initial failure explains connection/access recovery; refresh failure never replaces known counts with zero.
- **Standalone chrome:** the Actors marker is an active page label, not an extra navigation workflow. The theme button starts from OS preference and permits a manual override for the current page session; the skip link moves to main content.

Skeleton pulse is 1.5s ease-in-out and refresh spin is 1s linear. Reduced-motion preferences disable observer animations and transitions.

## Do's and Don'ts

- Do inherit host semantic tokens and fonts when embedded.
- Do keep la: utilities and observer-scoped base styles; keep standalone chrome separate.
- Do retain keyboard focus, reduced-motion support, wrapping identifiers, and horizontally scrollable tables.
- Do express residency and freshness with labels as well as color.
- Don’t restore the superseded warm Terse palette.
- Don’t load optional global theme defaults or standalone page styles into an already themed host.
- Don’t invent charts, history, metrics, or mutation controls outside the read-only product scope.
