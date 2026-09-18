---
name: Actor observer
description: Neutral light and dark developer console for actor residency, connections, and request timings.
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
  console-sidebar: "#f5f5f3"
  console-selected: "#e8eae5"
  console-sidebar-dark: "#131512"
  console-selected-dark: "#272d25"
  residency-dormant: "#b5c59f"
typography:
  overview-title:
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
    fontSize: "29px"
    fontWeight: 550
    lineHeight: 1.2
    letterSpacing: "-0.03em"
  overview-metric:
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'
    fontSize: "33px"
    fontWeight: 450
    lineHeight: 1.2
    letterSpacing: "-0.035em"
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
  console-control: "5px"
  console-panel: "7px"
spacing:
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "20px"
  xxl: "24px"
  section: "28px"
  region: "32px"
components:
  console-navigation-active:
    backgroundColor: "{colors.console-selected}"
    textColor: "{colors.foreground}"
    rounded: "{rounded.console-control}"
    padding: "8px 12px"
  overview-card:
    backgroundColor: "{colors.card}"
    rounded: "{rounded.console-panel}"
    padding: "17px 19px"
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

A precise runtime console: neutral light and dark surfaces, hairline boundaries, compact controls, and restrained semantic color. The standalone Operate direction follows the user-approved [little-actors Overview](https://doop.design/c/x71to_CeX0), frame `xIP9Ls3hDj`: a quiet sidebar, unequal metric cards, and a grouped actor table. Information density comes from aligned data and progressive disclosure.

The embedded observer inherits its host’s font and semantic tokens. Optional theme defaults and standalone navigation remain separate from the embeddable surface. Source authority is src/theme.css, src/styles.css, src/standalone.css, and src/components/ui/; PRODUCT.md defines the read-only product boundary. The approved screenshot and desktop/mobile implementation captures live in .impeccable/review/. The standalone composition does not replace the embedded observer’s existing geometry.

**Key Characteristics:**
- Neutral surfaces with green, amber, and red reserved for state.
- System sans typography with monospace for identifiers, overview data, and JSON.
- Bordered data regions, inline inspection, and short state transitions.

## Colors

The palette is neutral, with semantic status accents. Frontmatter records the optional standalone defaults; `-dark` entries describe the `.dark` overrides, not additional accents. Host tokens remain authoritative when embedded.

Primary is near-black in light mode and near-white in dark mode. Background, card, muted, and accent create quiet surface layers; foreground and muted foreground distinguish data from context. Border and input define hairlines; ring makes keyboard focus visible. Success green identifies live residency, warning amber identifies unknown residency and unavailable refresh, and danger/destructive red identifies failure. Zero live counts retain normal text color. Standalone navigation adds subtly tinted neutral sidebar and selected fills; the residency bar uses a soft green for dormant instances alongside the live and unknown colors.

**The Evidence Rule.** Status color always accompanies written state or a value with accessible threshold guidance; unavailable values remain visibly distinct from zero.

## Typography

Embedded typography inherits the host. Standalone uses `-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif`; the unused Satoshi utility mapping does not load a font. System stacks preserve the standalone Content Security Policy without external font loading. The title, section, body, label, and metric roles are recorded above. Tables and controls use 14px text, column headings use 12px, and table numbers use tabular numerals. Supporting paragraphs are capped at 70ch. IDs and JSON use the code stack; JSON line-height is 1.7. The embedded title reduces to 1.5rem on narrow screens. Overview uses its separate title and metric roles, a 15px section heading, 12px actor-class links, 11px monospace table values, and 9px uppercase monospace group headings. Its title reduces to 25px at the standalone mobile breakpoint.

## Layout

The embedded observer fills its host container. A heading and refresh action lead into a single divided summary strip, searchable inventory, and inline instance and connection details. Summary cells use 20px × 24px padding; tables use 20px horizontal cell padding, 40px headers, 60px actor rows, and 56px instance rows. Numeric actor columns are right aligned and 100px wide. Detail regions start after a 28px gap and separator. Search fields are 260px wide at desktop sizes.

At 640px and below, the actor search stacks beneath its section heading and expands to full width. Summary cells use 16px padding; a fourth Unknown cell causes a two-column arrangement. Tables retain horizontal scrolling, with 12px horizontal cell padding and wrapping identifiers. Buttons and inputs have 44px minimum touch height below Tailwind’s medium breakpoint; the state select and disclosure overrides use 44px at the 640px breakpoint.

The standalone shell uses a 204px sidebar and a flexible workspace, with a 64px topbar and main padding of 29px 32px 48px. Navigation lists little-actors, Actors, Requests, and WebSockets; theme and documentation controls sit in the topbar. A read-only runtime label anchors the sidebar. Overview leads with three metric cards in a 0.92:1.34:1 grid, separated by 14px, followed by a grouped actor-class table. Cards have a 154px minimum height. The table retains an 850px minimum width and scrolls within a focusable named region.

At 1150px and below, the first two cards share a row and the third spans both columns. At 760px and below, navigation becomes a horizontally scrollable row, the sidebar footer hides, the topbar becomes 52px high, and cards stack. Main padding becomes 26px 20px 40px. Filters wrap, the search expands to full width, and standalone interactive controls have at least 44px height. Preserve the table’s data columns through horizontal scrolling.

**The Separate Surfaces Rule.** Keep the standalone shell and Overview layout in standalone styles; embedded Actors and Requests retain their host-aware tokens and existing density.

Requests use the same toolbar and bordered table frame, with a 720px minimum table width and horizontal scrolling. Time is subdued; Total and Queue wait are right aligned with tabular numerals. Operation labels wrap above actor context, and expanded IDs stay inline. Below 640px, toolbar actions wrap while retaining their touch targets.

## Elevation & Depth

Borders and tonal fills provide depth. Summary and table frames are flat card surfaces; hovered rows receive a muted fill and selected rows a stronger muted fill. The default button alone uses the small control shadow defined in the theme; outline refresh and disclosure controls do not add card shadows. Failure banners mix a small amount of danger into the card and border colors.

## Shapes

The embedded default radius is 8px. Controls and badges derive their 6px radius from the host radius minus 2px; summary/table frames use an explicit 8px radius and JSON blocks 4px. State dots are circular. Empty instance and connection regions use dashed borders. Preserve host-derived control geometry when embedding. Standalone navigation, theme controls, and selects use the console-control radius; Overview cards, tables, and the threshold panel use the console-panel radius. Overview status highlights retain the smaller 4px radius.

## Components

- **Buttons:** the shadcn primitive supports default, destructive, outline, secondary, ghost, and link variants. Refresh and recovery use outline; inline disclosure uses ghost. Keep the 2px focus ring and offset, disabled opacity, 150ms ease-out transitions, and 1px active press. Disclosure rotates its chevron when expanded and exposes its state to assistive technology.
- **Inputs and select:** actor and instance search use the shared Input with an inset search icon. The native state select offers All states, Live, Dormant, and Unknown. Preserve explicit accessible names, focus feedback, and clear-filter recovery for no matches.
- **Badges:** muted count labels and outlined state labels are compact, medium-weight chips. State badges pair a dot with a written label.
- **Summary and tables:** one divided summary region presents existing inventory totals. Semantic tables retain named regions, column scopes, tabular values, hover/selected states, and horizontal overflow. Unknown summary/column content appears only when the inventory contains unknown counts. Actor and instance selection progressively reveal details inline.
- **Connection details:** show active socket IDs and formatted, wrapping JSON metadata in a scrollable code area capped at 240px height. Connections are WebSockets, not unique people.
- **Loading and freshness:** initial loading uses an accessible skeleton. The HTTP client subscribes to live inventory events and reconnects automatically on failure. Refresh restarts the subscription; prior data remains visible on failure with an explicit stale-count warning and retry action. The footer distinguishes live updates from reconnecting. Custom clients without subscriptions retain five-second polling. Counts reflect the latest persisted host report.
- **Empty and error states:** distinguish no deployed actors, actor types without instances, no matching filters, and no active connections. Initial failure explains connection/access recovery; refresh failure never replaces known counts with zero.
- **Request traces:** a newest-first table shows Time, Request, Transport, Outcome, Total, and Queue wait. Method and WebSocket transports use text; outlined outcome badges use success for Completed, danger for Failed/Rejected, and warning for Interrupted/Rerouted. The operation disclosure reveals wrapping monospace Request ID, Host, and optional Connection fields inline. A missing queue duration uses an em dash with an explanation that processing did not begin.
- **Request collection states:** outline Pause/Resume controls freeze the display while collection continues. Reconnection preserves received rows and exposes Retry; delivery loss has a separate alert. The footer names the current live, connecting, reconnecting, or paused state, the retention capacity, and reset-on-restart behavior. Expired records and host-side timing scope are explained beneath the table. Connecting, unavailable, and empty history states remain distinct.
- **Standalone chrome:** navigation buttons pair icons with labels. The current view uses a filled selected surface and aria-current="page"; inactive labels use muted foreground and brighten on hover. The theme button starts from OS preference and permits a manual override for the current page session; the skip link moves to main content. Overview actor-class actions open the existing Actors inspector with that class selected and move focus to the main region.
- **Overview:** actor-instance residency, retained request measurements, and current WebSocket connections occupy separate cards. Group table columns into Actors, Requests, and WebSockets, with subtle group separators and right-aligned numeric data. Class search and residency filters narrow table rows; the selected time window applies to request measurements. Label retained sample scope beside the values and below the table. Threshold guidance is an accessible disclosure; unavailable measurements display an em dash. Preserve distinct initial, empty, filtered, stale, and stream-failure states.
- **WebSocket inventory:** a searchable table presents current actor class, instance, connection ID, and wrapping JSON metadata from inventory. Preserve the same connection terminology and unavailable/empty/stale states as embedded inspection.

Skeleton pulse is 1.5s ease-in-out and refresh spin is 1s linear. Reduced-motion preferences disable observer animations and transitions.

## Do's and Don'ts

- Do inherit host semantic tokens and fonts when embedded.
- Do keep la: utilities and observer-scoped base styles; keep standalone chrome separate.
- Do retain keyboard focus, reduced-motion support, wrapping identifiers, and horizontally scrollable tables.
- Do express residency and freshness with labels as well as color.
- Don’t restore the superseded warm Terse palette.
- Don’t load optional global theme defaults or standalone page styles into an already themed host.
- Don’t fabricate deployment data, charts, full-window totals, identity, queue depth, or outgoing-write metrics; use the existing inventory and explicitly scoped retained traces.
