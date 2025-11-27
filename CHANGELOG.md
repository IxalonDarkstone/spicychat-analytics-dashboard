# Changelog
All notable changes to **SpicyChat Analytics** will be documented here.

## [2.1] – 2025-11-28
### Added
- **Multi-tag filtering system** with support for:
  - **AND** tag filters (`?and=tag1,tag2`)
  - **NOT** tag filters (`?not=tag3`)
  - Works simultaneously with sorting, paging, and creator filtering.
- **Creator filter chip** displayed above the grid when filtering by a specific creator.
- **Persistent sidebar tabs** (`Creators` and `Tags`) using `?tab=` query state.
- **Filtered bot count display** (“Showing X bots”) reflecting AND/NOT/creator filters.
- **Top 480 global trending system**:
  - Pagination across 10 pages of Typesense results.
  - Unified trending map handling 480 bots.
  - Avatar normalization to ND/CDN.
- **Historical top-rank tracking**:
  - New `bot_rank_history` table storing daily global rank snapshots.
  - New `top240_history` and `top480_history` tables storing daily counts of your bots.
- **Metadata-driven Last Snapshot timestamp** (appears on dashboard).
- **`--no_snapshot` CLI flag** to skip initial snapshot during development.
- **Reauth workflow improvements**:
  - Successful auth now triggers an immediate snapshot.
- **Chart smoothing** using spline interpolation for more readable analytics.
- **New error-tolerant Typesense fetch pipeline**:
  - `multi_search_request()` abstraction.
  - Automatic retry and timeout handling.
  - Clean caching of both filtered and unfiltered trending lists.

### Changed
- **Filtering pipeline restructured**:
  - Base trending now always starts with **Female + NSFW**.
  - Tag counts and creator counts now derive from the *same dataset* used for grid filtering (no more mismatches).
- **Pagination improved**:
  - Added First (`⏮`) and Last (`⏭`) page buttons.
  - Added Jump-to-Top button.
  - Page indicator redesigned for dark theme readability.
- **Reworked global trending UI**:
  - Three-column tag table (`Tag | # | Filter`).
  - ✓ (AND) and ✕ (NOT) filter controls placed in dedicated Filter column.
  - Cards now expand dynamically to fill available width.
- **Creator filtering stabilized**:
  - Normalized username matching (`strip()` + lowercase).
  - Filter preserved across tag chips, sorting, and tabs.
- **Snapshot scheduler upgraded**:
  - No longer autotriggers auth popup.
  - Pauses on invalid auth; resumes when fixed.
  - Works in dev mode even when `--no_snapshot` is enabled.

### Fixed
- Fixed mismatch between sidebar tag counts and grid results.
- Fixed creator links sometimes producing zero results due to URL newline encoding.
- Fixed avatar URLs breaking when raw Typesense values lacked full paths.
- Fixed charts crashing when single-point datasets were rendered.
- Fixed database initialization inconsistencies between environments.
- Fixed rare Unicode logging crashes via `safe_log`.

---

## [2.0] – 2025-11-15
### Added
- First complete release of SpicyChat Analytics.
- Snapshot capturing of all bots and daily totals.
- Local SQLite database with historical deltas.
- Dashboard with totals, deltas, trending charts, and bot rankings.
- Basic Typesense support for fetching trending bots.
- Basic creator list and tag list (non-interactive).
- Early global trending page prototype (single-page only).
- Authentication passthrough with local token capture.

---

## Format
This changelog follows the **Keep a Changelog** format  
and aims to be compatible with **Semantic Versioning**.

