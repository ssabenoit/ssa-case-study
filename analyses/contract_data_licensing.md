# Contract / Cap Data — Licensing Notes & Outreach (Phase B3 item 8)

## Findings (2026-07-30)

- **CapWages**: has a developer API, but the ToS prohibit reselling,
  reposting, bulk download, or redistribution; data remains their IP and
  access is revocable at any time. Displaying player-level cap data on a
  **public** dashboard likely qualifies as reposting → not usable for the
  showcase without written permission. (capwages.com/terms-of-service)
- **PuckPedia**: operates a formal Data API / partnership program
  (puckpedia.com/tools/data) — the right route for public display with
  attribution. Needs a partnership conversation → draft below.
- **Fallback (ships regardless)**: hand-curated seed of team-season payroll
  totals (cited) powering team-level $/point and $/xG. Zero licensing risk.

## Draft email to PuckPedia (from Nick)

> **Subject:** Data partnership inquiry — NHL analytics case study (Southshore Analytics)
>
> Hi PuckPedia team,
>
> I run Southshore Analytics, a data consultancy. We've built an open,
> end-to-end NHL analytics platform (19 seasons of play-by-play, an
> expected-goals model, season projections) as a public case study of our
> engineering practice, and we'd love to add a "value analytics" layer —
> cost per point, cost per expected goal, contract efficiency — built on
> PuckPedia contract data.
>
> The case study is public and non-commercial (it markets our consulting
> services, not a data product). We'd attribute PuckPedia prominently on
> every view that uses your data, link back to your player/team pages, and
> we're happy to share the write-up with you before publishing. Would a
> Data API partnership for this use be possible, and if so, on what terms?
>
> Happy to walk through the project on a call — the working dashboard and
> methodology are available for review.
>
> Thanks,
> Nick

## Decision rule

Player-level contract analytics ship only under a written PuckPedia (or
CapWages) permission. Team-level payroll efficiency ships from the curated
seed either way.
