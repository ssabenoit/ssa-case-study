# Omni semantic layer

Version-controlled semantic model for the **NHL Database** connection on
southshorellc.omniapp.co, mirroring the dbt ethos: the model is code, lives in
git, and is pushed (not clicked) into the BI tool.

## Layout

```
omni/
  model/                      the semantic model, one file per Omni YAML file
    relationships             global join graph (star-schema keys)
    *.topic                   curated topics with ai_context for Blobby
    DBT_ANALYTICS.PROD/*.view view extensions: labels, primary keys, measures
                              (ratio-of-sums percentages), hidden staging/int views
    DBT_ANALYTICS.ANALYTICS/  raw engine tables (hidden; marts serve instead)
  push_model.py               sync omni/model/ -> Omni via the model YAML API
  parity_check.py             Omni-vs-Snowflake parity suite (11 canonical numbers)
  build_dashboards.py         dashboards-as-code: the five flagship dashboards
```

## Dashboards (folder "NHL Showcase")

| Dashboard | identifier | Control |
|---|---|---|
| NHL League Pulse | `cad0598c` | Season picker |
| NHL Team Page | `b2c5a0d5` | Team picker (default NSH) |
| NHL Player Explorer | `e10f16fa` | Season picker |
| NHL Game Center | `71430d9d` | Game picker (default 2025030415, 2026 Cup Final) |
| NHL Projections Hub | `c6647f9b` | — (latest-run filters baked in) |

URLs: `https://southshorellc.omniapp.co/dashboards/<identifier>`. All 31 tile
queries verified via the query API; all five render to PDF via the downloads API.

### Dashboard controls via the v2 API (undocumented grammar)

`PATCH /api/v2/documents/{id}/draft` then `POST .../draft/publish` — a patch
and its publish must happen in ONE draft cycle (each PATCH starts a fresh
draft from the published doc; separate patches don't stack). Control shape:

```json
{"controls": {"data": {"<bound_field>": {
    "label": "Team",
    "config": {"type": "string", "kind": "EQUALS", "values": ["NSH"]},
    "map": {"1": "<field_in_tile_1>", "2": false}
}}, "order": ["<bound_field>"]}}
```

`map` keys are tile record-keys ("1", "2", …); the value is the field the
control drives in that tile (`false` = tile unaffected). Tile-level filters on
the same field must be removed in the same patch or they intersect with the
control and strand the tile on the old value.

### Chart visConfig grammar (undocumented; see restyle_dashboards.py)

The v1 documents API silently drops `visConfig` at creation — charts must be
applied afterward via the v2 draft API. Three things are required or the tile
silently falls back to a table:

1. `prefersChart: true` on the tile (THE switch — a valid chart config with
   `prefersChart: false` still renders as a table).
2. `visConfig.visConfig.visType: "basic"` (not `vegalite`).
3. A full cartesian config: `_dependentAxis`, `behaviors`, `configType`,
   `mark`, `x` (with `field`), `series` (each with `field`, `manual: true`,
   `mark._mark_color`, `yAxis`), `color` (valid `legendPosition`, e.g.
   `"bottom"`), and `tooltip`.

Valid `chartType` values (extracted from validation errors): auto, area,
areaStacked, bar, barLine, barGrouped, barStacked, boxplot, column,
columnGrouped, columnStacked, heatmap, kpi, line, lineColor, map, markdown,
pie, funnel, sankey, point, pointColor, pointSize, pointSizeColor,
singleRecord, summaryValue, svgMap, table, treemap.

For multi-series lines colored by a dimension (`lineColor`), put the color
dimension in `config.color.field` and do NOT pivot the query. Display formats
(percentages etc.) belong on the model (view-extension `format:`), not the
dashboard — they then apply everywhere including Blobby answers.

The iteration loop that makes this workable: patch → publish →
`POST /v1/dashboards/{id}/download {"format": "png"}` → poll
`.../download/{jobId}/status` → fetch `.../download/{jobId}` → look at the
image → fix. Renders are ground truth; stored configs can look right and
still fall back.

## Workflow

1. Edit files under `omni/model/`.
2. `python omni/push_model.py` — pushes every file (mode `extension`) and runs
   the model validator; exits nonzero on any push failure or validation error.
3. `python omni/parity_check.py` — the acceptance gate. Queries the same
   figures through Omni's query API and directly against Snowflake
   (plus record-book anchors: Matthews 69, Ovechkin 65, 720 lockout games,
   Cup odds summing to 1). All 11 must pass.

## Model facts

- Shared model **NHL Database** (`OMNI_MODEL_ID` in `.env`), built on the
  schema model for connection `OMNI_CONNECTION_ID` (Snowflake `DBT_ANALYTICS`,
  schemas `PROD` + `ANALYTICS`, service user `SVC_NHL_PIPELINE`, key-pair auth).
- Nightly `pipeline/refresh.py` triggers `POST /api/v1/models/{id}/refresh`
  and polls `GET /api/v1/jobs/{jobId}/status` until `COMPLETED`.
- Measure conventions: percentages are ratios of sums (never averages of
  per-game percentages); goalie GP counts only appearances with ice time.
- All staging/`int__`/legacy compatibility views are hidden so the field picker
  and Blobby only see the star schema, marts, and projections.

## Gotchas learned the hard way

- A connection created via the API has **no schema model** until you
  `POST /api/v1/models` with `modelKind: SCHEMA`, refresh it, then create the
  `SHARED` model. The UI does this implicitly; the API does not.
- `GET /models/{id}/yaml` hides schema-generated views unless you pass
  `?includeSchemas=DBT_ANALYTICS.PROD` (one schema at a time).
- Query-API filters need Omni's internal shapes, e.g.
  `{"type": "string", "kind": "EQUALS", "values": [...]}` and
  `{"type": "boolean", "is_negative": false}` — anything else is either
  rejected or silently ignored.
- `fct_games.game_type` is capitalized (`Regular`/`Playoff`); the per-player
  and per-team facts use lowercase. Topic `ai_context` spells this out so
  Blobby filters correctly.
