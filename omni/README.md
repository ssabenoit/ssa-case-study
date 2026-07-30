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
```

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
