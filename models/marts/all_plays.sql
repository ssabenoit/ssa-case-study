-- models/marts/all_plays.sql
-- production layer of all plays

select *
from {{ ref('stg_nhl__play_by_play') }}