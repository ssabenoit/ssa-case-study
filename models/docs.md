{% docs nhl_season_key %}
NHL season identifier in `YYYYZZZZ` form, where `YYYY` is the season's start
year and `ZZZZ` its end year — e.g. `20252026` for the 2025-26 season. Marts
also expose `season_display` ("2025-26") for presentation.
{% enddocs %}

{% docs game_type %}
Game classification: `regular` or `playoff` (preseason and special-event
games are excluded from all stats models). The canonical universe of NHL
league games is `int__league_games`, which keeps only games between two NHL
franchises validated against that season's standings participants — this is
what keeps All-Star and 4 Nations Face-Off games out of player and team
statistics.
{% enddocs %}

{% docs times_shorthanded %}
Approximation of the official NHL "times shorthanded" / opponent power-play
opportunity count, derived from play-by-play penalty events: minors (2),
double minors (4), and majors (5) each count once; offsetting penalties
(same game clock time, same duration, opposite teams) cancel; misconducts
never create a power play but do count toward PIM. Validated to produce
season PP opportunity totals in the official 213–260 range.
{% enddocs %}

{% docs faceoff_counts %}
Faceoff wins and losses are exact counts of play-by-play faceoff events
(the event owner is the winning team / winning player), not estimates and
not averages of per-game percentages.
{% enddocs %}

{% docs derived_team_stats %}
The loaded `game_summaries` table does not include the NHL's
`summary.teamGameStats` payload, so team counting stats are assembled from
first-party sources instead: hits/blocks/giveaways/takeaways/PP goals are
summed from player boxscores; PIM and times-shorthanded come from
play-by-play penalty events; faceoff percentage comes from play-by-play
faceoff events. Scores and shots-on-goal come from the game summary itself.
{% enddocs %}
