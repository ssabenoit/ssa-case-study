# NHL Power Rankings - Playoff Performance Integration

## Overview
The team power rankings model now incorporates previous season playoff performance to better predict early-season team strength. This enhancement recognizes that playoff success is often a stronger indicator of team quality than regular season performance alone.

## Playoff Performance Metrics

### 1. Playoff Round Reached
Based on games played in playoffs:
- **Cup Finals** (4): 20+ games played
- **Conference Finals** (3): 14-19 games played  
- **Second Round** (2): 8-13 games played
- **First Round** (1): 4-7 games played
- **Missed Playoffs** (0): 0-3 games played

### 2. Playoff Win Percentage
Calculated from actual game results where available:
```sql
playoff_wins / playoff_games_played
```

### 3. Playoff Goal Differential
Per-game goal differential in playoffs shows dominance:
```sql
(goals_for - goals_against) / games_played
```

## Power Score Calculation

### Teams That Made Playoffs
The previous season power score is calculated as:
- **60% Regular Season Performance**: Final standings position
- **40% Playoff Performance**, broken down as:
  - 60% Total playoff wins (recognizing volume of success)
  - 20% Playoff win percentage
  - 20% Playoff goal differential

### Teams That Missed Playoffs
- Receive 80% of their regular season score
- 20% penalty for missing playoffs

## Early Season Blending

The model blends previous season (including playoffs) with current season based on games played:

| Games Played | Current Season Weight | Previous Season Weight |
|--------------|----------------------|------------------------|
| 0            | 0%                   | 100%                   |
| 1-5          | 5-25%                | 75-95%                 |
| 6-10         | 30-50%               | 50-70%                 |
| 11-15        | 55-75%               | 25-45%                 |
| 16-19        | 80-95%               | 5-20%                  |
| 20+          | 100%                 | 0%                     |

## Impact on Rankings

Teams that performed well in playoffs receive a boost in early-season power rankings:
- **Stanley Cup Champions**: Significant boost reflecting championship pedigree
- **Conference Finalists**: Moderate boost for deep playoff runs
- **First Round Exits**: Minimal boost, but still credited for making playoffs
- **Non-Playoff Teams**: Penalized to reflect inability to qualify

## Output Columns

The model now includes additional playoff context columns:
- `prev_playoff_round`: Numeric round reached (0-4)
- `prev_playoff_result`: Text description ("Cup Finals", "Conference Finals", etc.)
- `prev_playoff_win_pct`: Win percentage in previous playoffs
- `prev_playoff_games`: Total playoff games played

This methodology ensures that teams with strong playoff performances are appropriately weighted in early-season rankings, while gradually transitioning to current season performance as more games are played.