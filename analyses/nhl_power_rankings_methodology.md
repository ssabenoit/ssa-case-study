# NHL Power Rankings Methodology

This document outlines the methodology used for calculating NHL team power rankings in the NHL Analytics project.

## Overview

The NHL Power Rankings provide a data-driven assessment of team strength that goes beyond simple win-loss records and standings points. The rankings incorporate multiple performance factors and handle early season rankings by blending previous season performance with current season data.

### Key Features:
1. Basic performance metrics (wins, losses, points)
2. Goal-based metrics (goal differential, scoring rates)
3. Advanced statistics (shot metrics, special teams)
4. Strength of schedule adjustments
5. Recency weighting (recent performance matters more)
6. **Previous season carryover for early season stability**

## Early Season Handling

The model addresses the challenge of early season rankings through a progressive blending approach:

### Blending Formula

```
Blend Weight = min(games_played / 20, 1.0)
```

- **0 games played**: 100% previous season, 0% current season
- **1-19 games**: Linear blend (e.g., 10 games = 50/50 blend)
- **20+ games**: 100% current season, 0% previous season

This ensures:
- Rankings are available before season starts
- Smooth transition from preseason expectations to actual performance
- No wild swings from small sample sizes
- Convergence to pure current-season metrics by game 20

## Ranking Formula

The power ranking score adapts based on games played:

### 0 Games Played (Preseason)
```
Power Score = (0.35 * PreviousPointsComponent) +
              (0.35 * PreviousGoalComponent) +
              (0.30 * NeutralValue)
```

### 1-4 Games (Very Early Season)
```
Power Score = (0.35 * BlendedPointsComponent) +
              (0.30 * BlendedGoalComponent) +
              (0.15 * AdvancedStatsComponent) +
              (0.10 * ScheduleStrengthComponent) +
              (0.10 * MomentumComponent)
```

### 5-9 Games (Early Season)
```
Power Score = (0.32 * BlendedPointsComponent) +
              (0.28 * BlendedGoalComponent) +
              (0.18 * AdvancedStatsComponent) +
              (0.12 * ScheduleStrengthComponent) +
              (0.10 * MomentumComponent)
```

### 10+ Games (Regular Formula)
```
Power Score = (0.30 * PointsComponent) +
              (0.25 * GoalComponent) +
              (0.20 * AdvancedStatsComponent) +
              (0.15 * ScheduleStrengthComponent) +
              (0.10 * MomentumComponent)
```

## Components Breakdown

### 1. Points Component (30-35%)
Measures team performance based on standings points and point percentage.

**Current Season Calculation:**
```
PointsComponent = normalized_points_pct
```

**Blended Calculation (Early Season):**
```
PointsComponent = (current_weight * current_points_pct) + 
                  (previous_weight * previous_points_pct)
```

Where:
- `normalized_points_pct` is the team's points percentage scaled from 0-100
- Previous season's final points percentage is carried forward

### 2. Goal Component (25-35%)
Measures team's offensive and defensive performance.

**Current Season Calculation:**
```
GoalComponent = (0.5 * normalized_goal_differential) +
                (0.3 * normalized_goals_for) +
                (0.2 * normalized_goals_against)
```

**Blended Calculation (Early Season):**
```
GoalComponent = (current_weight * current_goal_metrics) + 
                (previous_weight * previous_goal_metrics)
```

### 3. Advanced Stats Component (15-20%)
Incorporates analytics-based metrics. Only activated after 5+ games.

```
AdvancedStatsComponent = (0.4 * normalized_shot_metrics) +
                         (0.3 * normalized_powerplay_pct) +
                         (0.3 * normalized_penalty_kill_pct)
```

Returns neutral value (50) when insufficient data.

### 4. Schedule Strength Component (10-15%)
Accounts for strength of opponents faced. Only meaningful after 5+ games.

```
ScheduleStrengthComponent = normalized_opponent_points_percentage
```

Returns neutral value (50) when insufficient games played.

### 5. Momentum Component (10%)
Measures team's recent performance trend. Requires minimum games:

**10+ games:**
```
MomentumComponent = (0.6 * normalized_last10_points_pct) +
                   (0.4 * normalized_last5_goal_differential)
```

**5-9 games:**
```
MomentumComponent = (0.5 * normalized_trend) +
                   (0.5 * normalized_recent_goal_differential)
```

Returns neutral value (50) when insufficient data.

## Previous Season Carryover

### Data Sources
1. Final standings from previous season
2. Final goal differentials
3. Final ranking positions

### Conversion to Power Score
Previous season's final ranking is converted to a 0-100 scale:
```
Previous Power Score = 100 * (32 - final_rank) / 31
```

Where rank 1 = ~100 points, rank 32 = ~0 points

### Handling New Teams or Missing Data
- New teams receive neutral score (50)
- Teams without previous season data use league average metrics

## Edge Cases

### Division by Zero Prevention
All calculations use `NULLIF` to prevent division by zero:
```sql
goals / NULLIF(games_played, 0)
```

### Missing Data Handling
- Components with insufficient data return neutral values
- Coalesce statements provide fallback values
- Minimum thresholds before activation:
  - Advanced stats: 5 games
  - Schedule strength: 5 games
  - Full momentum: 10 games

## Data Updates

Power rankings are calculated daily during the NHL season, incorporating all games through the previous day. The model provides meaningful rankings at all stages:

1. **Before Season Start**: Previous season final rankings
2. **Games 1-4**: Heavy previous season influence (80-95%)
3. **Games 5-9**: Balanced blend (25-75% current season)
4. **Games 10-19**: Mostly current season (50-95%)
5. **Games 20+**: Pure current season metrics (100%)

## Visualization Enhancements

The power rankings visualization now includes:
1. Current rank and score
2. Blend indicator showing % current vs previous season
3. Games played counter
4. Component breakdown showing data availability
5. Trend chart showing ranking evolution from preseason through current
6. Confidence indicator based on games played