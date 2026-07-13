# NHL Analytics Project Roadmap

This document outlines the planned development roadmap for the NHL Analytics dbt project, prioritizing improvements to enhance data quality, usability, and analytical capabilities.

## Immediate (Completed)

### Documentation & Basic Quality ✅
- [x] Add schema.yml files for all model directories
- [x] Document all models and their primary columns
- [x] Implement basic not_null and unique tests
- [x] Create comprehensive README.md file
- [x] Add descriptions to source.yml file

## Current Phase

### Standardization (In Progress)
- [x] Implement consistent naming conventions for models
- [x] Standardize SQL formatting and commenting
- [ ] Create SQL style guide document
- [x] Apply naming conventions to all existing models

## Short-term (1 Month)

### NHL Power Rankings Model
- [ ] Design power rankings methodology
  - [x] Incorporate wins/losses, goal differential, strength of schedule
  - [ ] Include advanced metrics (e.g., expected goals, CORSI)
  - [x] Weight recent performance vs. season-long performance
- [x] Build daily snapshot model to track rankings over time
- [x] Create ranking components model to explain rankings
- [x] Implement recency weighting mechanism
- [ ] Develop visualization components for power rankings
- [ ] Publish initial power rankings before 2024-2025 season start

### Data Quality Enhancements
- [x] Add comprehensive data tests for key metrics
- [x] Create relationship tests between related models
- [x] Implement data validation checks

### Code Structure Improvements
- [x] Create macros for common calculations
- [x] Develop JSON parsing macros
- [x] Group mart models by subject area

## Medium-term (2-3 Months)

### Performance Optimization
- [x] Review materialization strategy for all models
- [ ] Implement incremental models where appropriate
- [ ] Add partitioning for larger tables

### Enhanced Documentation
- [x] Document all metrics calculations
- [x] Create exposures for dashboard components
- [ ] Build comprehensive data dictionary
- [ ] Add model lineage diagrams

### Extended Analytics
- [ ] Develop player matchup analysis
- [ ] Create team strength/weakness profiles
- [ ] Implement playoff odds modeling
- [x] Add power-play/penalty-kill specific metrics
- [x] Enhance goalie performance metrics

## Long-term (3-6 Months)

### Advanced Analytics
- [x] Implement advanced hockey metrics (CORSI, Fenwick, PDO)
- [ ] Add expected goals model
- [ ] Create player contribution metrics
- [ ] Develop team chemistry indicators
- [ ] Build playoff performance projections

### Infrastructure Improvements
- [x] Set up CI/CD with GitHub Actions
- [ ] Implement dbt Cloud or similar orchestration
- [x] Add freshness checks for source data
- [x] Create automated testing framework
- [ ] Develop data quality monitoring

### Expansion Possibilities
- [ ] Prepare architecture for multi-sport expansion
- [ ] Implement reusable project packages
- [ ] Create cross-sport analytics frameworks
- [ ] Develop historical comparison capabilities

## Maintenance & Governance
- [ ] Establish model ownership
- [ ] Create contribution guidelines
- [ ] Implement semantic versioning
- [ ] Build automated documentation updating
- [ ] Develop user feedback mechanism

---

This roadmap is a living document and will be updated as priorities evolve and new opportunities are identified.