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
- [ ] Implement consistent naming conventions for models
- [ ] Standardize SQL formatting and commenting
- [ ] Create SQL style guide document
- [ ] Apply naming conventions to all existing models

## Short-term (1 Month)

### NHL Power Rankings Model
- [ ] Design power rankings methodology
  - [ ] Incorporate wins/losses, goal differential, strength of schedule
  - [ ] Include advanced metrics (e.g., expected goals, CORSI)
  - [ ] Weight recent performance vs. season-long performance
- [ ] Build daily snapshot model to track rankings over time
- [ ] Create ranking components model to explain rankings
- [ ] Implement recency weighting mechanism
- [ ] Develop visualization components for power rankings
- [ ] Publish initial power rankings before 2024-2025 season start

### Data Quality Enhancements
- [ ] Add comprehensive data tests for key metrics
- [ ] Create relationship tests between related models
- [ ] Implement data validation checks

### Code Structure Improvements
- [ ] Create macros for common calculations
- [ ] Develop JSON parsing macros
- [ ] Group mart models by subject area

## Medium-term (2-3 Months)

### Performance Optimization
- [ ] Review materialization strategy for all models
- [ ] Implement incremental models where appropriate
- [ ] Add partitioning for larger tables

### Enhanced Documentation
- [ ] Document all metrics calculations
- [ ] Create exposures for dashboard components
- [ ] Build comprehensive data dictionary
- [ ] Add model lineage diagrams

### Extended Analytics
- [ ] Develop player matchup analysis
- [ ] Create team strength/weakness profiles
- [ ] Implement playoff odds modeling
- [ ] Add power-play/penalty-kill specific metrics
- [ ] Enhance goalie performance metrics

## Long-term (3-6 Months)

### Advanced Analytics
- [ ] Implement advanced hockey metrics (CORSI, Fenwick, PDO)
- [ ] Add expected goals model
- [ ] Create player contribution metrics
- [ ] Develop team chemistry indicators
- [ ] Build playoff performance projections

### Infrastructure Improvements
- [ ] Set up CI/CD with GitHub Actions
- [ ] Implement dbt Cloud or similar orchestration
- [ ] Add freshness checks for source data
- [ ] Create automated testing framework
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