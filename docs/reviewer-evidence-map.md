# Review Guide - DistrictPilot AI

Updated: 2026-05-30

Use this page as the short path through the repository. It keeps the review grounded in the code, docs, commands, and boundaries that are already present.

## Summary

| Field | Notes |
|---|---|
| Lane | B2G/B2B district operations analytics |
| Core idea | Forecast-to-action cards for move-in/home-service demand and district resource planning. |
| Primary reader | Local governments, field-service planners, civic analytics teams, and district operators. |
| Stack | Documentation-first |

## Open First

1. Start with the README fast path and architecture section.
2. Open `docs/service-launch-playbook.md` only when reviewing the product or service angle.
3. Check the commands below before making claims about quality.
4. Skim the CI workflows and fixture data before deeper implementation review.
5. Read the boundaries section before presenting the project externally.

## Checks

| Purpose | Command |
|---|---|
| Review gate | `Review README fast path, CI workflow, and documented demo artifacts` |

## CI

- .github/workflows/architecture-blueprint.yml
- .github/workflows/ci.yml
- .github/workflows/dependency-review.yml
- .github/workflows/repository-health.yml
- .github/workflows/repository-surface.yml
- .github/workflows/secret-scan.yml

## Evidence

- README and CI/documentation proof
- Judge fast path works
- Evidence chain is visible
- Synthetic/real data boundaries are clear

## Review Notes

| Possible offer | Working scope assumption |
|---|---|
| Planning dashboard pilot | Scope after reviewer intake |
| Forecast review workshop | reviewer-approved implementation diagnostic |
| Monthly operations monitoring setup | Scope after reviewer intake |

## Boundaries

- Forecasts are decision support
- Public-sector data lineage required
- Human review must stay explicit

## Useful Metrics

- Forecast error
- Action-card adoption
- Resource allocation cycle time
