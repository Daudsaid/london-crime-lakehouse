# Duplicate Crime IDs within a file: findings and rule recommendation

Archive 2026-08, bronze table `crimes_street`, forces `metropolitan` and `city-of-london`
(72 files, 36 months each). Produced by
[`analyses/profile_duplicate_crime_ids.py`](../analyses/profile_duplicate_crime_ids.py)
(read-only; run it with `uv run python analyses/profile_duplicate_crime_ids.py`).

A **group** is the rows sharing `(_force, _month, crime_id)`. A **variant** is a distinct
combination of all 12 source columns inside a group. "Final" outcome means anything other
than NULL, `Under investigation` or `Status update unavailable`.

## Summary

- Duplicates are rare (0.79% of groups) but 92% of them are not identical.
- The usual cause is one row with a real outcome next to one placeholder row:
  99.89% of outcome-only groups pair exactly one final outcome with a non-final one, and
  96% of the time the non-final one is `Status update unavailable`.
- In every group where exactly one variant has a final outcome (19,262 groups), that variant
  matches the Crime ID's latest event in the outcomes files. Never a mismatch.
- No Crime ID appears in more than one file, so "within the file" is the right grain.
- Recommendation: rule **R5** (below) instead of quarantine-all. It keeps 97% of the
  conflicting Crime IDs and needs only street data. It changes the agreed "no tie-breaks"
  rule, so it needs your decision.

## 0. Scope

| | rows / groups | share |
|---|---|---|
| Bronze rows | 3,440,762 | |
| Rows with no Crime ID (ASB, untouched by any rule) | 702,673 | 20.42% of rows |
| Rows with a Crime ID | 2,738,089 | 79.58% of rows |
| Groups | 2,713,999 | |
| Groups with more than one row | 21,564 | 0.79% of groups |
| Rows in those groups | 45,654 | 1.67% of rows with a Crime ID |
| Conflicting groups (more than one variant) | 19,861 | 92.10% of duplicate groups |

Group sizes: 19,719 have 2 rows, 1,372 have 3, 340 have 4, 86 have 5, 29 have 6, 12 have 7,
3 have 8, 2 have 9, 1 has 10.

## 1. Classification of duplicate groups

| Class (columns that differ) | Groups | Rows | % of duplicate groups |
|---|---|---|---|
| outcome only | 14,978 | 31,519 | 69.46% |
| outcome + crime_type | 4,095 | 8,837 | 18.99% |
| identical | 1,703 | 3,635 | 7.90% |
| crime_type only | 530 | 1,123 | 2.46% |
| outcome + location | 221 | 464 | 1.02% |
| location only | 36 | 74 | 0.17% |
| crime_type + location | 1 | 2 | 0.00% |

"location" means any of longitude, latitude, location, LSOA code or LSOA name.
`month`, `reported_by`, `falls_within` and `context` never differ.

## 2. Outcome-only groups (14,978)

Top combinations (NULL never appears among the top 20):

| Outcome pair | Groups | % |
|---|---|---|
| Investigation complete; no suspect identified / Status update unavailable | 6,945 | 46.37% |
| Status update unavailable / Unable to prosecute suspect | 4,224 | 28.20% |
| Local resolution / Status update unavailable | 1,517 | 10.13% |
| Court result unavailable / Status update unavailable | 1,130 | 7.54% |
| Unable to prosecute suspect / Under investigation | 231 | 1.54% |
| Awaiting court outcome / Status update unavailable | 195 | 1.30% |
| Local resolution / Under investigation | 139 | 0.93% |
| Investigation complete; no suspect identified / Under investigation | 125 | 0.83% |
| Action to be taken by another organisation / Status update unavailable | 121 | 0.81% |
| Offender given a caution / Status update unavailable | 110 | 0.73% |
| Formal action is not in the public interest / Status update unavailable | 94 | 0.63% |
| Offender given penalty notice / Status update unavailable | 58 | 0.39% |
| Awaiting court outcome / Under investigation | 41 | 0.27% |
| Further investigation is not in the public interest / Status update unavailable | 19 | 0.13% |
| Court result unavailable / Unable to prosecute suspect | 7 | 0.05% |
| Formal action is not in the public interest / Under investigation | 4 | 0.03% |
| Offender given a caution / Unable to prosecute suspect | 4 | 0.03% |
| Offender given penalty notice / Under investigation | 2 | 0.01% |
| Further action is not in the public interest / Status update unavailable | 2 | 0.01% |
| Further investigation is not in the public interest / Under investigation | 2 | 0.01% |

- **One final + one non-final:** 14,962 groups (99.89%). The non-final partner is
  `Status update unavailable` in at least 14,415 of them and `Under investigation` in at
  least 544 (counted from the top 20 above; the rest of the 14,962 are outside it).
- **Two different finals:** 16 groups (0.11%), for example `Court result unavailable` against
  `Unable to prosecute suspect`.
- **Two non-final values only:** none.

`Awaiting court outcome` and `Court result unavailable` are counted as final here, following
the non-final list you gave.

## 3. Groups where crime_type differs (4,626)

4,626 groups, 23.29% of conflicting groups. In 4,095 of them (88.52%) the outcome differs too.

| Crime types in the group | Groups | % |
|---|---|---|
| Public order / Violence and sexual offences | 1,368 | 29.57% |
| Shoplifting / Violence and sexual offences | 742 | 16.04% |
| Criminal damage and arson / Violence and sexual offences | 534 | 11.54% |
| Public order / Shoplifting | 344 | 7.44% |
| Other crime / Violence and sexual offences | 233 | 5.04% |
| Criminal damage and arson / Public order | 202 | 4.37% |
| Drugs / Possession of weapons | 160 | 3.46% |
| Robbery / Violence and sexual offences | 157 | 3.39% |
| Drugs / Violence and sexual offences | 87 | 1.88% |
| Other theft / Violence and sexual offences | 63 | 1.36% |
| Drugs / Other crime | 60 | 1.30% |
| Possession of weapons / Violence and sexual offences | 54 | 1.17% |
| Burglary / Violence and sexual offences | 36 | 0.78% |
| Other crime / Public order | 35 | 0.76% |
| Public order / Robbery | 34 | 0.73% |
| Criminal damage and arson / Public order / Violence and sexual offences | 25 | 0.54% |
| Burglary / Public order | 23 | 0.50% |
| Theft from the person / Violence and sexual offences | 22 | 0.48% |
| Drugs / Public order | 22 | 0.48% |
| Possession of weapons / Public order | 21 | 0.45% |

The reclassification has no consistent direction. Among the groups where one variant has
the only final outcome, `Violence and sexual offences` is on the final row against
`Public order` 734 times, but `Public order` is on the final row against
`Violence and sexual offences` 383 times. So there is no crime-type tie-break rule hiding in
the data. I cannot tell from this data which crime_type is the correct one.

## 4. Groups where location differs (258)

- 1.30% of conflicting groups; 221 of them (85.66%) also differ in outcome.
- Largest distance between two coordinates in the group: median 323 m, 90th percentile
  1,720 m, maximum 20,365 m.
- 65 groups have no coordinate change (text or LSOA differs but coordinates are equal or NULL).
- The LSOA code changes in 137 groups.

## 5. Cross-file

No Crime ID appears in more than one `(force, month)` file of this archive, and none spans
more than one force. Within-file is the whole problem.

## 6. Outcomes-file cross-check

Read directly from `data/_downloads/2026-08.zip` (72 outcomes files, copied to a temp
folder and deleted afterwards; nothing landed). 2,872,758 events for 2,490,104 Crime IDs.
Per Crime ID the latest event month holds one event for 2,284,948 IDs and two or more for
205,156 (8.2%), so "the latest event" is sometimes a set of events.

Vocabulary: street files say `Awaiting court outcome` / `Court result unavailable` where the
outcomes files say `Suspect charged`. I treated those as equal (an assumption). The outcomes
files never contain `Under investigation` or `Status update unavailable`.

A row "matches" if its outcome is among the outcomes of the Crime ID's latest-month events.

| Outcome-only groups (14,978) | Groups | % |
|---|---|---|
| Exactly one variant matches | 14,966 | 99.92% |
| More than one variant matches | 12 | 0.08% |
| No variant matches | 0 | 0% |
| Crime ID has no outcomes event | 0 | 0% |

The 12 are the groups with two different finals where both finals appear in the latest month.
Across all conflicting groups where exactly one variant has a final outcome (19,262 groups),
that variant matches the latest outcomes event in **100%** of cases.

## 7. Largest groups

- Largest group, 10 rows: Met 2024-02, Public order, Islington 014C, `On or near Penally
  Place`, outcome `Unable to prosecute suspect`. All 10 rows are identical, so it is not a
  conflict.
- Largest conflicting group, 9 rows: Met 2023-11, Violence and sexual offences, Westminster
  020C, `On or near St Margaret Street`. Eight rows are identical with outcome
  `Status update unavailable`; one has `Investigation complete; no suspect identified`.

## 8. Candidate rules: impact on the 42,019 rows in conflicting groups

Identical duplicate groups (1,703) collapse to one row under every rule: 1,932 extra rows.
"Dropped" means superseded rows in a group where one variant was kept.

| Rule | Rows kept | Rows dropped | Rows quarantined | Crime IDs not in silver |
|---|---|---|---|---|
| R1 quarantine all (current) | 0 | 0 | 42,019 | 19,861 (0.73% of groups) |
| R2 outcome precedence, quarantine if other columns differ | 14,962 | 16,523 | 10,534 | 4,899 (0.18%) |
| R3 outcomes-file authority | 19,270 | 21,487 | 1,262 | 591 (0.02%) |
| R4 keep all rows, flag them | 42,019 | 0 | 0 | 0 |
| R5 (proposed) single final outcome wins | 19,262 | 21,478 | 1,279 | 599 (0.02%) |

- **R1:** loses 0.73% of all Crime IDs, mostly real crimes whose only fault is a placeholder
  duplicate row.
- **R2:** keeps the final row only when outcome is the sole difference. It quarantines the
  4,883 groups where crime_type or location also differs, although most of those also have
  one clearly final row. It also quarantines 16 groups with two different finals.
- **R3:** the best coverage, but it makes the outcomes files a dependency of silver, which
  we have not extracted or modelled. Its 591 quarantines are 386 with no outcomes event, 205
  where several variants match.
- **R4:** keeps every row, so gold must pick one row per Crime ID or count distinct
  `crime_id`, and any per-row measure would double count. This postpones the decision.
- **R5:** for a conflicting group, count the variants that have a final outcome. If exactly
  one, keep it and drop the other rows as superseded. Otherwise quarantine all copies.

### R5 detail

| Decision | Groups | Rows |
|---|---|---|
| keep the one variant with a final outcome | 19,262 | 40,740 |
| quarantine: no final outcome | 386 | 812 |
| quarantine: several variants with a final outcome | 213 | 467 |

- The kept variant matches the Crime ID's latest outcomes event in 19,262 of 19,262 groups.
- R5 and R3 agree on 19,262 keeps and 386 + 205 quarantines. They differ in 8 groups, where
  R3 can keep a variant that R5 quarantines.
- The 386 "no final outcome" groups all differ in crime_type (350) or location (36).

## Recommendation

Adopt **R5**.

1. Collapse identical copies (unchanged). Always the same row, so it is safe.
2. If copies differ and exactly one variant has a final outcome, keep that variant. The
   other rows are counted as `deduped_rows` with a reason such as `superseded_by_final_outcome`.
3. Otherwise quarantine all copies with `_dq_reason = 'conflicting_duplicate_crime_id'`
   (no final outcome, or several variants with one).

Why: it is deterministic and explainable in one sentence, needs only street data, loses
0.02% of groups instead of 0.73%, and is corroborated by the outcomes files (100% of kept
rows match the latest event). R3 would rescue only 8 more groups at the cost of a new
dependency, so I would keep the outcomes files as a validation check, not as an input.

Things to decide or accept:
- R5 is a precedence rule, so it replaces the agreed "no tie-breaks" rule in CLAUDE.md.
  Its justification is the data above, not an arbitrary choice.
- For crime_type and location conflicts, R5 keeps the row with the final outcome, but the
  data cannot show whether that row has the right crime_type. The outcome is corroborated;
  the crime_type is not.
- "Final" is the definition you gave. `Awaiting court outcome` counts as final.

## Decision

R5 was approved with refinements, now written into CLAUDE.md section 5:

1. Identical copies collapse to one (`deduped_rows`).
2. If the distinct variants differ and exactly one has a progressed outcome, keep it. The
   other variants are `superseded_rows`. The kept row gets `dq_attribute_conflict = true` if
   crime_type, location, lat/long or LSOA differed among the variants.
3. Otherwise all copies are quarantined with `_dq_reason = 'conflicting_duplicate_crime_id'`.
4. ASB rows (no Crime ID) are never deduplicated.

Outcome vocabulary is two explicit lists, `PLACEHOLDER_OUTCOMES` (NULL, `Status update
unavailable`, `Under investigation`) and `PROGRESSED_OUTCOMES` (every other observed value,
including `Awaiting court outcome`). A value in neither list raises. Reconciliation is
`bronze = silver + quarantined + deduped + superseded`. The outcomes-file cross-check is
validation only, never a silver input.

The analysis above called these groups "final" and "non-final"; the decision uses
"progressed" and "placeholder" for the same two sets.

## Caveats

- One archive (2026-08) and two forces. Older archives and other forces may differ; the
  pattern should be re-profiled when more are loaded.
- That `Status update unavailable` rows are placeholders is inferred from the data, not
  from police.uk documentation.
- The street-to-outcomes vocabulary mapping (street `Awaiting court outcome` / `Court result
  unavailable` = outcomes `Suspect charged`) is an assumption. It affects only groups that include
  `Awaiting court outcome` or `Court result unavailable`.
