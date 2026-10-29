# Data caveats

What to know before using the gold tables. Everything here is an observation from the data
or a documented choice; causes are not claimed unless stated. Figures are from archive
2026-08 (3,440,762 bronze rows, months 2023-09 to 2026-08) for the two ingested forces,
`metropolitan` and `city-of-london`.

## Coverage

- **Greater Manchester is absent.** The 2026-08 archive contains 44 force slugs and none is
  Greater Manchester.
- **The force set varies by month.** In the archive's street files `btp` has no file from
  2025-02 onwards and `gloucestershire` has none from 2026-02 onwards, so a month has 43 or
  42 forces' street files, not 44. Code must not assume a fixed set.
- **Only two forces are ingested** (`config.FORCES`), and only one archive (2026-08) is
  loaded. `dim_force` maps those two; any other force raises until it is added.

## Duplicate Crime IDs

Within a file, 21,564 Crime IDs repeat (0.79% of Crime ID groups) and 19,861 of those are not
identical across the 12 source columns. Silver resolves them with the outcome-precedence
rule (R5): 1,932 identical copies collapsed, 21,478 placeholder copies superseded, and
1,279 rows (599 Crime IDs) quarantined because no single variant had a progressed outcome.
Full profile and rationale: [duplicate-crime-ids.md](duplicate-crime-ids.md).

Anti-social behaviour rows have no Crime ID, are never deduplicated, and identical ASB rows
are kept as separate incidents.

## Metropolitan Police records located outside London

- 12,868 of the 3,378,444 Metropolitan Police rows that have an LSOA (0.38%) fall in LSOAs
  outside London. City of London Police has 56 more. Together: 12,924 rows in 6,059 LSOAs
  across 297 local authorities, against 4,981 London LSOAs (all 33 London authorities
  present).
- These rows are mostly not anti-social behaviour (1.7% ASB, against 20.7% for London rows)
  and all have coordinates.
- 4,104 of the 6,059 outside-London LSOAs appear in a single month only. In the latest
  12 months, 2,004 of the 2,674 outside-London LSOAs with any record have exactly one record.
- **Drop from 2026-05 (observed, cause unverified).** Outside-London rows per month ran
  224 to 681 (mean 400.7) from 2023-09 to 2026-04, then were 26 (2026-05), 22 (2026-06),
  35 (2026-07) and 20 (2026-08).
- The dashboard's borough views, including "All London", use London LSOAs only. The 12,924 outside-London rows and the 12,210 rows with no LSOA are in `fct_crimes` and `mart_monthly_crimes` but not in the borough marts; the gold reconciliation checks that the borough mart plus those two groups equals the fact table (3,390,939 + 12,924 + 12,210 = 3,416,073).
- Gold keeps these rows in `dim_lsoa` and `fct_crimes` (`is_london = false`) and in
  `mart_monthly_crimes`. `mart_lsoa_ranking`, `mart_borough_monthly` and `mart_borough_outcomes` cover London LSOAs only.

## LSOA to local authority is a name match

`dim_lsoa.local_authority` is the LSOA name without its trailing code ("Hounslow 021D" ->
"Hounslow"), and `is_london` is a match against the explicit `LONDON_AUTHORITIES` list (32
boroughs plus City of London). This is a known limitation: an ONS LSOA -> local authority
lookup would replace both. Names that do not end in a 3-digit + letter code make gold raise.
In the loaded data every LSOA code has exactly one name and every name exactly one code.

## Outcome maturity

`mart_outcome_rates` leaves out ASB, the newest 16 months (`OUTCOME_MATURITY_MONTHS`) and
the months in `OUTCOME_EXCLUDED_MONTHS` (`2024-10`). With a latest month of 2026-08 it uses
2023-09 to 2025-04 minus 2024-10: 19 months and 1,424,734 non-ASB rows (52.5% of the
2,713,400 non-ASB rows). Every other gold table keeps 2024-10.

**The rule.** Exclude the smallest number of most-recent months so that every remaining
month newer than the baseline window has a placeholder share within 2pp of the baseline
(the median of months older than 18 months). Set from archive 2026-08; re-check when a new
archive lands with `uv run python analyses/profile_outcome_maturity.py`.

**The maturity curve** (non-ASB rows whose outcome is a placeholder: NULL, "Under
investigation" or "Status update unavailable"):

| Month | Age | Placeholder share | | Month | Age | Placeholder share |
|---|---|---|---|---|---|---|
| 2023-09 | 35 | 1.9% | | 2025-03 | 17 | 3.6% |
| 2023-10 | 34 | 1.9% | | 2025-04 | 16 | 4.0% |
| 2023-11 | 33 | 2.2% | | **2025-05** | 15 | **4.8%** (first month outside the band) |
| 2023-12 | 32 | 2.9% | | 2025-06 | 14 | 5.6% |
| 2024-01 | 31 | 4.9% | | 2025-07 | 13 | 5.7% |
| 2024-02 | 30 | 1.8% | | 2025-08 | 12 | 5.9% |
| 2024-03 | 29 | 2.3% | | 2025-09 | 11 | 6.0% |
| 2024-04 | 28 | 2.5% | | 2025-10 | 10 | 6.7% |
| 2024-05 | 27 | 3.1% | | 2025-11 | 9 | 7.3% |
| 2024-06 | 26 | 2.3% | | 2025-12 | 8 | 8.3% |
| 2024-07 | 25 | 2.3% | | 2026-01 | 7 | 11.2% |
| 2024-08 | 24 | 2.1% | | 2026-02 | 6 | 13.7% |
| 2024-09 | 23 | 2.4% | | 2026-03 | 5 | 15.9% |
| **2024-10** | 22 | **25.6%** (excluded) | | 2026-04 | 4 | 17.3% |
| 2024-11 | 21 | 2.5% | | 2026-05 | 3 | 19.1% |
| 2024-12 | 20 | 2.6% | | 2026-06 | 2 | 20.9% |
| 2025-01 | 19 | 2.9% | | 2026-07 | 1 | 27.9% |
| 2025-02 | 18 | 3.2% | | 2026-08 | 0 | 43.3% |

**Why 16.** The baseline (median of the 17 months 2023-09 to 2025-01) is 2.375%, so the
band is up to 4.38%. Leaving out the newest 15 months keeps 2025-05 at 4.8% (2.46pp above
the baseline); leaving out 16 months keeps 2025-04 at 4.0% (1.60pp) as the newest month, and
every kept month newer than the baseline window is inside the band. One baseline-window
month, 2024-01 (4.9%), is itself outside the band; it defines the baseline rather than
being judged against it. The share keeps rising slowly through 2025 (3.2% in 2025-02 to 6.0%
in 2025-09), so the line is partly a judgement; a looser tolerance gives a smaller cutoff
(2.5pp: 15 months, 3pp: 15, 4pp: 11).

**Why 2024-10 is excluded from outcome rates.** 20,778 of its 81,301 non-ASB rows (25.6%)
have the placeholder outcome "Status update unavailable", against 1.8% to 4.9% for every
other month of 2023-09 to 2025-03. It is excluded from `mart_outcome_rates` only; the cause
has not been investigated.

**Placeholder values switch in 2026-04 (cause unverified).** Up to 2026-03 every placeholder
row is "Status update unavailable" (no "Under investigation" rows and no NULL outcomes). From
2026-04 every placeholder row is "Under investigation" (no "Status update unavailable"). Both
count as placeholders in silver and gold.

## Metropolitan Police source changes, 2026-04/05

Three things change in the Metropolitan Police files around 2026-04 and 2026-05. These are
observations from archive 2026-08, the only archive landed, so earlier archives cannot be
compared. The cause of each is unverified and none of them is a pipeline fault: every row
reconciles and none of these rows is quarantined.

**1. Criminal damage and arson falls by about 94% from 2026-05.** Silver rows, all forces,
and the Metropolitan Police and City of London Police split for the last seven months:

| | 2025-09 | 2025-12 | 2026-02 | 2026-03 | 2026-04 | 2026-05 | 2026-06 | 2026-07 | 2026-08 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| All forces | 4,526 | 4,247 | 3,998 | 4,432 | 4,352 | **245** | 242 | 281 | 254 |
| Metropolitan | | | 3,978 | 4,405 | 4,322 | **221** | 215 | 245 | 240 |
| City of London | | | 20 | 27 | 30 | 24 | 27 | 36 | 14 |

- The raw `crime_type` strings in bronze are the same 14 values in all 36 months, so there
  is no renamed or new value at the same time.
- The surviving Metropolitan rows all have a location and an LSOA, and the same outcome mix
  as before. They cover about 200 LSOAs a month, against about 2,400 before.
- No other crime type changes by more than 50% from one month to the next between 2025-09
  and 2026-08 (the largest are Bicycle theft at 26% and Theft from the person at -21%).

**2. Other types rise by more than usual in the same month.** Metropolitan rows, April to
May, in 2025 and in 2026:

| Crime type | 2025-04 | 2025-05 | Change | 2026-04 | 2026-05 | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Criminal damage and arson | 4,640 | 4,827 | +187 | 4,322 | 221 | **-4,101** |
| Violence and sexual offences | 21,617 | 23,231 | +1,614 | 22,285 | 25,545 | +3,260 |
| Anti-social behaviour | 19,454 | 21,562 | +2,108 | 19,506 | 23,030 | +3,524 |
| Public order | 4,457 | 4,934 | +477 | 4,553 | 5,340 | +787 |
| Other theft | 8,058 | 8,555 | +497 | 7,278 | 8,058 | +780 |
| Shoplifting | 7,000 | 7,813 | +813 | 8,124 | 8,926 | +802 |
| Theft from the person | 7,573 | 7,008 | -565 | 5,771 | 5,656 | -115 |
| Robbery | 2,598 | 2,748 | +150 | 2,414 | 2,787 | +373 |
| Vehicle crime | 6,761 | 7,124 | +363 | 6,597 | 6,989 | +392 |
| Drugs | 4,602 | 4,946 | +344 | 3,592 | 3,418 | -174 |
| Bicycle theft | 1,174 | 1,338 | +164 | 1,122 | 1,367 | +245 |
| Burglary | 3,629 | 3,814 | +185 | 3,498 | 3,528 | +30 |
| Other crime | 1,163 | 1,054 | -109 | 1,160 | 1,149 | -11 |
| Possession of weapons | 485 | 430 | -55 | 671 | 659 | -12 |

The total number of Metropolitan rows hardly moves (90,893 to 96,673, +6%). Taken together,
the other thirteen types gain about 3,900 rows more than their 2025 April-to-May change,
against 4,288 fewer for Criminal damage and arson. That is consistent with some records
being recorded under another type (mostly violence and anti-social behaviour), but nothing
in the data shows it.

**3. Three Metropolitan changes, two of them in 2026-05.** Metropolitan rows per month:

| | 2026-03 | 2026-04 | 2026-05 | 2026-06 | 2026-07 | 2026-08 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Placeholder "Status update unavailable" | 11,736 | 0 | 0 | 0 | 0 | 0 |
| Placeholder "Under investigation" | 0 | 12,418 | 14,095 | 15,660 | 21,870 | 31,485 |
| Rows in LSOAs outside London | 526 | 479 | **23** | 18 | 32 | 18 |
| Criminal damage and arson rows | 4,405 | 4,322 | **221** | 215 | 245 | 240 |

- The placeholder text changes in 2026-04 (see "Outcome maturity" above); the outside-London
  rows and the Criminal damage and arson rows both change in 2026-05.
- The outside-London drop is not specific to one type: Violence and sexual offences outside
  London goes from 275 (2026-04) to 7 (2026-05).
- All three affect only the Metropolitan Police; City of London Police is unchanged. They may
  share a cause on the Metropolitan side, but the data does not say so.

**What the pipeline does about it.** `dq_volume_anomalies` (gold) flags any force, crime type
and month whose count differs from the median of the previous three months by more than 50%,
when that median is at least 500 rows. It logs each flag at WARNING and never fails a run.
On archive 2026-08 it flags exactly two rows, Metropolitan Criminal damage and arson in
2026-05 (-94.9%) and 2026-06 (-95.0%; the baseline still holds two normal months), and both
are acknowledged in `config.ACKNOWLEDGED_VOLUME_ANOMALIES`. An acknowledged flag stays in the
table and on the dashboard with its note. The dashboard marks 2026-05 on the trend chart as
"Met recording change" and lists the flags in its data-quality panel.

## Rankings are counts

`mart_lsoa_ranking` ranks London LSOAs by non-ASB `crime_count` over the newest 12 months
(2025-09 to 2026-08), with `asb_count` alongside. There is no population or footfall
denominator, so these are counts, not rates. The highest counts are in central LSOAs
(Westminster 013G with 10,107, Westminster 018A with 7,062, Westminster 013B with 6,824, City
of London 001F with 6,309, Hillingdon 031A with 4,890). They show where crimes were
recorded, not where residents are at higher risk.

## Severity tiers are our judgement

`dim_crime_type.severity_tier` (high / medium / low) is an explicit mapping written for this
project, not an official classification. It follows the logic of the ONS Crime Severity
Score (harm to the person and offence seriousness):

- high: Violence and sexual offences, Robbery, Burglary, Possession of weapons
- medium: Criminal damage and arson, Drugs, Theft from the person, Vehicle crime, Public order
- low: Anti-social behaviour, Bicycle theft, Other theft, Shoplifting, Other crime

The outcome groups in `dim_outcome` (charged_or_court, out_of_court, no_further_action,
other_organisation, pending, not_applicable) are likewise our own grouping of the police.uk
outcome categories.
