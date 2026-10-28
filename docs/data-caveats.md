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
- Gold keeps these rows in `dim_lsoa` and `fct_crimes` (`is_london = false`) and in
  `mart_monthly_crimes`. `mart_lsoa_ranking` covers London LSOAs only.

## LSOA to local authority is a name match

`dim_lsoa.local_authority` is the LSOA name without its trailing code ("Hounslow 021D" ->
"Hounslow"), and `is_london` is a match against the explicit `LONDON_AUTHORITIES` list (32
boroughs plus City of London). This is a known limitation: an ONS LSOA -> local authority
lookup would replace both. Names that do not end in a 3-digit + letter code make gold raise.
In the loaded data every LSOA code has exactly one name and every name exactly one code.

## Outcome maturity

`mart_outcome_rates` leaves out ASB and the newest 6 months (`OUTCOME_MATURITY_MONTHS`).
With a latest month of 2026-08 it uses 2023-09 to 2026-02 (2,264,176 non-ASB rows).

What the data shows: the share of non-ASB rows whose outcome is a placeholder (NULL, "Under
investigation" or "Status update unavailable") is 1.8% to 4.9% for each month from 2023-09
to 2025-03 except 2024-10, which is 25.6%. After that it rises month by month: 6.0% in
2025-09, 11.2% in 2026-01, 17.3% in 2026-04, 20.9% in 2026-06 and 43.3% in 2026-08. The
2024-10 value is an outlier that I have not investigated.

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
