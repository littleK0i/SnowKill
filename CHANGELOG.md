# Changelog

## [0.4.2] - 2024-01-04

- Add explicit timeout for `query-plan-data` API requests. It should help to prevent queries running on overloaded warehouses from blocking other checks and timing out lambda.

## [0.4.1] - 2023-09-18

- Fix calculation of amount of truncated lines during formatting.

## [0.4.0] - 2023-09-04

- Rename all "checkers" to "conditions" for more clarity.
- Improve documentation.
- Fix a problem with incorrect number of "truncates lines" while formatting notifications for long queries.
- Fix example not using retry logic for Slack notifications.

## [0.3.0] - 2023-08-31

- SnowKill was released for public under Apache 2.0 open source license.
