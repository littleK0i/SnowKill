# Changelog

## [0.4.5] - 2024-01-26

- Finalize debug message and exception handling when query plan is not available for running queries.

## [0.4.4] - 2024-01-25

- Catch any exception during debug mode. Exception hierarchy and wrapping in Snowflake driver is very confusing.

## [0.4.3] - 2024-01-25

- Introduce (temporary?) debug mode and debug logger to help identify rare issues with 504 Gateway Timeout response while loading query plan for very specific queries.

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
