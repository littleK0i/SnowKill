# SnowKill

[![PyPI](https://badge.fury.io/py/snowkill.svg)](https://badge.fury.io/py/snowkill)

SnowKill is a near real-time query monitoring tool for [Snowflake Data Cloud](https://www.snowflake.com/).

SnowKill helps to detect potential problems with queries which **are currently running**. It analyzes query stats and plans, detects bad patterns, generates notifications and optionally "kills" some queries automatically.

The core logic of SnowKill relies on internal REST API calls instead of SQL queries. It does not require an active warehouse to run, which makes it possible to maintain the constant monitoring almost free of charge.

SnowKill has programmatic access to query plan from ["Query Profile"](https://docs.snowflake.com/en/user-guide/ui-query-profile) page in SnowSight. SnowKill also has access to information about locks and tries to report the exact reason for transaction collisions.

SnowKill operates on present data, which normally allows it to react much faster relative to conventional monitoring tools operating on past data from [`QUERY_HISTORY`](https://docs.snowflake.com/en/sql-reference/account-usage/query_history) and [`GET_QUERY_OPERATOR_STATS`](https://docs.snowflake.com/en/sql-reference/functions/get_query_operator_stats).

## How does it work?

1. Load list of queries which are currently `RUNNING`, `QUEUED` or `BLOCKED`.
2. Load additional information about [query plans](https://docs.snowflake.com/en/user-guide/ui-query-profile) and [active locks](https://docs.snowflake.com/en/sql-reference/sql/show-locks), if necessary.
3. Check queries against list of fully customizable conditions.
4. Optionally terminate matched queries exceeding specific thresholds.
5. Detect and skip previously reported queries, avoid duplicates.
6. Send notifications about newly matched queries (via Slack, Email, etc.).

## Notification example

![Notification example 1](/misc/notification_example_1.png)

## Quick links

- [Getting started](https://docs.snowkill.net/getting-started)
- [Implementation details](https://docs.snowkill.net/implementation-details)
- [Examples](https://docs.snowkill.net/examples)
- [Deployment best practices](https://docs.snowkill.net/deployment/best-practices)

Built-in conditions:
- [Blocked Duration](https://docs.snowkill.net/condition/built-in-conditions/blocked-duration)
- [Cartesian Join Explosion](https://docs.snowkill.net/condition/built-in-conditions/cartesian-join-explosion)
- [Estimated Scan Duration](https://docs.snowkill.net/condition/built-in-conditions/estimated-scan-duration)
- [Execute Duration](https://docs.snowkill.net/condition/built-in-conditions/execute-duration)
- [Join Explosion](https://docs.snowkill.net/condition/built-in-conditions/join-explosion)
- [Storage Spilling](https://docs.snowkill.net/condition/built-in-conditions/storage-spilling)
- [Queued Duration](https://docs.snowkill.net/condition/built-in-conditions/queued-duration)
- [Union without ALL](https://docs.snowkill.net/condition/built-in-conditions/union-without-all)

Built-in formatters:
- [Markdown](https://docs.snowkill.net/formatter/built-in-formatters/markdown)
- [Slack](https://docs.snowkill.net/formatter/built-in-formatters/slack)

Built-in storages:
- [Postgres table](https://docs.snowkill.net/storage/built-in-storages/postgres-table)
- [Snowflake table](https://docs.snowkill.net/storage/built-in-storages/snowflake-table)
- [Snowflake hybrid table](https://docs.snowkill.net/storage/built-in-storages/snowflake-hybrid-table)

## Future plans

- More conditions, formatters, storages.
- Automated testing on push.
- Maybe some conditions for groups of queries, e.g. "more than 4 queries are queued on warehouse".

## Issues? Questions? Feedback?

Please use GitHub "Issues" to report bugs and technical problems.

Please use GitHub "Discussions" to ask questions and provide feedback.

## Created by
[Vitaly Markov](https://www.linkedin.com/in/markov-vitaly/), 2025

Enjoy!
