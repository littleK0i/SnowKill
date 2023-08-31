# SnowKill

SnowKill is a realtime query monitoring tool for Snowflake.

SnowKill helps to detect potential problems with queries which **are currently running**. It analyzes query plans, generates notifications and possibly terminates ("kills") queries automatically.

SnowKill provides results much faster relative to conventional monitoring tools operating on finished queries data from [`QUERY_HISTORY`](https://docs.snowflake.com/en/sql-reference/account-usage/query_history) and [`GET_QUERY_OPERATOR_STATS`](https://docs.snowflake.com/en/sql-reference/functions/get_query_operator_stats).

The core logic of SnowKill relies on REST API calls. It does not require an active warehouse, which makes this kind of monitoring nearly free of charge.

## How does it work?

1. Load list of queries which are currently `RUNNING`, `QUEUED` or `BLOCKED`.
2. Load additional information about [query plans](https://docs.snowflake.com/en/user-guide/ui-query-profile) and [active locks](https://docs.snowflake.com/en/sql-reference/sql/show-locks), if necessary.
3. Check queries against list of fully customizable conditions.
4. Optionally terminate matched queries exceeding specific thresholds.
5. Detect and skip previously reported queries, avoid duplicates.
6. Send notifications about newly detected queries (via Slack, Email, etc.).

![Diagram](/misc/snowkill_how_it_works.jpg)

## Documentation

(coming soon!)

## Issues? Questions? Feedback?

Please use GitHub "Issues" to report bugs and technical problems.

Please use GitHub "Discussions" to ask questions and provide feedback.

## Created by
[Vitaly Markov](https://www.linkedin.com/in/markov-vitaly/), 2023

Enjoy!
