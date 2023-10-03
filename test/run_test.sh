#!/bin/sh

# Set the following environment variables:
# - SNOWFLAKE_ACCOUNT
# - SNOWFLAKE_USER
# - SNOWFLAKE_PASSWORD

cd "${0%/*}"

pytest -W ignore::DeprecationWarning --tb=short */*.py
