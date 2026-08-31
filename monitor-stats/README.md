# monitor-stats

This script monitors various basic performance metrics (average load, overall CPU utilization, and free memory) and sends a Slack message whenever these values exceed a given threshold.

In addition, the script may look for processes matching regular expressions, and sends a warning if any of these have been running for longer than a specified time limit.

## Execution

It is recommended to use [uv](https://docs.astral.sh/uv) to install/run this script:

```bash
uv run monitor-stats config.toml
```

## Configuration

The monitoring script expects a `toml` file containing webhook URLs for Slack notifications and a list of regular exprssions. The format is as follow:

```toml

# Zero or more webhooks
slack-webhooks = [
    "https://hooks.slack.com/services/...",
]

process_blacklist = [
    "\\brg\\b.+--follow",
    "\\brsync\\b",
]
```
