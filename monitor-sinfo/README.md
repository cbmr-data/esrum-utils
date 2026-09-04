# monitor-sinfo

This script monitors the output from `sinfo` and reports when nodes become accessible/inaccessible. Upon detecting changes in availability, an email is sent to a provided list of recipients and a formatted slack message is sent to the provided webhooks.

## Execution

```bash
uv run python3 ./monitor-sinfo.py deploy.toml
```

## Configuration

The monitoring script expects a `toml` file containing SMTP servers/email address for email notifications and/or webhook URLs for Slack notifications. The format is as follow:

```toml
smtp_server = "smtp.example.com"

# Zero or more email addresses
email_recipients = [
    "abc123@sund.ku.dk",
]

[slack]

# Optional URL to Slack webhook
# webhook_url = "https://hooks.slack.com/services/..."

# Optional name of environment variable containing Slack webhook URL
# webhook_env = "NAME_OF_VARIABLE"

# Optional path to file containing a single Slack webhook URL
# webhook_path = "/path/to/webhook.txt"
```

## Testing

The script `sim-sinfo.py` is provided for simulating the output of `sinfo` for the purpose of testing/developing this script. Suggested usage (setup as above):

```bash
# Initialize simulator with 13 nodes with random states
uv run python3 sinfo-sim.py --init 13
# Run monitor-sinfo.py using `sinfo-sim.py` instead of `sinfo`
uv run python3 monitor-sinfo test.toml --dry-run --interval 0.1 --sinfo ./sinfo-sim.py
```
