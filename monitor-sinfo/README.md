# monitor-sinfo

This script monitors the output from `sinfo` and reports when nodes become accessible/inaccessible. Upon detecting changes in availability, an email is sent to a provided list of recipients and a formatted slack message is sent to the provided webhooks.

## Execution

```bash
# Suggested setup:
pip install --user pipx
pipx install uv
uv venv
. .venv/bin/activate.sh
uv pip install -r requirements.txt
# Required modules must have been installed in active environment
python3 ./monitor-sinfo.py deploy.toml
```

## Configuration

The monitoring script expects a `toml` file containing SMTP servers/email address for email notifications and/or webhook URLs for Slack notifications. The format is as follow:

```toml
smtp-server = "smtp.example.com"

# Zero or more email addresses
email-recipients = [
    "abc123@sund.ku.dk",
]

# Zero or more webhooks
slack-webhooks = [
    "https://hooks.slack.com/services/etc",
]
```

## Testing

The script `sim-sinfo.py` is provided for simulating the output of `sinfo` for the purpose of testing/developing this script. Suggested usage (setup as above):

```bash
# Initialize simulator with 13 nodes with random states
python3 sinfo-sim.py --init 13
# Run monitor-sinfo.py using `sinfo-sim.py` instead of `sinfo`
python3 monitor-sinfo.py test.toml --dry-run --interval 0.1 --sinfo ./sinfo-sim.py
```
