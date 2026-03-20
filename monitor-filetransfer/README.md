# monitor-filetransfers

Monitors a folder, sending a email or Slack notification when changes in the number and/or size of files are detected. This is intended to produce alerts when a third party (including automated processes) adds files to a folder, so that these can be examined.

## Example usage

```console
uv run monitor-filetransfers.py ./config.toml
```

The script may optionally take a list of paths, in which case only those files/folders are monitored:

```console
uv run monitor-filetransfers.py ./config.toml filenames.txt
```

## Configuration

```toml
# Location to monitor
root = "..."

# "Database" in which folder statistics are stored between runs
database = "database.txt"

# SMTP server for sending notifications
smtp_server = "..."

# Email addresses to be sent notifications when `root` changes
email_recipients = [
        "..."
]
```

## Usage

```console
usage: monitor-filetransfers.py [-h] [--log-level {DEBUG,INFO,WARNING,ERROR}]
                                TOML [FILE]

positional arguments:
  TOML                  Path to TOML file containing notification configuration
  FILE                  File containing expected filenames (default: None)

options:
  -h, --help            show this help message and exit
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        Verbosity level for console logging (default: None)
```
