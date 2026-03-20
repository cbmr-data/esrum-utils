# monitor-members

This script monitors membership of LDAP groups and reports changes via Slack webhooks. Additionally, the script can be used to keep `sacct` accounts up to date with a LDAP group, adding and removing members as required.

## Execution

```bash
# To monitor changes to LDAP groups
uv run monitor_members ldap configuration.toml
# To sync changes to an LDAP group to sacct
uv run monitor_members sacct configuration.toml
```

## Configuration

The monitoring script requires a configuration file in `toml` format:

```toml
# Database of (changes to) membership
database = "database.sqlite3"

# Optional Kerberos keytab file used to authenticate for LDAP access
[kerberos]

username = "abc123@UNICPH.DOMAIN"
keytab = "abc123.keytab"

# Zero or more webhooks; used by both the `ldap` and the `sacct` command
[slack.urls]

default = "https://hooks.slack.com/services/..."
other-hook = "https://hooks.slack.com/services/..."
# Webhook for testing, see below
localhost = "http://localhost:8000"

# Settings relating to the `ldap` monitoring command
[ldap]

# LDAP server URL
uri = "ldap:///dc%3Dunicph%2Cdc%3Ddomain"
# LDAP search group
searchbase = "dc=unicph,dc=domain"

# Groups for which additions and removals should trigger a warning
sensitive_groups = [
    "*-admin",
    "*-rw",
    "SRV-esrumcmpn-users",
    "SRV-esrumcont-users",
    "SRV-esrumgpun-users",
]

# Groups for which removals should trigger a warning
mandatory_groups = [
    "srv-esrumhead-users",
    "srv-esrumweb-users",
]

# List of regular groups to monitor; may overlap with the above
groups = [
    "SRV-esrumhead-admin",
    "SRV-esrumhead-users",
]

# Settings relating to the `sacct` monitoring command
[sacct]

ldap_group = "srv-esrumhead-users"
cluster = "cluster"
account = "cbmr"

# Command to be executed when a new user is added to the monitored group
add_member = [
    "sudo",
    "sacctmgr",
    "-i",
    "create",
    "user",
    "name={user}",
    "cluster={cluster}",
    "account={account}"
]

# Command to be executed when a new user is removed from the monitored group
remove_member = [
    "sudo",
    "sacctmgr",
    "-i",
    "delete",
    "user",
    "name={user}",
    "cluster={cluster}",
    "account={account}"
]

```

## Creating a keytab

To create a keytab file, run the following commands replacing `abc123` with your username:

```console
$ ktutil
ktutil:  addent -password -p abc123@UNICPH.DOMAIN -k 1 -e aes256-cts
Password for abc123@UNICPH.DOMAIN: ************
ktutil:  wkt abc123.keytab
ktutil:  quit
```

This writes the keytab to `abc123.keytab`.

## Testing

The script `simulator/ldapsearch.py` is provided for simulating changes to a set of groups:

```console
uv run monitor_members ldap test.toml \
    --slack localhost \
    --ldapsearch-exe ./simulator/ldapsearch.py \
    --kinit-exe true
```

Use `nc` to monitor the response sent to localhost:

```console
while true; do true | nc -lN -p 8080;echo;done
```

The output may be previewed using <https://app.slack.com/block-kit-builder/>

Limited testing of the `sacct` command can be performed via

```console
uv run monitor_members sacct test.toml \
    --slack localhost \
    --ldapsearch-exe ./simulator/ldapsearch.py \
    --kinit-exe true \
    --sacctmgr-exe true
```