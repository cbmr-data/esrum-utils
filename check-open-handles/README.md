# check-open-handles

This script checks for open handles, including current working directories, that are located where they will generate warnings. This may either be due to autofs warning on non-existing mappings (in the `/projects` or `/datasets` root folders) or due to CIFS warning due to expired Kerberos tickets in network drives.

## Usage

```console
sudo python3.11 check-open-handles.py
```
