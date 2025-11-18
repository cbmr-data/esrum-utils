# clean `/scratch` and `/tmp`

This script is intended for (automatically) cleaning up the local `/scratch` and `/tmp` folders on Esrum.

The script is used as follows:

```bash
# Preview changes to be made
sudo python3 clean-scratch-and-tmp.py | less -S
# Perform changes to file-system
sudo python3 clean-scratch-and-tmp.py --commit
```

By default, this will

1. Collect all files and folders located under `/tmp`, `/scratch`, and `/scratch/tmp`

    * Skipping files/folders owned by users with UIDs less than 65535
    * Skipping files/folders owned by users with running processes
    * Skipping files/folders created/modified/accessed in the last 24 hours
    * Skipping the following special folders

        * `/scratch/containers`
        * `/scratch/rstudio`
        * `/scratch/rstudio-proj`

2. Delete all collected files, if `--commit` is used
3. Delete all resulting empty folders, if `--commit` is used
