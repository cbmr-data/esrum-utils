# esrum-utils

This repository contains scripts related to monitoring, administration, and other tasks on Esrum:

- [add-accessions](add-accessions) \
  Add (dbSNP) IDs to a whitespace separated table of variants, based on table plain-text table of variants and IDs.
- [archive-old-data](archive-old-data) \
  Scripts for compressing old data files, while preserving filesystem meta-information.
- [check-open-handles](check-open-handles) \
  Check for open file handles, that may generate syslog warnings due to triggering autofs.
- [fix_permissions](fix_permissions) \
  Fix/cleans up permissions for files and folders that are intended to be shared with other users.
- [ic-container](ic-container) \
  Podman/singularity container and wrapper script for running `ic`, a post-Imputation data checking program
- [jupyter-slurm](jupyter-slurm) \
  Python module for running Slurm from Python/Jupyter notebooks
- [monitor-filetransfer](monitor-filetransfer) \
  Monitors a folder, sending a email or Slack notification when changes in the number and/or size of files are detected
- [monitor-members](monitor-members) \
  Monitors changes to membership of LDAP groups and reports via Slack webhooks, and keep `sacct` accounts in sync with an LDAP group.
- [monitor-sinfo](monitor-sinfo) \
  Monitor the output from the Slurm `sinfo` command and reports when nodes become accessible/inaccessible via e-mail or Slack webhooks.
- [monitor-stats](monitor-stats) \
  Monitor load average, CPU%, MEM%, and running processes on a node, sending
  Slack notifications when dynamic limits are exceeded or a blacklisted process
  has been running for some time.
- [peak-user-memory](peak-user-memory) \
  Collects and reports peak virtual memory usage (PeakVM) for user processes.
- [sacct-usage](sacct-usage) \
  Wrapper script around `sacct` and `sstat` that reports reserved vs used resources for Slurm jobs
- [simple-nfs-stats](simple-nfs-stats) \
  Print latency/throughput statistics for each NFS mount-point.
- [slurm-usage-summary](slurm-usage-summary) \
  Query `sacct` for job information and print report (optionally) aggregated by research group.
- [tabulate-files](tabulate-files) \
  Script for generating table of files and folders, to allow comparison between two locations on different systems.

It is intended that some or all of these tools be made available to users, in particular those related to monitoring jobs. Confidential information should therefore not be checked into this repo.

See the subfolders for documentation of the individual tools.
