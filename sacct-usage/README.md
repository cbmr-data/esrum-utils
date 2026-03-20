# sacct-usage

The `sacct-usage` provides an easy-to-read summary of the output from `sacct` and ``sstat`, making it easier to monitor the resource usage of running and finished Slurm jobs.

```console
$ sacct-usage
User    Job   Start                   Elapsed  State      CPUsReserved  CPUsUsed  MemReserved  MemUsed  Name
abc123  1     2025-09-01 10:15:01  252:04:52s  FAILED                8       1.1        124.6    105.9  python3
abc123  2[1]  2025-09-15 16:02:35   02:49:25s  COMPLETED            32      16.3        512.0    358.7  paleomix
abc123  3     2025-09-23 12:35:19   01:00:53s  RUNNING              24      22.6         64.0     16.0  bash
```

Briefly, `sacct-usage` reports how many CPUs and how much memory has been reserved for jobs, as well as how many CPUs and how much memory the jobs actually utilized. `sacct-usage` uses `sstat` to retrieve information about a user's own running jobs, and for all jobs when running as root.

Note that the `MemUsed` values may not be accurate in cases where processes were killed due to requesting/using excess amounts of memory.


## Usage

```text
usage: sacct-usage.py [-h] [-V] [--mode {per-job,per-user}]
                      [--metric {Used,Wasted,Both}] [--sort SORT_KEY]
                      [--column-separator COLUMN_SEPARATOR] [-a | -u USERS]
                      [-S TIME] [-E TIME] [-T] [-g GROUP] [-j job(.step)]
                      [-s state_list] [--show-overhead]

options:
  -h, --help            show this help message and exit
  -V, --version         show program's version number and exit
  --mode {per-job,per-user}
                        Either show per-job or per-user statistics; 'per-user'
                        implies --allusers, unless the --user option is used
                        (default: per-job)
  --metric {Used,Wasted,Both}
                        Show either resources used or wasted, relative to
                        reservations (default: Used)
  --sort SORT_KEY, --sort-key SORT_KEY
                        Column name to sort by; prefix with `-` to reverse sort
                        (ascending) (default: Start)
  --column-separator COLUMN_SEPARATOR
                        Character or characters to use to separate columns in
                        the output. Possible values are 'spaces', 'commas', or
                        any single character. By default columns are aligned
                        using spaces when outputting to a terminal, and tabs
                        when outputting to a pipe (default: auto)
  -a, --allusers, --all-users
                        Display all users' jobs (default: False)
  -u USERS, --uid USERS, --user USERS
                        Display the specified user's jobs; see `man sacct`
                        (default: None)
  -S TIME, --starttime TIME, --start-time TIME
                        Show tasks starting at this time; see `man sacct` for
                        format (default: now-24hours)
  -E TIME, --endtime TIME, --end-time TIME
                        Show tasks ending at this time; see `man sacct` for
                        format (default: now)
  -T, --truncate        Truncate time; see `man sacct` (default: False)
  -g GROUP, --gid GROUP, --group GROUP
                        Show jobs belonging to group(s); see `man sacct`
                        (default: None)
  -j job(.step), --jobs job(.step)
                        Show only the specified jobs; see `man sacct` (default:
                        None)
  -s state_list, --state state_list
                        Show only jobs with the specified state(s); see `man
                        sacct` (default: None)
  --show-overhead       Show overhead in terms of unused memory and
                        unused/blocked CPUs. This is intended as a tool to help
                        the cluster admins (default: False)
```