# slurm-usage-summary

This script queries `sacct` for job information and print either a summary of this information (optionally) aggregated by group.

```console
$ python3 slurm-usage-summary.py summary users_and_groups.tsv | column -t
Year  Month  Group   Users  Jobs   CPUNodeHours  GPUNodeHours
2023  12     Group2  3      3399   3316.5        0.0
2023  12     Group1  5      2385   11349.8       7.2
2023  12     Group1  6      2567   2363.2        0.0
```

Alternatively, a per-day, per-user report can be generated

```console
$ python3 slurm-usage-summary.py report users_and_groups.tsv | column -t
Date        Group        User    Jobs  CPUNodeHours  GPUNodeHours
2023-12-01  Group1       abc123  29    1.4           0.0
2023-12-01  Group2       def456  21    0.2           0.0
2023-12-01  Group1       ghi789  75    5.3           0.0
[...]
```

`NodeHours` are the sums of the runtimes of jobs multiplied by the number of CPUs reserved, either on the compute queue (`standardqueue`) or on the GPU queue (`gpuqueue`). For example, a job that reserved 12 CPUs and ran for 2 hours on the `standardqueue` would count for 24 `CPUNodeHours`.
