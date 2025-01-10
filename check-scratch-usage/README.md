# check-scratch-usage

Checks the disk usage in `/scratch`, `/tmp`, and `/` across all nodes:

```console
$ make
host                         root  tmp  scratch
esrumcmpn01fl.unicph.domain  11.9  1.4  90.9
esrumcmpn02fl.unicph.domain  10.9  1.4  21.1
esrumcmpn03fl.unicph.domain  11.9  1.4  20.7
```

Utilization is reported in GB.

Note that nodes may be skipped if they are fully booked, since this tool uses `srun` to execute the checks.
