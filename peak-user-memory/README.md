# peak-user-memory

This script collects user processes from `/proc`, excluding UIDs < 1000 by default, and reports the peak virtual memory usage (PeakVM) for every process with a peak of at least 1 GB by default.

Note that PeakVM may not necessarily reflect the actually peak memory, and may exceed the reserved amount, as Linux allows processes to over-allocate virtual memory. However, a process cannot have used *more* than this, allowing for some sampling uncertainty.

## Execution

```bash
$ peak-user-memory | column -t
Hostname        User    PeakVM  PID     Process
esrumhead01fl   slurm   1.2     2372    slurmdbd
esrumhead01fl   abc123  9.8     121650  python
esrumhead01fl   def456  202.6   230667  jupyter-noteboo
```

Srun can be used to execute the tool on multiple nodes simultaneously, but this requires that there is free capacity on the nodes:

```bash
$ srun -N3 peak-user-memory | column -t
Hostname       User    PeakVM  PID      Process
esrumcmpn03fl  abc123  41.8    1259622  python
Hostname       User    PeakVM  PID      Process
Hostname       User    PeakVM  PID      Process
esrumcmpn11fl  def456  53.3    1077871  python
esrumcmpn11fl  def456  66.1    1078437  python
```
