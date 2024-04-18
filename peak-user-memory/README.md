# peak-memory-usage

This script collects user processes from `/proc`, excluding UIDs < 1000 by default, and reports the peak virtual memory usage (PeakVM) for every process with a peak of at least 1 GB by default.

## Execution

```bash
./peak-memory-usage
```
