# Simple NFS IO stats

This script prints simple latency/throughput statistics for each NFS mount-point.

This script is based on
https://github.com/stefanha/nfs-utils/blob/vsock/tools/mountstats/mountstats.py

## Usage

By default the script will check for and print IO activity every 5 seconds

	$ ./simple-nfs-stats
	Start                  End                    ReadMiBpS    ReadOpsTime    WriteMiBpS    WriteOpsTime    MountPoints
	2024-09-10T10:54:39    2024-09-10T10:54:44    0            0              205           1308.3          /maps/projects/example/data
	2024-09-10T10:54:44    2024-09-10T10:54:49    0            0              238           5629.6          /maps/projects/example/data
	2024-09-10T10:54:49    2024-09-10T10:54:54    0            0              265           8867.3          /maps/projects/example/data
	2024-09-10T10:55:19    2024-09-10T10:55:24    0.01         15.0           0             0               /maps/direct/software

The `MiBpS` columns report reads/writes in MiB per second and the `OpsTime` columns report the average waiting time for read/write operations.

To filter activity with low latency, use the `--min-ops-time` to skip updates where both `ReadOpsTime` and `WriteOpsTime` is below this value:

	$ ./simple-nfs-stats --min-ops-time 25

