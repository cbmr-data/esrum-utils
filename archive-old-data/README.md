# archive-old-data

Scripts for compressing old data files. This tool is intended to be used with [big_text](https://github.com/MikkelSchubert/big_text), but can process arbitrary lists of files.

For each file compressed by `archive-old-data.py`, a corresponding `.archived_by_dap.txt` file will be generated. This file contains information the original file, including permissions and timestamps.

## Example usage

```console
$ cat compressible-files.tsv
/projects/old_emc_cbmr-AUDIT/data/example.fna
$ python3 archive-old-data.py --state state.tsv compressible-files.tsv
$ ls -1 /projects/old_emc_cbmr-AUDIT/data/example.fna*
/projects/old_emc_cbmr-AUDIT/data/example.fna.gz
/projects/old_emc_cbmr-AUDIT/data/example.fna.archived_by_dap.txt
```

The `compressible-files.tsv` file may contain multiple, tab-separated columns, the last of which is assumed to be the filename. The resulting `.archived_by_dap.txt` file contains the following:

```plain
filename=/projects/old_emc_cbmr-AUDIT/data/example.fna
uid=436828696/zlc187
gid=2006030967/comp-prj-old_emc_cbmr-audit
mode=100664
size=35325058153
atime=2025-11-07T14:22:42.926748+01:00
mtime=2022-07-14T01:05:59.000000+01:00
ctime=2025-09-29T10:42:17.053108+01:00
```

The states file file contains the outcome, original size, compressed size, and path of each file that has been processed, and is used to resume/continue interrupted runs:

```console
$ cat state.tsv
compressed	35325058153	10208522894	/projects/old_emc_cbmr-AUDIT/data/example.fna
```

This file may be summarized using `summarize-states.py`:

```console
$ python3 summarize-states.py states.tsv
State                Files        SizeBefore         SizeAfter  Ratio
compressed           12374             86.4T             17.0T  0.20
not_found               79                 0                 0  1.00
target_exists           52            812.9G            812.9G  1.00
incompressible          14             74.3G             73.1G  0.98
permissions              2              5.5G              5.5G  1.00
```

In cases where a `path/to/filename.gz` file already exists for candidate file with name `path/to/filename`, the `check-existing-targets.py` script may be used to check if the (uncompressed) content of the two files match or mismatch:

```
$ uv run check-existing-targets.py state.tsv
MATCH   /path/to/file
```

The uncompressed MATCH'ing files can typically be deleted to save space.

## Usage

```
usage: archive-old-data.py [-h] --state STATE
                           [--compression-ratio COMPRESSION_RATIO]
                           [--threads THREADS]
                           filelist [filelist ...]

positional arguments:
  filelist

options:
  -h, --help            show this help message and exit
  --state STATE         Location of log file listing files already processed or
                        skipped (default: None)
  --compression-ratio COMPRESSION_RATIO
                        Compression ratio must be no more than this value,
                        calculated as compressed_size / original_size. If the
                        size is larger, the file is skipped (default: 0.9)
  --threads THREADS     Number of threads used for gzip compression (default:
                        16)
```

