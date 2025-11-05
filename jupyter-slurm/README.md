# `jupyter_slurm` - running commands in Slurm from Jupyter notebooks

## Installation

### Installing `jupyter_slurm` in a virtual environment (recommended)

1. Deactivate any currently active `conda` and python environments

    ```shell
    conda deactivate # conda
    deactivate  # python
    ```

2. Load the python version you wish to use

    ```shell
    module load python/3.12.8
    ```

3. Create a virtual environment for `jupyter` / `jupyter_slurm`

    The name `jupyter-slurm` may be replaced by any name that you prefer

    ```shell
    python3 -m venv jupyter-slurm
    ```

4. Install `jupyter` in the environment

    You can install either the latest version or, if you prefer, a specific version of `jupyter` notebook:

    ```shell
    ./jupyter-slurm/bin/pip install notebook # the latest version, or
    ./jupyter-slurm/bin/pip install notebook==7.4.5 # a specific version
    ```

    Install any other python modules you need in the same manner.

5. Install `jupyter_slurm` in the environment

    ```shell
    # latest version
    ./jupyter-slurm/bin/pip install /projects/cbmr_shared/apps/dap/jupyter_slurm/latest
    # or a specific version
    # ./jupyter-slurm/bin/pip install /projects/cbmr_shared/apps/dap/jupyter_slurm/0.0.1
    ```

To start the notebook, run, replacing `XYZ` with the port number you are using (see the official documentation)

```shell
srun --pty -- ./jupyter-slurm/bin/jupyter notebook --no-browser --ip=0.0.0.0 --port=XYZ
```

You can now import / use `jupyter_slurm` as described in the `Usage` section below.

### Using `jupyter_slurm` in an existing/read-only Jupyter installation (alternative)

This method is not recommended, but allows you make use of `jupyter_slurm` if you are using the `jupyter` environment module, or another version of Jupyter where you cannot install your own python modules.

Instead of installing the module, add it to Python's `sys.path` list as shown below. This code has to be done before attempting to make use of the module:

```python
import sys
# latest version
sys.path.append("/projects/cbmr_shared/apps/dap/jupyter_slurm/latest/src")
# or a specific version
# sys.path.append("/projects/cbmr_shared/apps/dap/jupyter_slurm/0.0.1/src")
```

You can now import / use `jupyter_slurm` as described in the `Usage` section below.

## Usage

`jupyter_slurm` contains a wrapper for `sbatch` and for `srun`:

```python
import jupyter_slurm as jp

input_sam = "my-data.sam"
input_bam = "my-data.markdup.bam"

jobid = jp.sbatch(
    [
        ["samtools", "markdup", "my-data.sam", "--output", "my-data.markdup.bam"],
        ["samtools", "index", "my-data.markdup.bam"],
    ],
    modules=["samtools"],
)
print("Started job with ID", jobid)
```

```python
import jupyter_slurm as jp

input_sam = "my-data.sam"
input_bam = "my-data.markdup.bam"

result = jp.srun(
    ["samtools", "idxstats", "my-data.markdup.bam"],
    modules=["samtools"],
    capture=True,
)
print("Command ", ("failed" if result else "completed"), " with return code", result.returncode)
print("  STDOUT =", result.stdout)
print("  STDERR =", result.stderr)
```

## Reference

### sbatch

```python
def sbatch(commands: Sequence[str] | Sequence[Sequence[str]],
           *,
           cpus: int = 1,
           gpus: int = 0,
           gpu_type: Literal["a100", "h100", "A100", "H100"] | None = None,
           memory: int | str | None = None,
           job_name: str | None = None,
           modules: SequenceNotStr[str] = (),
           extra_args: SequenceNotStr[str] = (),
           output_file: str | Path | None = None,
           array_params: str | None = None,
           wait: bool = False,
           mail_user: str | bool = False,
           strict: bool = True) -> int
```

Submit an sbatch script for running one or more commands.

**Arguments**:

- `commands` - One or more commands to be run using sbatch. May be a list of strings,
  in which case the strings are assumed to be properly formatted commands and
  included as is, or a list of list of strings, in which case the each list of
  strings is assumed to represent a single command, and each argument is
  quoted/escaped to ensure that special characters are properly handled.
- `cpus` - The number of CPUs to reserve. Must be a number in the range 1 to 128.
  Defaults to 1.
- `memory` - The amount of memory to reserve. Must be a positive number (in MB) or a
  string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
- `gpus` - The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
  will be run on the GPU queue. Defaults to 0.
- `gpu_type` - Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
- `job_name` - An optional string naming the current Slurm job.
- `modules` - A list of zero or more environment modules to load before running the
  commands specified above. Defaults to ().
- `extra_args` - A list of arguments passed directly to srun/sbatch. Multi-part
  arguments must therefore be split into multiple values:
  ["--foo", "bar"] and not ["--foo bar"]
- `output_file` - Optional name of log-file foom the job.
- `array_params` - Optional job-array parameters (see "--array").
- `mail_user` - Send an email to user on failures or completion of the job. May
  either be an email address, or `True` to send an email to `$USER@ku.dk`.
- `wait` - If true, wait for the job to complete before returning. Defaults to False.
- `strict` - If true, the script is configured to terminate on the first error.
  Defaults to true.
  

**Returns**:

- `int` - The JobID of the submitted job.


### srun

```python
def srun(
    command: Sequence[str],
    *,
    cpus: int = 1,
    gpus: int = 0,
    memory: int | str | None = None,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = (),
    capture: bool = False,
    text: bool = True,
    strict: bool = True
) -> SrunResult[None] | SrunResult[str] | SrunResult[bytes]
```

Run command via `srun`, and optionally capture its output.

WARNING: This function can only be used from esrumhead01fl!

**Arguments**:

- `command` - The command to run, either as a single string that is assumed to
  contain a properly formatted shell command, or as a list of strings, that is
  assumed to present each argument in the command.
- `cpus` - The number of CPUs to reserve. Must be a number in the range 1 to 128.
  Defaults to 1.
- `memory` - The amount of memory to reserve. Must be a positive number (in MB) or a
  string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
- `gpus` - The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
  will be run on the GPU queue. Defaults to 0.
- `gpu_type` - Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
- `extra_args` - A list of arguments passed directly to srun/sbatch. Multi-part
  arguments must therefore be split into multiple values:
  ["--foo", "bar"] and not ["--foo bar"]
- `modules` - A list of zero or more environment modules to load before running the
  commands specified above. Defaults to ().
- `capture` - If true, srun's stdout and stderr is captured and returned. Defaults to
  False.
- `text` - If true, output captured by `capture` is assumed to be UTF8 and decoded
  to strings. Otherwise bytes are returned. Defaults to True.
- `strict` - If true, the script is configured to terminate on the first error.
  Defaults to true.
  

**Raises**:

- `SlurmError` - Raised if this command is invoked on a compute node.
  

**Returns**:

- `int` - The exit-code from running `srun` (non-zero on error)
  int, str, str: The srun exit-code, stdout, and stderr, if `capture` is True.
  int, bytes, bytes: As above, but `text` is False.

