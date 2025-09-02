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

### Writing commands for `sbatch` / `srun`
